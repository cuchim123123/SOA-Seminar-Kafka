import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { AppConfigService } from '../../src/config/app-config.service';
import { validateEnv } from '../../src/config/env.validation';
import { PrismaService } from '../../src/database/prisma.service';
import { ApplySalesEventCommand } from '../../src/analytics/application/commands/apply-sales-event.command';
import { ApplySalesEventHandler } from '../../src/analytics/application/commands/apply-sales-event.handler';
import { PostgresAnalyticsUnitOfWorkAdapter } from '../../src/analytics/infrastructure/adapters/postgres-analytics-unit-of-work.adapter';
import { PostgresSalesSummaryReadDaoAdapter } from '../../src/analytics/infrastructure/adapters/postgres-sales-summary-read-dao.adapter';

describe('Analytics Durable Processing Integration Test (P4-05)', () => {
  let prisma: PrismaService;
  let handler: ApplySalesEventHandler;
  let readDao: PostgresSalesSummaryReadDaoAdapter;
  const config = new AppConfigService(validateEnv(process.env));

  const evtId1 = `evt_ana_durable_${Date.now()}_1`;
  const evtId2 = `evt_ana_durable_${Date.now()}_2`;
  const evtCross = `evt_cross_ctx_${Date.now()}`;

  beforeAll(async () => {
    prisma = new PrismaService(config);
    await prisma.onModuleInit();

    const uow = new PostgresAnalyticsUnitOfWorkAdapter(prisma);
    handler = new ApplySalesEventHandler(uow);
    readDao = new PostgresSalesSummaryReadDaoAdapter(prisma);
  });

  afterAll(async () => {
    if (prisma) {
      await prisma.processedEvent.deleteMany({
        where: {
          eventId: { in: [evtId1, evtId2, evtCross] },
        },
      });
      await prisma.onModuleDestroy();
    }
  });

  beforeEach(async () => {
    // Reset global analytics summary to clean baseline
    await prisma.analyticsSummary.upsert({
      where: { id: 'global' },
      create: {
        id: 'global',
        totalRevenueCents: 0n,
        purchaseCount: 0n,
        customerIds: [],
        lastEventId: null,
        lastProcessedAt: null,
      },
      update: {
        totalRevenueCents: 0n,
        purchaseCount: 0n,
        customerIds: [],
        lastEventId: null,
        lastProcessedAt: null,
      },
    });

    await prisma.processedEvent.deleteMany({
      where: {
        eventId: { in: [evtId1, evtId2, evtCross] },
      },
    });
  });

  it('atomically initializes analytics summary and records processed event on first purchase', async () => {
    const cmd1 = new ApplySalesEventCommand({
      eventId: evtId1,
      customerId: 'CUST_ANA_1',
      purchaseId: 'pur_ana_1',
      amountCents: 15_000,
      customerSequence: 1,
      correlationId: 'corr_ana_1',
    });

    const result1 = await handler.execute(cmd1);

    expect(result1.isDuplicate).toBe(false);
    expect(result1.totalRevenueCents).toBe(15_000);
    expect(result1.purchaseCount).toBe(1);
    expect(result1.uniqueCustomerCount).toBe(1);

    // Verify DB state directly
    const dbSummary = await prisma.analyticsSummary.findUnique({
      where: { id: 'global' },
    });
    expect(dbSummary?.totalRevenueCents).toBe(15_000n);
    expect(dbSummary?.purchaseCount).toBe(1n);

    const dbProcessed = await prisma.processedEvent.findUnique({
      where: {
        consumerName_eventId: {
          consumerName: 'analytics-v1',
          eventId: evtId1,
        },
      },
    });
    expect(dbProcessed).not.toBeNull();

    // Verify Read DAO
    const projection = await readDao.getSummary();
    expect(projection.totalRevenueCents).toBe(15_000);
    expect(projection.purchaseCount).toBe(1);
    expect(projection.uniqueCustomerCount).toBe(1);
  });

  it('guarantees idempotency: duplicate event does NOT mutate total revenue or count', async () => {
    const cmd1 = new ApplySalesEventCommand({
      eventId: evtId1,
      customerId: 'CUST_ANA_1',
      purchaseId: 'pur_ana_1',
      amountCents: 15_000,
      customerSequence: 1,
      correlationId: 'corr_ana_1',
    });

    // 1. Initial delivery
    const result1 = await handler.execute(cmd1);
    expect(result1.isDuplicate).toBe(false);
    expect(result1.totalRevenueCents).toBe(15_000);

    // 2. Deliver identical duplicate event (simulating Kafka at-least-once redelivery)
    const resultDuplicate = await handler.execute(cmd1);

    expect(resultDuplicate.isDuplicate).toBe(true);
    expect(resultDuplicate.totalRevenueCents).toBe(15_000); // Must NOT double to 30,000!
    expect(resultDuplicate.purchaseCount).toBe(1);

    // Verify DB state remains unchanged
    const dbSummary = await prisma.analyticsSummary.findUnique({
      where: { id: 'global' },
    });
    expect(dbSummary?.totalRevenueCents).toBe(15_000n);
    expect(dbSummary?.purchaseCount).toBe(1n);
  });

  it('aggregates multiple customers correctly in durable projection', async () => {
    await handler.execute(
      new ApplySalesEventCommand({
        eventId: evtId1,
        customerId: 'CUST_ANA_1',
        purchaseId: 'pur_ana_1',
        amountCents: 12_000,
        customerSequence: 1,
      }),
    );

    const result2 = await handler.execute(
      new ApplySalesEventCommand({
        eventId: evtId2,
        customerId: 'CUST_ANA_2',
        purchaseId: 'pur_ana_2',
        amountCents: 20_000,
        customerSequence: 1,
      }),
    );

    expect(result2.isDuplicate).toBe(false);
    expect(result2.totalRevenueCents).toBe(32_000);
    expect(result2.purchaseCount).toBe(2);
    expect(result2.uniqueCustomerCount).toBe(2);

    const projection = await readDao.getSummary();
    expect(projection.totalRevenueCents).toBe(32_000);
    expect(projection.purchaseCount).toBe(2);
    expect(projection.uniqueCustomerCount).toBe(2);
  });

  it('proves consumer group isolation: processed_events in loyalty-v1 does not block analytics-v1', async () => {
    // 1. Seed event as already processed by loyalty-v1
    await prisma.processedEvent.create({
      data: {
        consumerName: 'loyalty-v1',
        eventId: evtCross,
      },
    });

    // 2. Process the same eventId in analytics-v1
    const result = await handler.execute(
      new ApplySalesEventCommand({
        eventId: evtCross,
        customerId: 'CUST_CROSS',
        purchaseId: 'pur_cross',
        amountCents: 8_000,
        customerSequence: 1,
      }),
    );

    // Must be processed as NEW for analytics-v1 despite existing in loyalty-v1!
    expect(result.isDuplicate).toBe(false);
    expect(result.totalRevenueCents).toBe(8_000);
    expect(result.purchaseCount).toBe(1);

    // Both consumer records must now coexist in processed_events table independently
    const rows = await prisma.processedEvent.findMany({
      where: { eventId: evtCross },
    });
    expect(rows).toHaveLength(2);
    const consumerNames = rows.map((r) => r.consumerName).sort();
    expect(consumerNames).toEqual(['analytics-v1', 'loyalty-v1']);
  });
});
