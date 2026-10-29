import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { AppConfigService } from '../../src/config/app-config.service';
import { validateEnv } from '../../src/config/env.validation';
import { PrismaService } from '../../src/database/prisma.service';
import { ApplyPurchaseEventCommand } from '../../src/loyalty/application/commands/apply-purchase-event.command';
import { ApplyPurchaseEventHandler } from '../../src/loyalty/application/commands/apply-purchase-event.handler';
import { PostgresLoyaltyAccountReadDaoAdapter } from '../../src/loyalty/infrastructure/adapters/postgres-loyalty-account-read-dao.adapter';
import { PostgresLoyaltyUnitOfWorkAdapter } from '../../src/loyalty/infrastructure/adapters/postgres-loyalty-unit-of-work.adapter';

describe('Loyalty Durable Processing Integration Test (P4-04)', () => {
  let prisma: PrismaService;
  let handler: ApplyPurchaseEventHandler;
  let readDao: PostgresLoyaltyAccountReadDaoAdapter;
  const config = new AppConfigService(validateEnv(process.env));

  const testCustomerId = `TEST_LOYALTY_${Date.now()}`;
  const evtId1 = `evt_durable_${Date.now()}_1`;
  const evtId2 = `evt_durable_${Date.now()}_2`;
  const evtId4 = `evt_durable_${Date.now()}_4`;

  beforeAll(async () => {
    prisma = new PrismaService(config);
    await prisma.onModuleInit();

    const uow = new PostgresLoyaltyUnitOfWorkAdapter(prisma);
    handler = new ApplyPurchaseEventHandler(uow);
    readDao = new PostgresLoyaltyAccountReadDaoAdapter(prisma);
  });

  afterAll(async () => {
    // Clean up test data
    if (prisma) {
      await prisma.loyaltyAccount.deleteMany({
        where: { customerId: testCustomerId },
      });
      await prisma.processedEvent.deleteMany({
        where: {
          consumerName: 'loyalty-v1',
          eventId: { in: [evtId1, evtId2, evtId4] },
        },
      });
      await prisma.onModuleDestroy();
    }
  });

  beforeEach(async () => {
    // Ensure clean state before each test
    await prisma.loyaltyAccount.deleteMany({
      where: { customerId: testCustomerId },
    });
    await prisma.processedEvent.deleteMany({
      where: {
        consumerName: 'loyalty-v1',
        eventId: { in: [evtId1, evtId2, evtId4] },
      },
    });
  });

  it('atomically creates loyalty account and records processed event on first purchase', async () => {
    const cmd1 = new ApplyPurchaseEventCommand({
      eventId: evtId1,
      customerId: testCustomerId,
      purchaseId: `pur_${testCustomerId}_1`,
      amountCents: 12_000, // $120.00 -> evaluated at Bronze (1x) -> 12 points, moves to Silver
      customerSequence: 1,
      correlationId: 'corr_1',
    });

    const result1 = await handler.execute(cmd1);

    expect(result1.isDuplicate).toBe(false);
    expect(result1.pointsEarned).toBe(12);
    expect(result1.totalPoints).toBe(12);
    expect(result1.currentTier).toBe('SILVER');
    expect(result1.totalSpendCents).toBe(12_000);

    // Verify database state directly
    const dbAccount = await prisma.loyaltyAccount.findUnique({
      where: { customerId: testCustomerId },
    });
    expect(dbAccount).not.toBeNull();
    expect(dbAccount?.points).toBe(12n);
    expect(dbAccount?.tier).toBe('SILVER');
    expect(dbAccount?.lastCustomerSequence).toBe(1n);

    const dbProcessed = await prisma.processedEvent.findUnique({
      where: {
        consumerName_eventId: {
          consumerName: 'loyalty-v1',
          eventId: evtId1,
        },
      },
    });
    expect(dbProcessed).not.toBeNull();

    // Verify Read DAO
    const projection = await readDao.findById(testCustomerId);
    expect(projection?.pointsBalance).toBe(12);
    expect(projection?.tier).toBe('SILVER');
  });

  it('guarantees idempotency: duplicate event delivery does NOT update points twice', async () => {
    const cmd1 = new ApplyPurchaseEventCommand({
      eventId: evtId1,
      customerId: testCustomerId,
      purchaseId: `pur_${testCustomerId}_1`,
      amountCents: 12_000,
      customerSequence: 1,
      correlationId: 'corr_1',
    });

    // 1. Process first delivery
    const result1 = await handler.execute(cmd1);
    expect(result1.isDuplicate).toBe(false);
    expect(result1.pointsEarned).toBe(12);
    expect(result1.totalPoints).toBe(12);

    // 2. Deliver identical duplicate event (simulating redelivery after network failure)
    const resultDuplicate = await handler.execute(cmd1);

    expect(resultDuplicate.isDuplicate).toBe(true);
    expect(resultDuplicate.pointsEarned).toBe(0);
    expect(resultDuplicate.totalPoints).toBe(12); // Points must NOT double to 24!

    // Verify database state: points remain strictly 12
    const dbAccount = await prisma.loyaltyAccount.findUnique({
      where: { customerId: testCustomerId },
    });
    expect(dbAccount?.points).toBe(12n);
    expect(dbAccount?.eligibleSpendCents).toBe(12_000n);
    expect(dbAccount?.lastCustomerSequence).toBe(1n);
  });

  it('processes subsequent sequential purchases and correctly recalculates tier multipliers', async () => {
    // Event 1: $120.00 (12,000 cents), seq 1 -> evaluated at Bronze -> 12 pts, new tier Silver
    await handler.execute(
      new ApplyPurchaseEventCommand({
        eventId: evtId1,
        customerId: testCustomerId,
        purchaseId: `pur_${testCustomerId}_1`,
        amountCents: 12_000,
        customerSequence: 1,
        correlationId: 'corr_1',
      }),
    );

    // Event 2: $200.00 (20,000 cents), seq 2 -> evaluated at Silver (2x) -> 40 pts, new tier Gold
    const result2 = await handler.execute(
      new ApplyPurchaseEventCommand({
        eventId: evtId2,
        customerId: testCustomerId,
        purchaseId: `pur_${testCustomerId}_2`,
        amountCents: 20_000,
        customerSequence: 2,
        correlationId: 'corr_2',
      }),
    );

    expect(result2.isDuplicate).toBe(false);
    expect(result2.pointsEarned).toBe(40);
    expect(result2.totalPoints).toBe(52); // 12 + 40
    expect(result2.currentTier).toBe('GOLD');
    expect(result2.totalSpendCents).toBe(32_000);

    const dbAccount = await prisma.loyaltyAccount.findUnique({
      where: { customerId: testCustomerId },
    });
    expect(dbAccount?.points).toBe(52n);
    expect(dbAccount?.tier).toBe('GOLD');
    expect(dbAccount?.lastCustomerSequence).toBe(2n);
  });

  it('rolls back database transaction and does not mark event processed on sequence gap', async () => {
    // Event 1: seq 1
    await handler.execute(
      new ApplyPurchaseEventCommand({
        eventId: evtId1,
        customerId: testCustomerId,
        purchaseId: `pur_${testCustomerId}_1`,
        amountCents: 10_000,
        customerSequence: 1,
        correlationId: 'corr_1',
      }),
    );

    // Attempt Event 4: seq 4 (gap: missing seq 2 and 3)
    const gapCommand = new ApplyPurchaseEventCommand({
      eventId: evtId4,
      customerId: testCustomerId,
      purchaseId: `pur_${testCustomerId}_4`,
      amountCents: 10_000,
      customerSequence: 4,
      correlationId: 'corr_4',
    });

    await expect(handler.execute(gapCommand)).rejects.toThrowError(
      /Sequence gap or out-of-order event/,
    );

    // Verify rollback: evtId4 must NOT be recorded in processed_events
    const processedGap = await prisma.processedEvent.findUnique({
      where: {
        consumerName_eventId: {
          consumerName: 'loyalty-v1',
          eventId: evtId4,
        },
      },
    });
    expect(processedGap).toBeNull();

    // Verify loyalty account was unchanged
    const dbAccount = await prisma.loyaltyAccount.findUnique({
      where: { customerId: testCustomerId },
    });
    expect(dbAccount?.points).toBe(10n);
    expect(dbAccount?.lastCustomerSequence).toBe(1n);
  });
});
