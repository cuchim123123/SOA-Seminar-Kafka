import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { AppConfigService } from '../../src/config/app-config.service';
import { validateEnv } from '../../src/config/env.validation';
import { PrismaService } from '../../src/database/prisma.service';
import { ApplyPurchaseEventCommand } from '../../src/loyalty/application/commands/apply-purchase-event.command';
import { ApplyPurchaseEventHandler } from '../../src/loyalty/application/commands/apply-purchase-event.handler';
import { PostgresLoyaltyUnitOfWorkAdapter } from '../../src/loyalty/infrastructure/adapters/postgres-loyalty-unit-of-work.adapter';

describe('Fault Injection Integration Suite (P4-06)', () => {
  let prisma: PrismaService;
  let handler: ApplyPurchaseEventHandler;
  const config = new AppConfigService(validateEnv(process.env));

  const custFaultD4 = `FAULT_D4_INTEG_${Date.now()}`;
  const custFaultD5 = `FAULT_D5_INTEG_${Date.now()}`;
  const evtFaultD4 = `evt_fault_d4_integ_${Date.now()}`;
  const evtFaultD5 = `evt_fault_d5_integ_${Date.now()}`;

  beforeAll(async () => {
    prisma = new PrismaService(config);
    await prisma.onModuleInit();

    const uow = new PostgresLoyaltyUnitOfWorkAdapter(prisma);
    handler = new ApplyPurchaseEventHandler(uow);
  });

  afterAll(async () => {
    if (prisma) {
      await prisma.loyaltyAccount.deleteMany({
        where: { customerId: { in: [custFaultD4, custFaultD5] } },
      });
      await prisma.processedEvent.deleteMany({
        where: {
          consumerName: 'loyalty-v1',
          eventId: { in: [evtFaultD4, evtFaultD5] },
        },
      });
      await prisma.onModuleDestroy();
    }
  });

  beforeEach(async () => {
    await prisma.loyaltyAccount.deleteMany({
      where: { customerId: { in: [custFaultD4, custFaultD5] } },
    });
    await prisma.processedEvent.deleteMany({
      where: {
        consumerName: 'loyalty-v1',
        eventId: { in: [evtFaultD4, evtFaultD5] },
      },
    });
  });

  it('D4 scenario: after DB commit crash simulation, redelivered message is skipped without duplicate points', async () => {
    const cmd1 = new ApplyPurchaseEventCommand({
      eventId: evtFaultD4,
      customerId: custFaultD4,
      purchaseId: `pur_${custFaultD4}_1`,
      amountCents: 12_000,
      customerSequence: 1,
      correlationId: `corr_${custFaultD4}_1`,
    });

    // 1. Initial delivery: DB commits points and processed_events row
    const result1 = await handler.execute(cmd1);
    expect(result1.isDuplicate).toBe(false);
    expect(result1.pointsEarned).toBe(12);
    expect(result1.totalPoints).toBe(12);

    // Verify DB state
    const account1 = await prisma.loyaltyAccount.findUnique({
      where: { customerId: custFaultD4 },
    });
    expect(account1?.points).toBe(12n);

    // 2. Simulated Crash before Kafka offset commit: Kafka redelivers identical message
    const resultRedelivery = await handler.execute(cmd1);
    expect(resultRedelivery.isDuplicate).toBe(true);
    expect(resultRedelivery.pointsEarned).toBe(0);
    expect(resultRedelivery.totalPoints).toBe(12); // Must NOT double to 24

    // Verify DB state remains strictly 12 points (attempts >= 2, mutations == 1)
    const accountAfterRedelivery = await prisma.loyaltyAccount.findUnique({
      where: { customerId: custFaultD4 },
    });
    expect(accountAfterRedelivery?.points).toBe(12n);
  });

  it('D5 scenario: duplicate published event from outbox crash is deduplicated idempotently by consumer', async () => {
    // Both publisher deliveries carry the identical stable eventId
    const cmdPublish1 = new ApplyPurchaseEventCommand({
      eventId: evtFaultD5,
      customerId: custFaultD5,
      purchaseId: `pur_${custFaultD5}_1`,
      amountCents: 20_000,
      customerSequence: 1,
      correlationId: `corr_${custFaultD5}_1`,
    });

    const res1 = await handler.execute(cmdPublish1);
    expect(res1.isDuplicate).toBe(false);
    expect(res1.pointsEarned).toBe(20);
    expect(res1.totalPoints).toBe(20);

    // Second publish from outbox publisher redrive after crash carries the SAME eventId
    const resDuplicatePublish = await handler.execute(cmdPublish1);
    expect(resDuplicatePublish.isDuplicate).toBe(true);
    expect(resDuplicatePublish.pointsEarned).toBe(0);
    expect(resDuplicatePublish.totalPoints).toBe(20); // Must NOT double to 40

    const account = await prisma.loyaltyAccount.findUnique({
      where: { customerId: custFaultD5 },
    });
    expect(account?.points).toBe(20n);
  });
});
