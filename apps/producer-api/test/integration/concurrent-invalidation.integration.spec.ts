import 'reflect-metadata';
import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { AppModule } from '../../src/app.module';
import { PrismaService } from '../../src/database/prisma.service';
import { PurchasesController } from '../../src/purchases/interfaces/http/purchases.controller';

describe('PostgreSQL Concurrent Invalidation & Idempotency Integration Test', () => {
  let app: INestApplication;
  let controller: PurchasesController;
  let prisma: PrismaService;

  beforeAll(async () => {
    const moduleRef: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.init();

    controller = app.get(PurchasesController);
    prisma = app.get(PrismaService);
  }, 30000);

  afterAll(async () => {
    if (app) {
      await app.close();
    }
  }, 10000);

  it('guarantees single invalidation under concurrency and returns identical eventId and customerSequence on duplicate calls', async () => {
    const customerId = `U_CONC_INV_${Date.now()}`;

    // 1. Create initial purchase (assigned customerSequence = 1)
    const purchaseResult = await controller.createPurchase({
      customerId,
      amountCents: 15000,
    });

    const purchaseId = purchaseResult.purchaseId;
    expect(purchaseId).toBeDefined();

    // Verify initial state in DB
    const initialPurchase = await prisma.purchase.findUnique({
      where: { purchaseId },
    });
    expect(initialPurchase?.status).toBe('COMPLETED');
    expect(Number(initialPurchase?.completionSequence)).toBe(1);
    expect(initialPurchase?.invalidationSequence).toBeNull();
    expect(initialPurchase?.invalidationEventId).toBeNull();

    // 2. Launch 5 concurrent invalidation requests for the same purchase simultaneously
    const concurrentCount = 5;
    const invalidationPromises = Array.from({ length: concurrentCount }, () =>
      controller.invalidatePurchase(purchaseId, {
        reason: 'DEMO_INVALIDATION',
      }),
    );

    const results = await Promise.all(invalidationPromises);

    // 3. Exactly ONE request should be the winner (isAlreadyInvalidated: false)
    const initialInvalidations = results.filter((r) => !r.isAlreadyInvalidated);
    const duplicateInvalidations = results.filter((r) => r.isAlreadyInvalidated);

    expect(initialInvalidations).toHaveLength(1);
    expect(duplicateInvalidations).toHaveLength(concurrentCount - 1);

    const winner = initialInvalidations[0];
    expect(winner.status).toBe('INVALIDATED');
    expect(winner.customerSequence).toBe(2); // Next monotonic sequence allocated (1 -> 2)
    expect(winner.eventId).toBeDefined();

    // 4. All duplicate responses must match the winner's customerSequence and eventId
    for (const dup of duplicateInvalidations) {
      expect(dup.status).toBe('INVALIDATED');
      expect(dup.customerSequence).toBe(winner.customerSequence); // Must be 2, NOT 1!
      expect(dup.eventId).toBe(winner.eventId); // Must match winner's eventId, NOT purchaseId!
      expect(dup.isAlreadyInvalidated).toBe(true);
    }

    // 5. Verify database state
    const dbPurchaseAfter = await prisma.purchase.findUnique({
      where: { purchaseId },
    });
    expect(dbPurchaseAfter?.status).toBe('INVALIDATED');
    expect(Number(dbPurchaseAfter?.completionSequence)).toBe(1);
    expect(Number(dbPurchaseAfter?.invalidationSequence)).toBe(2);
    expect(dbPurchaseAfter?.invalidationEventId).toBe(winner.eventId);
    expect(dbPurchaseAfter?.invalidationReason).toBe('DEMO_INVALIDATION');

    // Exactly ONE PurchaseInvalidated record should exist in outbox_events
    const outboxEvents = await prisma.outboxEvent.findMany({
      where: {
        aggregateId: customerId,
        eventType: 'PurchaseInvalidated',
      },
    });
    expect(outboxEvents).toHaveLength(1);
    expect(outboxEvents[0].eventId).toBe(winner.eventId);
    expect(Number(outboxEvents[0].customerSequence)).toBe(2);

    // Sequence generator must only have advanced to 3 (1 for purchase, 2 for invalidation, next is 3)
    const customerRecord = await prisma.purchaseCustomer.findUnique({
      where: { customerId },
    });
    expect(Number(customerRecord?.nextEventSequence)).toBe(3);

    // 6. Test subsequent sequential call: must also return identical eventId and customerSequence
    const sequentialResult = await controller.invalidatePurchase(purchaseId, {
      reason: 'DEMO_INVALIDATION',
    });
    expect(sequentialResult.status).toBe('INVALIDATED');
    expect(sequentialResult.isAlreadyInvalidated).toBe(true);
    expect(sequentialResult.customerSequence).toBe(winner.customerSequence);
    expect(sequentialResult.eventId).toBe(winner.eventId);
  });
});
