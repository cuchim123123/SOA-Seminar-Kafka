import 'reflect-metadata';
import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { AppModule } from '../../src/app.module';
import { PrismaService } from '../../src/database/prisma.service';
import { PurchasesController } from '../../src/purchases/interfaces/http/purchases.controller';

describe('PostgreSQL Concurrent Sequence Integration Test (P4-01)', () => {
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

  it('assigns strictly unique and sequential numbers (1..10) across 10 concurrent requests for the same customer', async () => {
    const customerId = `U_CONCURRENT_${Date.now()}`;
    const concurrentCount = 10;

    // Dispatch 10 parallel purchase requests simultaneously
    const tasks = Array.from({ length: concurrentCount }, (_, i) =>
      controller.createPurchase({
        customerId,
        amountCents: (i + 1) * 1000,
      }),
    );

    const results = await Promise.all(tasks);

    // 1. Verify all 10 responses are ACCEPTED
    expect(results).toHaveLength(concurrentCount);
    for (const res of results) {
      expect(res.requestStatus).toBe('ACCEPTED');
      expect(res.purchaseId).toBeDefined();
      expect(res.eventId).toBeDefined();
    }

    // 2. Query persisted purchases directly from PostgreSQL
    const dbPurchases = await prisma.purchase.findMany({
      where: { customerId },
      orderBy: { completionSequence: 'asc' },
    });

    expect(dbPurchases).toHaveLength(concurrentCount);

    // 3. Verify assigned sequences are strictly sequential: 1, 2, 3, ..., 10
    const sequences = dbPurchases.map((p) => Number(p.completionSequence));
    const expectedSequences = Array.from({ length: concurrentCount }, (_, i) => i + 1);
    expect(sequences).toEqual(expectedSequences);

    // 4. Verify customer record next_event_sequence is incremented to 11
    const customerRecord = await prisma.purchaseCustomer.findUnique({
      where: { customerId },
    });
    expect(customerRecord).not.toBeNull();
    expect(Number(customerRecord?.nextEventSequence)).toBe(concurrentCount + 1);
  });
});
