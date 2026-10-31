import { Inject, Injectable, Optional } from '@nestjs/common';
import { uuidv7 } from '@lab/contracts';
import { PrismaService } from '../../../database/prisma.service';

export interface OutboxStatusResult {
  totalCount: number;
  events: Array<{
    eventId: string;
    aggregateId: string;
    customerSequence: string;
    eventType: string;
    payload: unknown;
    status: string;
    attemptCount: number;
    createdAt: string;
    publishedAt: string | null;
  }>;
  recentIdempotencyKeys: Array<{
    customerId: string;
    idempotencyKey: string;
    requestFingerprint: string;
    purchaseId: string;
  }>;
}

export interface RollbackTestResult {
  status: string;
  simulatedFailureReason: string;
  testCustomerId: string;
  testPurchaseId: string;
  persistedOutboxRows: number;
  persistedPurchases: number;
  dualWritePrevented: boolean;
  verificationDetails: string;
}

@Injectable()
export class OpsDatabaseService {
  constructor(
    @Optional()
    @Inject(PrismaService)
    private readonly prisma?: PrismaService,
  ) {}

  async getOutboxStatus(): Promise<OutboxStatusResult> {
    if (!this.prisma) {
      return { totalCount: 0, events: [], recentIdempotencyKeys: [] };
    }

    const [totalCount, events, recentKeys] = await Promise.all([
      this.prisma.outboxEvent.count(),
      this.prisma.outboxEvent.findMany({
        take: 30,
        orderBy: { createdAt: 'desc' },
      }),
      this.prisma.httpIdempotencyKey.findMany({
        take: 15,
      }),
    ]);

    return {
      totalCount,
      events: events.map((e) => ({
        eventId: e.eventId,
        aggregateId: e.aggregateId,
        customerSequence: e.customerSequence.toString(),
        eventType: e.eventType,
        payload: e.payloadJson,
        status: e.status,
        attemptCount: e.attemptCount,
        createdAt: e.createdAt.toISOString(),
        publishedAt: e.publishedAt?.toISOString() ?? null,
      })),
      recentIdempotencyKeys: recentKeys.map((k) => ({
        customerId: k.customerId,
        idempotencyKey: k.idempotencyKey,
        requestFingerprint: k.requestFingerprint,
        purchaseId: k.purchaseId,
      })),
    };
  }

  async testOutboxRollback(): Promise<RollbackTestResult> {
    if (!this.prisma) {
      throw new Error('Database service not available');
    }

    const testCustomerId = `ROLLBACK_TEST_${Date.now()}`;
    const testPurchaseId = `pur_fail_${uuidv7().slice(0, 8)}`;
    const testEventId = uuidv7();

    let caughtError = false;
    try {
      await this.prisma.$transaction(async (tx) => {
        await tx.purchaseCustomer.upsert({
          where: { customerId: testCustomerId },
          create: { customerId: testCustomerId, nextEventSequence: 1n },
          update: {},
        });

        await tx.purchase.create({
          data: {
            purchaseId: testPurchaseId,
            customerId: testCustomerId,
            amountCents: 9999n,
            status: 'PENDING_CONFIRMATION',
            completionSequence: 1n,
          },
        });

        await tx.outboxEvent.create({
          data: {
            eventId: testEventId,
            aggregateId: testCustomerId,
            customerSequence: 1n,
            eventType: 'PurchaseCompleted',
            payloadJson: {
              testRollback: true,
              purchaseId: testPurchaseId,
              amountCents: 9999,
            },
            status: 'PENDING',
          },
        });

        throw new Error('SIMULATED_TRANSACTION_FAILURE_BEFORE_COMMIT');
      });
    } catch {
      caughtError = true;
    }

    const [persistedOutboxRows, persistedPurchases] = await Promise.all([
      this.prisma.outboxEvent.count({ where: { aggregateId: testCustomerId } }),
      this.prisma.purchase.count({ where: { purchaseId: testPurchaseId } }),
    ]);

    const dualWritePrevented = caughtError && persistedOutboxRows === 0 && persistedPurchases === 0;

    return {
      status: dualWritePrevented ? 'ROLLED_BACK_ATOMIC_SUCCESS' : 'ROLLBACK_FAILED',
      simulatedFailureReason:
        'Deliberate runtime exception thrown before unit-of-work commit to test transaction atomicity.',
      testCustomerId,
      testPurchaseId,
      persistedOutboxRows,
      persistedPurchases,
      dualWritePrevented,
      verificationDetails: dualWritePrevented
        ? 'Postgres transactional isolation successfully rolled back all changes. Zero outbox events were persisted, ensuring Debezium CDC and Kafka brokers receive NO corrupted or phantom records.'
        : 'Warning: transaction did not roll back cleanly.',
    };
  }
}
