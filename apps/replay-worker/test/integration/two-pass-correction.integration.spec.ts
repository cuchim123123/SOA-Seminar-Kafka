import 'reflect-metadata';
import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  createPurchaseCompletedEnvelope,
  createPurchaseInvalidatedEnvelope,
  getEventPartitionKey,
  serializeEnvelope,
} from '@lab/contracts';
import { Admin, Kafka, Producer } from 'kafkajs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { AppModule } from '../../src/app.module';
import { AppConfigService } from '../../src/config/app-config.service';
import { RAW_ENV_TOKEN } from '../../src/config/config.module';
import { PrismaService } from '../../src/database/prisma.service';
import { StartBoundedReplayHandler } from '../../src/replay/application/commands/start-bounded-replay.handler';
import { CompareReplayWithLiveHandler } from '../../src/replay/application/queries/compare-replay-with-live.handler';
import { CompareReplayWithLiveQuery } from '../../src/replay/application/queries/compare-replay-with-live.query';
import { GetReplayStagingHandler } from '../../src/replay/application/queries/get-replay-staging.handler';
import { GetReplayStagingQuery } from '../../src/replay/application/queries/get-replay-staging.query';

describe('Two-Pass Correction Integration Test (P5-03)', () => {
  let app: INestApplication;
  let config: AppConfigService;
  let prisma: PrismaService;
  let kafka: Kafka;
  let producer: Producer;
  let admin: Admin;

  beforeAll(async () => {
    const moduleRef: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    })
      .overrideProvider(RAW_ENV_TOKEN)
      .useValue({
        NODE_ENV: 'test',
        PORT: '4010',
        REPLAY_WORKER_PORT: '4010',
        KAFKA_BROKERS: 'localhost:9092',
        KAFKA_CLIENT_ID: `two-pass-integ-${Date.now()}`,
        PURCHASE_TOPIC: 'purchase.events',
        LOYALTY_DATABASE_URL:
          process.env.LOYALTY_DATABASE_URL ||
          'postgresql://postgres:postgres@localhost:5432/loyalty_db?schema=public',
      })
      .compile();

    app = moduleRef.createNestApplication();
    await app.init();

    config = app.get(AppConfigService);
    prisma = app.get(PrismaService);

    kafka = new Kafka({
      clientId: `two-pass-client-${Date.now()}`,
      brokers: config.kafkaBrokers,
    });

    producer = kafka.producer();
    admin = kafka.admin();
    await producer.connect();
    await admin.connect();
  }, 30000);

  afterAll(async () => {
    if (producer) await producer.disconnect();
    if (admin) await admin.disconnect();
    if (app) await app.close();
  });

  it('performs two-pass correction: calculates 40 points instead of naive 70, isolates in staging, and preserves live projections', async () => {
    const topic = config.purchaseTopic;
    const testCustomerId = `U_CORRECTION_${Date.now()}`;
    const p1Id = `PUR_1_${Date.now()}`;
    const p2Id = `PUR_2_${Date.now()}`;
    const p3Id = `PUR_3_${Date.now()}`;

    // 1. Pre-seed live projection in loyalty_accounts to simulate live worker having processed U001 naively:
    // P1: $120 -> 12 pts (Bronze -> Silver)
    // P2: $200 -> 40 pts (Silver -> Gold)
    // P3: $100 -> 30 pts (Gold -> Gold)
    // Total live = 82 pts, 42000 cents, Gold, correctionRequired = true
    await prisma.loyaltyAccount.upsert({
      where: { customerId: testCustomerId },
      create: {
        customerId: testCustomerId,
        tier: 'GOLD',
        points: 82n,
        eligibleSpendCents: 42000n,
        lastCustomerSequence: 4n,
        correctionRequired: true,
      },
      update: {
        tier: 'GOLD',
        points: 82n,
        eligibleSpendCents: 42000n,
        lastCustomerSequence: 4n,
        correctionRequired: true,
      },
    });

    // 2. Publish P001 ($120), P002 ($200), P003 ($100), and Invalidation of P001 to Kafka
    const e1 = createPurchaseCompletedEnvelope({
      customerId: testCustomerId,
      customerSequence: 1,
      purchaseId: p1Id,
      amountCents: 12000,
    });
    const e2 = createPurchaseCompletedEnvelope({
      customerId: testCustomerId,
      customerSequence: 2,
      purchaseId: p2Id,
      amountCents: 20000,
    });
    const e3 = createPurchaseCompletedEnvelope({
      customerId: testCustomerId,
      customerSequence: 3,
      purchaseId: p3Id,
      amountCents: 10000,
    });
    const e4 = createPurchaseInvalidatedEnvelope({
      customerId: testCustomerId,
      customerSequence: 4,
      purchaseId: p1Id,
      reason: 'DEMO_INVALIDATION',
    });

    for (const env of [e1, e2, e3, e4]) {
      await producer.send({
        topic,
        messages: [
          {
            key: getEventPartitionKey(env),
            value: serializeEnvelope(env),
          },
        ],
      });
    }

    // 3. Execute bounded replay with unique runId
    const runId = `corr-run-${Date.now()}`;
    const startHandler = app.get(StartBoundedReplayHandler);
    const result = await startHandler.execute({
      runId,
      topic,
    });

    expect(result.id).toBe(runId);
    expect(result.status).toBe('COMPLETED');
    expect(result.scannedCount).toBeGreaterThanOrEqual(4);

    // 4. Query Replay Staging for this customer
    const stagingHandler = app.get(GetReplayStagingHandler);
    const stagingRecords = await stagingHandler.execute(new GetReplayStagingQuery(runId));
    const customerStaging = stagingRecords.find((r) => r.customerId === testCustomerId);

    expect(customerStaging).toBeDefined();
    // CRITICAL PROOF: 82 -> 40 points!
    // P001 ($120) is invalidated.
    // Remaining valid: P002 ($200 at BRONZE = 20 pts -> SILVER) + P003 ($100 at SILVER = 20 pts -> GOLD) = 40 points!
    // Naive subtraction would be 82 - 12 = 70 points (wrong).
    expect(customerStaging?.points).toBe(40);
    expect(customerStaging?.tier).toBe('GOLD');
    expect(customerStaging?.eligibleSpendCents).toBe(30000);
    expect(customerStaging?.lastCustomerSequence).toBe(4);

    // 5. Query Comparison endpoint: live vs staged
    const comparisonHandler = app.get(CompareReplayWithLiveHandler);
    const comparison = await comparisonHandler.execute(
      new CompareReplayWithLiveQuery(runId, testCustomerId),
    );

    expect(comparison.customerId).toBe(testCustomerId);
    expect(comparison.live).toBeDefined();
    expect(comparison.live?.points).toBe(82);
    expect(comparison.live?.correctionRequired).toBe(true);

    expect(comparison.staged).toBeDefined();
    expect(comparison.staged?.points).toBe(40);
    expect(comparison.staged?.tier).toBe('GOLD');
    expect(comparison.staged?.eligibleSpendCents).toBe(30000);

    // Discrepancy: 40 - 82 = -42 points
    expect(comparison.discrepancy.pointsDifference).toBe(-42);
    expect(comparison.discrepancy.spendDifferenceCents).toBe(-12000);
    expect(comparison.discrepancy.tierChanged).toBe(false);

    // 6. Verify live projection in loyalty_accounts was NOT mutated or overwritten
    const liveAccountAfter = await prisma.loyaltyAccount.findUnique({
      where: { customerId: testCustomerId },
    });
    expect(liveAccountAfter).toBeDefined();
    expect(liveAccountAfter?.points).toBe(82n);
    expect(liveAccountAfter?.eligibleSpendCents).toBe(42000n);
    expect(liveAccountAfter?.tier).toBe('GOLD');
  }, 45000);
});
