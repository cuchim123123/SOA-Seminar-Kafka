import 'reflect-metadata';
import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  createPurchaseCompletedEnvelope,
  getEventPartitionKey,
  serializeEnvelope,
} from '@lab/contracts';
import { Kafka, Partitioners, Producer, RecordMetadata } from 'kafkajs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { AppModule } from '../../src/app.module';
import { AppConfigService } from '../../src/config/app-config.service';
import { RAW_ENV_TOKEN } from '../../src/config/config.module';
import {
  LOYALTY_ACCOUNT_REPOSITORY_PORT,
  LoyaltyAccountRepositoryPort,
} from '../../src/loyalty/application/ports/loyalty-account-repository.port';

describe('Ordered Workload & Multi-Partition Integration Test (P2-02)', () => {
  let app: INestApplication;
  let config: AppConfigService;
  let repository: LoyaltyAccountRepositoryPort;
  let producer: Producer;
  let kafka: Kafka;
  const uniqueGroupId = `loyalty-p2-workload-${Date.now()}`;

  beforeAll(async () => {
    const moduleRef: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    })
      .overrideProvider(RAW_ENV_TOKEN)
      .useValue({
        NODE_ENV: 'test',
        PORT: '4006',
        KAFKA_BROKERS: 'localhost:9092',
        KAFKA_CLIENT_ID: `loyalty-p2-client-${Date.now()}`,
        KAFKA_GROUP_ID: uniqueGroupId,
        PURCHASE_TOPIC: 'purchase.events',
      })
      .compile();

    app = moduleRef.createNestApplication();
    await app.init();

    config = app.get(AppConfigService);
    repository = app.get<LoyaltyAccountRepositoryPort>(LOYALTY_ACCOUNT_REPOSITORY_PORT);

    kafka = new Kafka({
      clientId: 'p2-workload-producer',
      brokers: config.kafkaBrokers,
    });

    producer = kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner,
    });
    await producer.connect();

    // Allow consumer to complete partition assignment across all partitions
    await new Promise((resolve) => setTimeout(resolve, 3000));
  }, 30000);

  afterAll(async () => {
    if (producer) {
      await producer.disconnect();
    }
    if (app) {
      await app.close();
    }
  }, 10000);

  it('proves same partition and ascending offsets for U001, multi-partition distribution, and 82 points calculation', async () => {
    const customerU001 = `U001_${Date.now()}`;
    const simultaneousCustomers = Array.from(
      { length: 20 },
      (_, i) => `U${String(i + 2).padStart(3, '0')}_${Date.now()}`,
    );

    // Track record metadata per event
    const u001Results: RecordMetadata[] = [];
    const allPartitionCounts = new Map<number, number>();

    // Step 1: Send U001 seq 1 ($120.00 / 12,000 cents)
    const env1 = createPurchaseCompletedEnvelope({
      eventId: `evt_${Date.now()}_1`,
      customerId: customerU001,
      purchaseId: `P001_${Date.now()}`,
      amountCents: 12_000,
      customerSequence: 1,
      correlationId: 'req-p2-001',
    });
    const res1 = await producer.send({
      topic: config.purchaseTopic,
      messages: [{ key: getEventPartitionKey(env1), value: serializeEnvelope(env1) }],
      acks: -1,
    });
    u001Results.push(...res1);

    // Step 2: Send simultaneous events for 20 other customers interleaved
    const otherSends = simultaneousCustomers.map(async (custId, idx) => {
      const otherEnv = createPurchaseCompletedEnvelope({
        eventId: `evt_other_${idx}_${Date.now()}`,
        customerId: custId,
        purchaseId: `P_other_${idx}`,
        amountCents: 5_000 + idx * 500,
        customerSequence: 1,
        correlationId: `req-other-${idx}`,
      });
      return producer.send({
        topic: config.purchaseTopic,
        messages: [{ key: getEventPartitionKey(otherEnv), value: serializeEnvelope(otherEnv) }],
        acks: -1,
      });
    });

    // Step 3: Send U001 seq 2 ($200.00 / 20,000 cents)
    const env2 = createPurchaseCompletedEnvelope({
      eventId: `evt_${Date.now()}_2`,
      customerId: customerU001,
      purchaseId: `P002_${Date.now()}`,
      amountCents: 20_000,
      customerSequence: 2,
      correlationId: 'req-p2-002',
    });
    const res2 = await producer.send({
      topic: config.purchaseTopic,
      messages: [{ key: getEventPartitionKey(env2), value: serializeEnvelope(env2) }],
      acks: -1,
    });
    u001Results.push(...res2);

    // Await simultaneous batch
    const otherResults = await Promise.all(otherSends);
    for (const batch of otherResults) {
      for (const rec of batch) {
        const count = allPartitionCounts.get(rec.partition) ?? 0;
        allPartitionCounts.set(rec.partition, count + 1);
      }
    }

    // Step 4: Send U001 seq 3 ($100.00 / 10,000 cents)
    const env3 = createPurchaseCompletedEnvelope({
      eventId: `evt_${Date.now()}_3`,
      customerId: customerU001,
      purchaseId: `P003_${Date.now()}`,
      amountCents: 10_000,
      customerSequence: 3,
      correlationId: 'req-p2-003',
    });
    const res3 = await producer.send({
      topic: config.purchaseTopic,
      messages: [{ key: getEventPartitionKey(env3), value: serializeEnvelope(env3) }],
      acks: -1,
    });
    u001Results.push(...res3);

    for (const rec of u001Results) {
      const count = allPartitionCounts.get(rec.partition) ?? 0;
      allPartitionCounts.set(rec.partition, count + 1);
    }

    // --- PROOF 1: U001 records all land on the EXACT same partition ---
    const [meta1, meta2, meta3] = u001Results;
    expect(meta1).toBeDefined();
    expect(meta2).toBeDefined();
    expect(meta3).toBeDefined();

    expect(meta2?.partition).toBe(meta1?.partition);
    expect(meta3?.partition).toBe(meta1?.partition);

    // --- PROOF 2: U001 offsets on that partition are strictly ascending ---
    const offset1 = BigInt(meta1?.baseOffset ?? meta1?.offset ?? '0');
    const offset2 = BigInt(meta2?.baseOffset ?? meta2?.offset ?? '0');
    const offset3 = BigInt(meta3?.baseOffset ?? meta3?.offset ?? '0');

    expect(offset2 > offset1).toBe(true);
    expect(offset3 > offset2).toBe(true);

    // --- PROOF 3: Workload is distributed across multiple partitions ---
    expect(allPartitionCounts.size).toBeGreaterThan(1);

    // --- PROOF 4: Loyalty Worker calculates exactly 82 points for U001 ---
    const startTime = Date.now();
    let account = null;

    while (Date.now() - startTime < 15000) {
      account = await repository.findByCustomerId(customerU001);
      if (account && account.pointsBalance === 82) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 300));
    }

    expect(account).not.toBeNull();
    expect(account?.customerId).toBe(customerU001);
    expect(account?.pointsBalance).toBe(82);
    expect(account?.tier).toBe('GOLD');
    expect(account?.totalEligibleSpendCents).toBe(42_000);
  }, 30000);
});
