import 'reflect-metadata';
import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  createPurchaseCompletedEnvelope,
  getEventPartitionKey,
  serializeEnvelope,
} from '@lab/contracts';
import { Kafka, Producer } from 'kafkajs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { AppModule } from '../../src/app.module';
import { AppConfigService } from '../../src/config/app-config.service';
import { RAW_ENV_TOKEN } from '../../src/config/config.module';
import {
  LOYALTY_ACCOUNT_REPOSITORY_PORT,
  LoyaltyAccountRepositoryPort,
} from '../../src/loyalty/application/ports/loyalty-account-repository.port';

describe('Loyalty Consumer Kafka Integration Test', () => {
  let app: INestApplication;
  let config: AppConfigService;
  let repository: LoyaltyAccountRepositoryPort;
  let producer: Producer;
  let kafka: Kafka;
  const uniqueGroupId = `loyalty-integ-${Date.now()}`;

  beforeAll(async () => {
    const moduleRef: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    })
      .overrideProvider(RAW_ENV_TOKEN)
      .useValue({
        NODE_ENV: 'test',
        PORT: '4005',
        KAFKA_BROKERS: 'localhost:9092',
        KAFKA_CLIENT_ID: `loyalty-integ-client-${Date.now()}`,
        KAFKA_GROUP_ID: uniqueGroupId,
        PURCHASE_TOPIC: 'purchase.events',
        DATABASE_URL: process.env.DATABASE_URL,
        LOYALTY_DATABASE_URL: process.env.LOYALTY_DATABASE_URL,
      })
      .compile();

    app = moduleRef.createNestApplication();
    await app.init(); // Triggers onModuleInit -> consumer connects & subscribes

    config = app.get(AppConfigService);
    repository = app.get<LoyaltyAccountRepositoryPort>(LOYALTY_ACCOUNT_REPOSITORY_PORT);

    kafka = new Kafka({
      clientId: 'integration-test-producer',
      brokers: config.kafkaBrokers,
    });

    producer = kafka.producer();
    await producer.connect();

    // Allow consumer to complete partition assignment
    await new Promise((resolve) => setTimeout(resolve, 2500));
  }, 30000);

  afterAll(async () => {
    if (producer) {
      await producer.disconnect();
    }
    if (app) {
      await app.close();
    }
  }, 10000);

  it('consumes PurchaseCompleted event from Kafka and applies points calculation to loyalty account', async () => {
    const testCustomerId = `CUST_LOYALTY_${Date.now()}`;
    const testPurchaseId = `pur_${Date.now()}`;
    const testAmountCents = 12_000; // $120.00 -> 12 points at Bronze, tier becomes Silver

    const envelope = createPurchaseCompletedEnvelope({
      eventId: `evt_${Date.now()}`,
      customerId: testCustomerId,
      purchaseId: testPurchaseId,
      amountCents: testAmountCents,
      customerSequence: 1,
      correlationId: '00000000-0000-4000-8000-000000000099',
    });

    const key = getEventPartitionKey(envelope);
    const value = serializeEnvelope(envelope);

    // Publish event directly to Kafka topic
    await producer.send({
      topic: config.purchaseTopic,
      messages: [{ key, value }],
      acks: -1,
    });

    // Poll repository until consumer updates account (up to 15 seconds)
    const startTime = Date.now();
    let updatedAccount = null;

    while (Date.now() - startTime < 15000) {
      updatedAccount = await repository.findByCustomerId(testCustomerId);
      if (updatedAccount) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 300));
    }

    // Verification Proofs:
    expect(updatedAccount).not.toBeNull();
    expect(updatedAccount?.customerId).toBe(testCustomerId);
    expect(updatedAccount?.pointsBalance).toBe(12);
    expect(updatedAccount?.tier).toBe('SILVER');
    expect(updatedAccount?.totalEligibleSpendCents).toBe(12_000);
  }, 25000);
});
