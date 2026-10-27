import 'reflect-metadata';
import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Consumer, Kafka } from 'kafkajs';
import { describe, expect, it, afterAll, beforeAll } from 'vitest';
import { deserializeEnvelope } from '@lab/contracts';
import { AppModule } from '../../src/app.module';
import { AppConfigService } from '../../src/config/app-config.service';
import { PurchasesController } from '../../src/purchases/interfaces/http/purchases.controller';

describe('Kafka Producer Integration Test', () => {
  let app: INestApplication;
  let controller: PurchasesController;
  let config: AppConfigService;
  let kafka: Kafka;
  let consumer: Consumer;

  beforeAll(async () => {
    const moduleRef: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.init();

    controller = app.get(PurchasesController);
    config = app.get(AppConfigService);

    kafka = new Kafka({
      clientId: 'integration-test-consumer',
      brokers: config.kafkaBrokers,
    });

    const groupId = `test-verifier-${Date.now()}`;
    consumer = kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic: config.purchaseTopic, fromBeginning: false });
  }, 30000);

  afterAll(async () => {
    if (consumer) {
      await consumer.disconnect();
    }
    if (app) {
      await app.close();
    }
  }, 10000);

  it('publishes a PurchaseCompleted event to Kafka and verifies record key and metadata', async () => {
    const testCustomerId = `CUST_TEST_${Date.now()}`;
    const testAmountCents = 7500;

    // Collect received Kafka record in promise
    const receivedRecordPromise = new Promise<{
      key: string;
      value: string;
      partition: number;
      offset: string;
    }>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Timeout waiting for Kafka record'));
      }, 15000);

      consumer
        .run({
          eachMessage: async ({ topic: _t, partition, message }) => {
            const key = message.key?.toString() ?? '';
            if (key === testCustomerId) {
              clearTimeout(timeout);
              resolve({
                key,
                value: message.value?.toString() ?? '',
                partition,
                offset: message.offset,
              });
            }
          },
        })
        .catch(reject);
    });

    // Small delay to ensure consumer group rebalance has completed
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Execute purchase via controller
    const response = await controller.createPurchase({
      customerId: testCustomerId,
      amountCents: testAmountCents,
    });

    // Verify HTTP 202 Accepted response shape
    expect(response.requestStatus).toBe('ACCEPTED');
    expect(response.purchaseId).toMatch(/^pur_/);
    expect(typeof response.eventId).toBe('string');
    expect(typeof response.correlationId).toBe('string');
    expect(typeof response.partition).toBe('number');
    expect(typeof response.offset).toBe('string');

    // Await message confirmation from Kafka consumer
    const receivedRecord = await receivedRecordPromise;

    // Invariant Proof: Kafka key equals customerId
    expect(receivedRecord.key).toBe(testCustomerId);
    expect(receivedRecord.partition).toBe(response.partition);
    expect(receivedRecord.offset).toBe(response.offset);

    // Invariant Proof: Payload and envelope deserialization
    const envelope = deserializeEnvelope(receivedRecord.value);
    expect(envelope.schemaVersion).toBe(1);
    expect(envelope.eventType).toBe('PurchaseCompleted');
    expect(envelope.aggregateId).toBe(testCustomerId);
    expect(envelope.eventId).toBe(response.eventId);
    expect(envelope.correlationId).toBe(response.correlationId);

    if (envelope.eventType === 'PurchaseCompleted') {
      expect(envelope.payload.purchaseId).toBe(response.purchaseId);
      expect(envelope.payload.customerId).toBe(testCustomerId);
      expect(envelope.payload.amountCents).toBe(testAmountCents);
    }
  }, 25000);
});
