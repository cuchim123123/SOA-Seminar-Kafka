import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createPurchaseCompletedEnvelope,
  deserializeEnvelope,
  IntegrationEventEnvelopeV1,
} from '@lab/contracts';
import { Prisma } from '@prisma/client';
import { Kafka } from 'kafkajs';
import { AppConfigService } from '../../src/config/app-config.service';
import { PrismaService } from '../../src/database/prisma.service';
import { OutboxPublisherService } from '../../src/publisher/application/services/outbox-publisher.service';
import { KafkaEventPublisherAdapter } from '../../src/publisher/infrastructure/adapters/kafka-event-publisher.adapter';
import { PostgresOutboxPublisherRepositoryAdapter } from '../../src/publisher/infrastructure/adapters/postgres-outbox-publisher-repository.adapter';

describe('OutboxPublisher Integration Test (P4-03)', () => {
  let prisma: PrismaService;
  let kafkaPublisherAdapter: KafkaEventPublisherAdapter;
  let publisherService: OutboxPublisherService;
  let kafka: Kafka;
  const config = new AppConfigService();

  beforeAll(async () => {
    prisma = new PrismaService(config);
    await prisma.onModuleInit();

    kafkaPublisherAdapter = new KafkaEventPublisherAdapter(config);
    await kafkaPublisherAdapter.onModuleInit();

    const repo = new PostgresOutboxPublisherRepositoryAdapter(prisma);
    publisherService = new OutboxPublisherService(config, repo, kafkaPublisherAdapter);

    kafka = new Kafka({
      clientId: `test-subscriber-${Date.now()}`,
      brokers: config.kafkaBrokers,
    });
  });

  afterAll(async () => {
    if (publisherService) {
      await publisherService.onModuleDestroy();
    }
    if (kafkaPublisherAdapter) {
      await kafkaPublisherAdapter.onModuleDestroy();
    }
    if (prisma) {
      await prisma.onModuleDestroy();
    }
  });

  it('drains pending outbox records from PostgreSQL and publishes them in sequence to Kafka', async () => {
    const customerA = `U_PUB_A_${Date.now()}`;
    const customerB = `U_PUB_B_${Date.now()}`;

    // Clean up any leftover pending events from earlier disconnected outbox tests
    await prisma.outboxEvent.deleteMany({ where: { status: 'PENDING' } });

    // 1. Seed pending outbox records directly into PostgreSQL
    const envA1 = createPurchaseCompletedEnvelope({
      customerId: customerA,
      customerSequence: 1,
      purchaseId: `pur_${customerA}_1`,
      amountCents: 1000,
      occurredAt: new Date().toISOString(),
    });

    const envA2 = createPurchaseCompletedEnvelope({
      customerId: customerA,
      customerSequence: 2,
      purchaseId: `pur_${customerA}_2`,
      amountCents: 2000,
      occurredAt: new Date().toISOString(),
    });

    const envB1 = createPurchaseCompletedEnvelope({
      customerId: customerB,
      customerSequence: 1,
      purchaseId: `pur_${customerB}_1`,
      amountCents: 3000,
      occurredAt: new Date().toISOString(),
    });

    await prisma.outboxEvent.createMany({
      data: [
        {
          eventId: envA1.eventId,
          aggregateId: customerA,
          customerSequence: BigInt(1),
          eventType: 'PurchaseCompleted',
          payloadJson: envA1 as unknown as Prisma.InputJsonValue,
          status: 'PENDING',
          attemptCount: 0,
        },
        {
          eventId: envA2.eventId,
          aggregateId: customerA,
          customerSequence: BigInt(2),
          eventType: 'PurchaseCompleted',
          payloadJson: envA2 as unknown as Prisma.InputJsonValue,
          status: 'PENDING',
          attemptCount: 0,
        },
        {
          eventId: envB1.eventId,
          aggregateId: customerB,
          customerSequence: BigInt(1),
          eventType: 'PurchaseCompleted',
          payloadJson: envB1 as unknown as Prisma.InputJsonValue,
          status: 'PENDING',
          attemptCount: 0,
        },
      ],
    });

    // 2. Set up a Kafka consumer to capture published messages
    const groupId = `test-verify-publisher-${Date.now()}`;
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic: config.purchaseTopic, fromBeginning: false });

    const receivedMessages: { key: string; envelope: IntegrationEventEnvelopeV1 }[] = [];
    await consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        const env = deserializeEnvelope(message.value.toString('utf8'));
        if (env.aggregateId === customerA || env.aggregateId === customerB) {
          receivedMessages.push({
            key: message.key ? message.key.toString('utf8') : '',
            envelope: env,
          });
        }
      },
    });

    // Allow consumer to establish partition assignment
    await new Promise((resolve) => setTimeout(resolve, 1500));

    // 3. Trigger drainBatch on publisher service
    const publishedCount = await publisherService.drainBatch();
    expect(publishedCount).toBe(3);

    // 4. Verify DB records are updated to PUBLISHED with timestamps
    const dbRecords = await prisma.outboxEvent.findMany({
      where: {
        eventId: { in: [envA1.eventId, envA2.eventId, envB1.eventId] },
      },
    });

    expect(dbRecords).toHaveLength(3);
    for (const record of dbRecords) {
      expect(record.status).toBe('PUBLISHED');
      expect(record.publishedAt).not.toBeNull();
    }

    // 5. Wait for Kafka consumer to collect messages
    const startTime = Date.now();
    while (receivedMessages.length < 3 && Date.now() - startTime < 10000) {
      await new Promise((resolve) => setTimeout(resolve, 300));
    }

    await consumer.disconnect();

    // Verify ordering for Customer A
    const customerAMessages = receivedMessages.filter((m) => m.key === customerA);
    expect(customerAMessages).toHaveLength(2);
    expect(customerAMessages[0]?.envelope.customerSequence).toBe(1);
    expect(customerAMessages[1]?.envelope.customerSequence).toBe(2);

    // Verify Customer B message
    const customerBMessages = receivedMessages.filter((m) => m.key === customerB);
    expect(customerBMessages).toHaveLength(1);
    expect(customerBMessages[0]?.envelope.customerSequence).toBe(1);
  }, 20000);
});
