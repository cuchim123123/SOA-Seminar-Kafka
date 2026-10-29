import 'reflect-metadata';
import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  createPurchaseCompletedEnvelope,
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

describe('Bounded Replay Integration Test (P5-02)', () => {
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
        PORT: '4009',
        REPLAY_WORKER_PORT: '4009',
        KAFKA_BROKERS: 'localhost:9092',
        KAFKA_CLIENT_ID: `replay-integ-${Date.now()}`,
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
      clientId: `replay-test-client-${Date.now()}`,
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

  it('scans retained records up to captured high-watermarks with an on-demand group without altering live consumer groups', async () => {
    const topic = config.purchaseTopic;
    const testCustId = `U_REPLAY_${Date.now()}`;

    // 1. Publish 3 test events to Kafka
    for (let seq = 1; seq <= 3; seq++) {
      const envelope = createPurchaseCompletedEnvelope({
        customerId: testCustId,
        customerSequence: seq,
        amountCents: 10000,
      });

      await producer.send({
        topic,
        messages: [
          {
            key: getEventPartitionKey(envelope),
            value: serializeEnvelope(envelope),
          },
        ],
      });
    }

    // 2. Capture baseline offsets for live consumer groups (e.g. loyalty-v1, analytics-v1)
    const getGroupOffsets = async (groupId: string) => {
      try {
        const offsets = await admin.fetchOffsets({ groupId, topics: [topic] });
        const topicOffsets = offsets.find((o) => o.topic === topic);
        const map: Record<number, string> = {};
        topicOffsets?.partitions.forEach((p) => {
          map[p.partition] = p.offset;
        });
        return map;
      } catch {
        return {};
      }
    };

    const initialLoyaltyOffsets = await getGroupOffsets('loyalty-v1');
    const initialAnalyticsOffsets = await getGroupOffsets('analytics-v1');

    // 3. Execute bounded replay with unique runId
    const runId = `test-run-${Date.now()}`;
    const handler = app.get(StartBoundedReplayHandler);

    const result = await handler.execute({
      runId,
      topic,
    });

    // 4. Verify replay run response
    expect(result.id).toBe(runId);
    expect(result.groupId).toBe(`replay-${runId}`);
    expect(result.status).toBe('COMPLETED');
    expect(result.scannedCount).toBeGreaterThanOrEqual(3);
    expect(result.highWatermarks).toBeDefined();

    // 5. Verify isolated staging persistence in loyalty_db
    const savedRun = await prisma.replayRun.findUnique({
      where: { id: runId },
    });
    expect(savedRun).toBeDefined();
    expect(savedRun?.status).toBe('COMPLETED');
    expect(savedRun?.scannedCount).toBe(result.scannedCount);

    // 6. Verify replay group's committed offsets in Kafka Admin match the captured high-watermarks
    const replayOffsets = await getGroupOffsets(`replay-${runId}`);
    for (const [partition, hw] of Object.entries(result.highWatermarks)) {
      expect(replayOffsets[Number(partition)]).toBe(hw);
    }

    // 7. Verify live consumer groups' committed offsets remain strictly untouched
    const afterLoyaltyOffsets = await getGroupOffsets('loyalty-v1');
    const afterAnalyticsOffsets = await getGroupOffsets('analytics-v1');

    expect(afterLoyaltyOffsets).toEqual(initialLoyaltyOffsets);
    expect(afterAnalyticsOffsets).toEqual(initialAnalyticsOffsets);
  }, 45000);
});
