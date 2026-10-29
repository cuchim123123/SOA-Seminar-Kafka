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
import { StartBoundedReplayCommand } from '../../src/replay/application/commands/start-bounded-replay.command';
import { StartBoundedReplayHandler } from '../../src/replay/application/commands/start-bounded-replay.handler';
import { InsufficientRetentionError } from '../../src/replay/domain/errors/insufficient-retention.error';

describe('Retention Failure Reporting Integration Test (P5-04)', () => {
  let app: INestApplication;
  let config: AppConfigService;
  let prisma: PrismaService;
  let kafka: Kafka;
  let producer: Producer;
  let admin: Admin;
  let startHandler: StartBoundedReplayHandler;
  const testTopic = `retention-test-${Date.now()}`;
  const createdRunIds: string[] = [];

  beforeAll(async () => {
    const moduleRef: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    })
      .overrideProvider(RAW_ENV_TOKEN)
      .useValue({
        NODE_ENV: 'test',
        PORT: '4011',
        REPLAY_WORKER_PORT: '4011',
        KAFKA_BROKERS: 'localhost:9092',
        KAFKA_CLIENT_ID: `retention-test-client-${Date.now()}`,
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
    startHandler = app.get(StartBoundedReplayHandler);

    kafka = new Kafka({
      clientId: `retention-admin-client-${Date.now()}`,
      brokers: config.kafkaBrokers,
    });

    producer = kafka.producer();
    admin = kafka.admin();
    await producer.connect();
    await admin.connect();

    // 1. Create a dedicated test topic with 2 partitions for controlled retention testing
    await admin.createTopics({
      topics: [
        {
          topic: testTopic,
          numPartitions: 2,
          replicationFactor: 1,
        },
      ],
    });

    // Allow metadata propagation
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }, 30000);

  afterAll(async () => {
    try {
      if (admin) {
        await admin.deleteTopics({ topics: [testTopic] }).catch(() => {});
        await admin.disconnect();
      }
      if (producer) {
        await producer.disconnect();
      }
      if (prisma && createdRunIds.length > 0) {
        await prisma.replayRun
          .deleteMany({
            where: { id: { in: createdRunIds } },
          })
          .catch(() => {});
      }
      if (app) {
        await app.close();
      }
    } catch {
      // Ignore cleanup errors
    }
  });

  it('detects insufficient retained history, marks run as FAILED in database, and throws structured InsufficientRetentionError', async () => {
    // 1. Produce 5 messages to partition 0 and 5 messages to partition 1
    for (let i = 0; i < 5; i++) {
      const e0 = createPurchaseCompletedEnvelope({
        customerId: `CUST_P0_${i}`,
        customerSequence: i + 1,
        amountCents: 1000,
      });
      await producer.send({
        topic: testTopic,
        messages: [
          {
            partition: 0,
            key: getEventPartitionKey(e0),
            value: serializeEnvelope(e0),
          },
        ],
      });

      const e1 = createPurchaseCompletedEnvelope({
        customerId: `CUST_P1_${i}`,
        customerSequence: i + 1,
        amountCents: 2000,
      });
      await producer.send({
        topic: testTopic,
        messages: [
          {
            partition: 1,
            key: getEventPartitionKey(e1),
            value: serializeEnvelope(e1),
          },
        ],
      });
    }

    // 2. Perform controlled truncation: purge records 0..2 on partition 0 (advancing logStartOffset to 3)
    await admin.fetchTopicMetadata({ topics: [testTopic] });
    await admin.deleteTopicRecords({
      topic: testTopic,
      partitions: [{ partition: 0, offset: '3' }],
    });

    // 3. Trigger replay requiring full history from default offset 0
    const runId = `retention-fail-run-${Date.now()}`;
    createdRunIds.push(runId);

    let caughtError: unknown;
    try {
      await startHandler.execute(
        new StartBoundedReplayCommand({
          runId,
          topic: testTopic,
        }),
      );
    } catch (err) {
      caughtError = err;
    }

    // 4. Verification Proofs:
    expect(caughtError).toBeInstanceOf(InsufficientRetentionError);
    const retentionError = caughtError as InsufficientRetentionError;

    expect(retentionError.message).toContain(
      `Insufficient retained history for topic '${testTopic}'`,
    );
    expect(retentionError.message).toContain(
      'partition 0 earliest retained offset is 3 (required: 0, purged: 3)',
    );

    const diagnostic = retentionError.retentionDetails.truncatedPartitions.find(
      (p) => p.partition === 0,
    );
    expect(diagnostic).toBeDefined();
    expect(diagnostic?.earliestRetainedOffset).toBe('3');
    expect(diagnostic?.requiredStartOffset).toBe('0');
    expect(diagnostic?.purgedOffsetCount).toBe('3');

    // 5. Verify database persistence in replay_runs: status is FAILED and diagnostics recorded
    const persistedRun = await prisma.replayRun.findUnique({
      where: { id: runId },
    });

    expect(persistedRun).not.toBeNull();
    expect(persistedRun?.status).toBe('FAILED');
    expect(persistedRun?.scannedCount).toBe(0);
    expect(persistedRun?.errorMessage).toContain('partition 0 earliest retained offset is 3');
    expect(persistedRun?.completedAt).not.toBeNull();

    const startOffsets = persistedRun?.startOffsets as Record<string, string>;
    expect(startOffsets['0']).toBe('3');
    expect(startOffsets['1']).toBe('0');
  });

  it('succeeds when custom requiredStartOffsets accommodates the truncated retention horizon', async () => {
    const successRunId = `retention-success-run-${Date.now()}`;
    createdRunIds.push(successRunId);

    // Provide requiredStartOffsets: partition 0 starts at 3 (matching retained), partition 1 at 0
    const result = await startHandler.execute(
      new StartBoundedReplayCommand({
        runId: successRunId,
        topic: testTopic,
        requiredStartOffsets: {
          0: '3',
          1: '0',
        },
      }),
    );

    expect(result.id).toBe(successRunId);
    expect(result.status).toBe('COMPLETED');
    expect(result.scannedCount).toBeGreaterThan(0);

    const persistedRun = await prisma.replayRun.findUnique({
      where: { id: successRunId },
    });
    expect(persistedRun?.status).toBe('COMPLETED');
    expect(persistedRun?.errorMessage).toBeNull();
  });
});
