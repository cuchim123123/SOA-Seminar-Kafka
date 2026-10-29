import { describe, expect, it, vi } from 'vitest';
import { createPurchaseCompletedEnvelope } from '@lab/contracts';
import { AppConfigService } from '../../../src/config/app-config.service';
import {
  EventPublisherPort,
  PublishResult,
} from '../../../src/publisher/application/ports/event-publisher.port';
import {
  OutboxPublisherRecord,
  OutboxPublisherRepositoryPort,
} from '../../../src/publisher/application/ports/outbox-publisher-repository.port';
import { OutboxPublisherService } from '../../../src/publisher/application/services/outbox-publisher.service';

describe('OutboxPublisherService', () => {
  const mockConfig = {
    nodeEnv: 'test',
    pollIntervalMs: 50,
    batchSize: 10,
    initialBackoffMs: 100,
    maxBackoffMs: 1000,
  } as unknown as AppConfigService;

  it('drains and publishes pending outbox records to Kafka broker and marks them published', async () => {
    const envelope1 = createPurchaseCompletedEnvelope({
      customerId: 'CUST_A',
      customerSequence: 1,
      purchaseId: 'pur_1',
      amountCents: 1000,
      occurredAt: new Date().toISOString(),
      correlationId: '01a18000-0000-7000-8000-000000000001',
    });

    const envelope2 = createPurchaseCompletedEnvelope({
      customerId: 'CUST_A',
      customerSequence: 2,
      purchaseId: 'pur_2',
      amountCents: 2000,
      occurredAt: new Date().toISOString(),
      correlationId: '01a18000-0000-7000-8000-000000000002',
    });

    const pendingRecords: OutboxPublisherRecord[] = [
      {
        eventId: envelope1.eventId,
        aggregateId: 'CUST_A',
        customerSequence: 1,
        eventType: 'PurchaseCompleted',
        envelope: envelope1,
        status: 'PENDING',
        attemptCount: 0,
        createdAt: new Date().toISOString(),
      },
      {
        eventId: envelope2.eventId,
        aggregateId: 'CUST_A',
        customerSequence: 2,
        eventType: 'PurchaseCompleted',
        envelope: envelope2,
        status: 'PENDING',
        attemptCount: 0,
        createdAt: new Date().toISOString(),
      },
    ];

    const mockRepo: OutboxPublisherRepositoryPort = {
      findPending: vi.fn(async () => pendingRecords),
      markPublished: vi.fn(),
      markFailed: vi.fn(),
    };

    const publishedOffsets: string[] = [];
    const mockPublisher: EventPublisherPort = {
      publish: vi.fn(async (env): Promise<PublishResult> => {
        publishedOffsets.push(String(env.customerSequence));
        return {
          topic: 'purchase.events',
          partition: 0,
          offset: String(env.customerSequence),
        };
      }),
    };

    const service = new OutboxPublisherService(mockConfig, mockRepo, mockPublisher);
    const publishedCount = await service.drainBatch();

    expect(publishedCount).toBe(2);
    expect(mockPublisher.publish).toHaveBeenCalledTimes(2);
    expect(publishedOffsets).toEqual(['1', '2']);
    expect(mockRepo.markPublished).toHaveBeenCalledTimes(2);
    expect(mockRepo.markFailed).not.toHaveBeenCalled();

    const stats = service.getStats();
    expect(stats.totalPublished).toBe(2);
    expect(stats.totalErrors).toBe(0);
    expect(stats.lastPublishedEventId).toBe(envelope2.eventId);
  });

  it('stops processing and marks record as failed when Kafka publication fails to preserve customer ordering', async () => {
    const envelope1 = createPurchaseCompletedEnvelope({
      customerId: 'CUST_B',
      customerSequence: 1,
      purchaseId: 'pur_10',
      amountCents: 1500,
      occurredAt: new Date().toISOString(),
      correlationId: '01a18000-0000-7000-8000-000000000010',
    });

    const envelope2 = createPurchaseCompletedEnvelope({
      customerId: 'CUST_B',
      customerSequence: 2,
      purchaseId: 'pur_20',
      amountCents: 2500,
      occurredAt: new Date().toISOString(),
      correlationId: '01a18000-0000-7000-8000-000000000020',
    });

    const pendingRecords: OutboxPublisherRecord[] = [
      {
        eventId: envelope1.eventId,
        aggregateId: 'CUST_B',
        customerSequence: 1,
        eventType: 'PurchaseCompleted',
        envelope: envelope1,
        status: 'PENDING',
        attemptCount: 0,
        createdAt: new Date().toISOString(),
      },
      {
        eventId: envelope2.eventId,
        aggregateId: 'CUST_B',
        customerSequence: 2,
        eventType: 'PurchaseCompleted',
        envelope: envelope2,
        status: 'PENDING',
        attemptCount: 0,
        createdAt: new Date().toISOString(),
      },
    ];

    const mockRepo: OutboxPublisherRepositoryPort = {
      findPending: vi.fn(async () => pendingRecords),
      markPublished: vi.fn(),
      markFailed: vi.fn(),
    };

    const mockPublisher: EventPublisherPort = {
      publish: vi.fn(async () => {
        throw new Error('Kafka broker connection error');
      }),
    };

    const service = new OutboxPublisherService(mockConfig, mockRepo, mockPublisher);
    const publishedCount = await service.drainBatch();

    expect(publishedCount).toBe(0);
    // Sequence 1 failed, so sequence 2 must NOT have been attempted!
    expect(mockPublisher.publish).toHaveBeenCalledTimes(1);
    expect(mockRepo.markFailed).toHaveBeenCalledTimes(1);
    expect(mockRepo.markFailed).toHaveBeenCalledWith(
      envelope1.eventId,
      expect.stringContaining('Kafka broker connection error'),
    );
    expect(mockRepo.markPublished).not.toHaveBeenCalled();

    const stats = service.getStats();
    expect(stats.totalPublished).toBe(0);
    expect(stats.totalErrors).toBe(1);
  });
});
