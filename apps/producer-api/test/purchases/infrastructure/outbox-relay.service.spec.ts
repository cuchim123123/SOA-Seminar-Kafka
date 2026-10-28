import { describe, expect, it, vi } from 'vitest';
import { createPurchaseCompletedFixture } from '@lab/contracts';
import {
  EventPublisherPort,
  PublishResult,
} from '../../../src/purchases/application/ports/event-publisher.port';
import {
  OutboxRecord,
  OutboxRepositoryPort,
} from '../../../src/purchases/application/ports/outbox-repository.port';
import { OutboxRelayService } from '../../../src/purchases/infrastructure/services/outbox-relay.service';

describe('OutboxRelayService', () => {
  it('relays pending outbox events to Kafka and marks them published upon success', async () => {
    const pendingFixture: OutboxRecord = {
      eventId: '01a18192-9c94-7d7b-952b-02265a63bac4',
      aggregateId: 'U001',
      customerSequence: 1,
      eventType: 'PurchaseCompleted',
      envelope: createPurchaseCompletedFixture({
        eventId: '01a18192-9c94-7d7b-952b-02265a63bac4',
        aggregateId: 'U001',
      }),
      status: 'PENDING',
      attemptCount: 0,
      createdAt: new Date().toISOString(),
    };

    const mockOutboxRepo: OutboxRepositoryPort = {
      save: vi.fn(),
      findPending: vi.fn(async () => [pendingFixture]),
      markPublished: vi.fn(async () => {}),
      markFailed: vi.fn(),
    };

    const mockPublisher: EventPublisherPort = {
      publish: vi.fn(async (envelope): Promise<PublishResult> => ({
        topic: 'purchase.events',
        partition: 0,
        offset: '10',
        eventId: envelope.eventId,
        correlationId: envelope.correlationId,
      })),
    };

    const relayService = new OutboxRelayService(mockOutboxRepo, mockPublisher);
    const count = await relayService.processPending();

    expect(count).toBe(1);
    expect(mockPublisher.publish).toHaveBeenCalledWith(pendingFixture.envelope);
    expect(mockOutboxRepo.markPublished).toHaveBeenCalledWith(
      pendingFixture.eventId,
      expect.any(String),
    );
  });

  it('stops processing and marks record as failed when Kafka publication errors to preserve ordering', async () => {
    const record1: OutboxRecord = {
      eventId: 'evt-1',
      aggregateId: 'U001',
      customerSequence: 1,
      eventType: 'PurchaseCompleted',
      envelope: createPurchaseCompletedFixture({ eventId: 'evt-1' }),
      status: 'PENDING',
      attemptCount: 0,
      createdAt: new Date().toISOString(),
    };

    const record2: OutboxRecord = {
      eventId: 'evt-2',
      aggregateId: 'U001',
      customerSequence: 2,
      eventType: 'PurchaseCompleted',
      envelope: createPurchaseCompletedFixture({ eventId: 'evt-2' }),
      status: 'PENDING',
      attemptCount: 0,
      createdAt: new Date().toISOString(),
    };

    const mockOutboxRepo: OutboxRepositoryPort = {
      save: vi.fn(),
      findPending: vi.fn(async () => [record1, record2]),
      markPublished: vi.fn(),
      markFailed: vi.fn(async () => {}),
    };

    const mockPublisher: EventPublisherPort = {
      publish: vi.fn(async () => {
        throw new Error('Kafka broker connection error');
      }),
    };

    const relayService = new OutboxRelayService(mockOutboxRepo, mockPublisher);
    const count = await relayService.processPending();

    expect(count).toBe(0);
    expect(mockPublisher.publish).toHaveBeenCalledTimes(1);
    expect(mockOutboxRepo.markFailed).toHaveBeenCalledWith(
      'evt-1',
      'Kafka broker connection error',
    );
    expect(mockOutboxRepo.markPublished).not.toHaveBeenCalled();
  });
});
