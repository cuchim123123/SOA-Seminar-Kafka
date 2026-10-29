import { describe, expect, it, vi } from 'vitest';
import { StartBoundedReplayCommand } from '../../../src/replay/application/commands/start-bounded-replay.command';
import { StartBoundedReplayHandler } from '../../../src/replay/application/commands/start-bounded-replay.handler';
import { BoundedReplayScannerPort } from '../../../src/replay/application/ports/bounded-replay-scanner.port';
import { ReplayRunRepositoryPort } from '../../../src/replay/application/ports/replay-run-repository.port';
import { ReplayStagingRepositoryPort } from '../../../src/replay/application/ports/replay-staging-repository.port';
import { TopicBoundaryInspectorPort } from '../../../src/replay/application/ports/topic-boundary-inspector.port';
import { ReplayRun } from '../../../src/replay/domain/replay-run.entity';
import { ReplayStagingRecord } from '../../../src/replay/domain/replay-staging-record.entity';

describe('StartBoundedReplayHandler', () => {
  it('captures boundaries, initializes on-demand group, runs bounded scan and performs two-pass correction', async () => {
    const savedRuns: ReplayRun[] = [];
    const savedStagingRecords: ReplayStagingRecord[] = [];

    const mockRepo: ReplayRunRepositoryPort = {
      save: vi.fn().mockImplementation(async (run: ReplayRun) => {
        savedRuns.push(run);
      }),
      findById: vi.fn(),
      findLatest: vi.fn(),
    };

    const mockStagingRepo: ReplayStagingRepositoryPort = {
      upsertRecord: vi.fn().mockImplementation(async (record: ReplayStagingRecord) => {
        savedStagingRecords.push(record);
      }),
      findByRunId: vi.fn().mockResolvedValue([]),
    };

    const mockBoundaryInspector: TopicBoundaryInspectorPort = {
      getTopicBoundaries: vi.fn().mockResolvedValue({
        topic: 'purchase.events',
        startOffsets: { 0: '0', 1: '0' },
        highWatermarks: { 0: '4', 1: '6' },
      }),
    };

    const mockScanner: BoundedReplayScannerPort = {
      scanBoundedTopic: vi.fn().mockImplementation(async (options) => {
        // simulate scanning 3 purchases and 1 invalidation
        const records = [
          {
            topic: options.topic,
            partition: 0,
            offset: '0',
            key: 'U001',
            timestamp: '100',
            value: {
              eventType: 'PurchaseCompleted',
              aggregateId: 'U001',
              customerSequence: 1,
              payload: { customerId: 'U001', purchaseId: 'P001', amountCents: 12000 },
            },
          },
          {
            topic: options.topic,
            partition: 0,
            offset: '1',
            key: 'U001',
            timestamp: '200',
            value: {
              eventType: 'PurchaseCompleted',
              aggregateId: 'U001',
              customerSequence: 2,
              payload: { customerId: 'U001', purchaseId: 'P002', amountCents: 20000 },
            },
          },
          {
            topic: options.topic,
            partition: 0,
            offset: '2',
            key: 'U001',
            timestamp: '300',
            value: {
              eventType: 'PurchaseCompleted',
              aggregateId: 'U001',
              customerSequence: 3,
              payload: { customerId: 'U001', purchaseId: 'P003', amountCents: 10000 },
            },
          },
          {
            topic: options.topic,
            partition: 0,
            offset: '3',
            key: 'U001',
            timestamp: '400',
            value: {
              eventType: 'PurchaseInvalidated',
              aggregateId: 'U001',
              customerSequence: 4,
              payload: { customerId: 'U001', purchaseId: 'P001', reason: 'Returned item' },
            },
          },
        ];

        for (const r of records) {
          if (options.onRecord) {
            await options.onRecord(r);
          }
        }
        return {
          scannedCount: records.length,
          finalCommittedOffsets: options.highWatermarks,
        };
      }),
    };

    const handler = new StartBoundedReplayHandler(
      mockRepo,
      mockStagingRepo,
      mockBoundaryInspector,
      mockScanner,
    );

    const result = await handler.execute(
      new StartBoundedReplayCommand({
        runId: 'test-run-100',
        topic: 'purchase.events',
      }),
    );

    expect(mockBoundaryInspector.getTopicBoundaries).toHaveBeenCalledWith('purchase.events');
    expect(mockScanner.scanBoundedTopic).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: 'purchase.events',
        groupId: 'replay-test-run-100',
        startOffsets: { 0: '0', 1: '0' },
        highWatermarks: { 0: '4', 1: '6' },
      }),
    );

    expect(result.id).toBe('test-run-100');
    expect(result.groupId).toBe('replay-test-run-100');
    expect(result.status).toBe('COMPLETED');
    expect(result.scannedCount).toBe(4);
    expect(result.stagingCount).toBe(1);
    expect(result.highWatermarks).toEqual({ 0: '4', 1: '6' });
    expect(result.finalCommittedOffsets).toEqual({ 0: '4', 1: '6' });

    expect(mockStagingRepo.upsertRecord).toHaveBeenCalledTimes(1);
    const stagingRecord = savedStagingRecords[0];
    expect(stagingRecord.customerId).toBe('U001');
    expect(stagingRecord.points).toBe(40n);
    expect(stagingRecord.tier).toBe('GOLD');
    expect(stagingRecord.eligibleSpendCents).toBe(30000n);
    expect(stagingRecord.lastCustomerSequence).toBe(4n);
  });

  it('marks run as FAILED and rethrows if scanner encounters error', async () => {
    const savedRuns: ReplayRun[] = [];

    const mockRepo: ReplayRunRepositoryPort = {
      save: vi.fn().mockImplementation(async (run: ReplayRun) => {
        savedRuns.push(run);
      }),
      findById: vi.fn(),
      findLatest: vi.fn(),
    };

    const mockStagingRepo: ReplayStagingRepositoryPort = {
      upsertRecord: vi.fn(),
      findByRunId: vi.fn(),
    };

    const mockBoundaryInspector: TopicBoundaryInspectorPort = {
      getTopicBoundaries: vi.fn().mockResolvedValue({
        topic: 'purchase.events',
        startOffsets: { 0: '0' },
        highWatermarks: { 0: '5' },
      }),
    };

    const mockScanner: BoundedReplayScannerPort = {
      scanBoundedTopic: vi.fn().mockRejectedValue(new Error('Connection lost')),
    };

    const handler = new StartBoundedReplayHandler(
      mockRepo,
      mockStagingRepo,
      mockBoundaryInspector,
      mockScanner,
    );

    await expect(
      handler.execute(
        new StartBoundedReplayCommand({
          runId: 'failed-run',
        }),
      ),
    ).rejects.toThrow('Connection lost');

    const lastSaved = savedRuns[savedRuns.length - 1];
    expect(lastSaved.status).toBe('FAILED');
    expect(lastSaved.errorMessage).toBe('Connection lost');
  });
});
