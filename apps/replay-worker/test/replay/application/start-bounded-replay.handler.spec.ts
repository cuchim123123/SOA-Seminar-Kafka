import { describe, expect, it, vi } from 'vitest';
import { StartBoundedReplayCommand } from '../../../src/replay/application/commands/start-bounded-replay.command';
import { StartBoundedReplayHandler } from '../../../src/replay/application/commands/start-bounded-replay.handler';
import { BoundedReplayScannerPort } from '../../../src/replay/application/ports/bounded-replay-scanner.port';
import { ReplayRunRepositoryPort } from '../../../src/replay/application/ports/replay-run-repository.port';
import { TopicBoundaryInspectorPort } from '../../../src/replay/application/ports/topic-boundary-inspector.port';
import { ReplayRun } from '../../../src/replay/domain/replay-run.entity';

describe('StartBoundedReplayHandler', () => {
  it('captures boundaries, initializes on-demand group, and runs bounded scan', async () => {
    const savedRuns: ReplayRun[] = [];

    const mockRepo: ReplayRunRepositoryPort = {
      save: vi.fn().mockImplementation(async (run: ReplayRun) => {
        savedRuns.push(run);
      }),
      findById: vi.fn(),
      findLatest: vi.fn(),
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
        // simulate scanning 10 records
        for (let i = 0; i < 10; i++) {
          if (options.onRecord) {
            await options.onRecord({
              topic: options.topic,
              partition: 0,
              offset: String(i),
              key: 'cust-1',
              timestamp: '123',
              value: {},
            });
          }
        }
        return {
          scannedCount: 10,
          finalCommittedOffsets: options.highWatermarks,
        };
      }),
    };

    const handler = new StartBoundedReplayHandler(mockRepo, mockBoundaryInspector, mockScanner);

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
    expect(result.scannedCount).toBe(10);
    expect(result.highWatermarks).toEqual({ 0: '4', 1: '6' });
    expect(result.finalCommittedOffsets).toEqual({ 0: '4', 1: '6' });
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

    const handler = new StartBoundedReplayHandler(mockRepo, mockBoundaryInspector, mockScanner);

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
