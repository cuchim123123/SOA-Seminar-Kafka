import { describe, expect, it, vi } from 'vitest';
import { StartBoundedReplayHandler } from '../../../src/replay/application/commands/start-bounded-replay.handler';
import { CompareReplayWithLiveHandler } from '../../../src/replay/application/queries/compare-replay-with-live.handler';
import { GetReplayRunHandler } from '../../../src/replay/application/queries/get-replay-run.handler';
import { GetReplayStagingHandler } from '../../../src/replay/application/queries/get-replay-staging.handler';
import { ReplayController } from '../../../src/replay/interfaces/http/replay.controller';

describe('ReplayController', () => {
  it('triggers replay run via startHandler and formats response DTO', async () => {
    const mockStartHandler = {
      execute: vi.fn().mockResolvedValue({
        id: 'run-xyz',
        groupId: 'replay-run-xyz',
        topic: 'purchase.events',
        status: 'COMPLETED',
        startOffsets: { 0: '0' },
        highWatermarks: { 0: '8' },
        scannedCount: 8,
        finalCommittedOffsets: { 0: '8' },
        startedAt: new Date('2026-10-29T10:00:00Z'),
        completedAt: new Date('2026-10-29T10:00:05Z'),
      }),
    } as unknown as StartBoundedReplayHandler;

    const mockGetHandler = {
      execute: vi.fn(),
    } as unknown as GetReplayRunHandler;

    const mockStagingHandler = {
      execute: vi.fn(),
    } as unknown as GetReplayStagingHandler;

    const mockComparisonHandler = {
      execute: vi.fn(),
    } as unknown as CompareReplayWithLiveHandler;

    const controller = new ReplayController(
      mockStartHandler,
      mockGetHandler,
      mockStagingHandler,
      mockComparisonHandler,
    );
    const response = await controller.startReplay({ runId: 'run-xyz' });

    expect(mockStartHandler.execute).toHaveBeenCalled();
    expect(response.id).toBe('run-xyz');
    expect(response.groupId).toBe('replay-run-xyz');
    expect(response.status).toBe('COMPLETED');
    expect(response.scannedCount).toBe(8);
  });

  it('retrieves replay run details via getHandler', async () => {
    const mockStartHandler = {
      execute: vi.fn(),
    } as unknown as StartBoundedReplayHandler;

    const mockGetHandler = {
      execute: vi.fn().mockResolvedValue({
        id: 'run-xyz',
        groupId: 'replay-run-xyz',
        topic: 'purchase.events',
        status: 'COMPLETED',
        startOffsets: { 0: '0' },
        highWatermarks: { 0: '8' },
        scannedCount: 8,
        errorMessage: null,
        startedAt: new Date('2026-10-29T10:00:00Z'),
        completedAt: new Date('2026-10-29T10:00:05Z'),
        createdAt: new Date('2026-10-29T10:00:00Z'),
        updatedAt: new Date('2026-10-29T10:00:05Z'),
      }),
    } as unknown as GetReplayRunHandler;

    const mockStagingHandler = {
      execute: vi.fn(),
    } as unknown as GetReplayStagingHandler;

    const mockComparisonHandler = {
      execute: vi.fn(),
    } as unknown as CompareReplayWithLiveHandler;

    const controller = new ReplayController(
      mockStartHandler,
      mockGetHandler,
      mockStagingHandler,
      mockComparisonHandler,
    );
    const response = await controller.getReplayRun('run-xyz');

    expect(mockGetHandler.execute).toHaveBeenCalled();
    expect(response.id).toBe('run-xyz');
    expect(response.status).toBe('COMPLETED');
  });

  it('retrieves staging records via stagingHandler', async () => {
    const mockStartHandler = {} as unknown as StartBoundedReplayHandler;
    const mockGetHandler = {} as unknown as GetReplayRunHandler;
    const mockStagingHandler = {
      execute: vi.fn().mockResolvedValue([
        {
          id: 'staging-1',
          runId: 'run-xyz',
          customerId: 'U001',
          points: 40,
          tier: 'GOLD',
          eligibleSpendCents: 30000,
          scannedEventCount: 4,
          lastCustomerSequence: 4,
          createdAt: new Date().toISOString(),
        },
      ]),
    } as unknown as GetReplayStagingHandler;
    const mockComparisonHandler = {} as unknown as CompareReplayWithLiveHandler;

    const controller = new ReplayController(
      mockStartHandler,
      mockGetHandler,
      mockStagingHandler,
      mockComparisonHandler,
    );
    const staging = await controller.getStagingRecords('run-xyz');

    expect(mockStagingHandler.execute).toHaveBeenCalled();
    expect(staging).toHaveLength(1);
    expect(staging[0].points).toBe(40);
  });

  it('retrieves comparison via comparisonHandler', async () => {
    const mockStartHandler = {} as unknown as StartBoundedReplayHandler;
    const mockGetHandler = {} as unknown as GetReplayRunHandler;
    const mockStagingHandler = {} as unknown as GetReplayStagingHandler;
    const mockComparisonHandler = {
      execute: vi.fn().mockResolvedValue({
        runId: 'run-xyz',
        customerId: 'U001',
        live: {
          points: 82,
          tier: 'GOLD',
          eligibleSpendCents: 42000,
          lastCustomerSequence: 4,
          correctionRequired: true,
        },
        staged: {
          points: 40,
          tier: 'GOLD',
          eligibleSpendCents: 30000,
          lastCustomerSequence: 4,
          scannedEventCount: 4,
        },
        discrepancy: {
          pointsDifference: -42,
          spendDifferenceCents: -12000,
          tierChanged: false,
        },
      }),
    } as unknown as CompareReplayWithLiveHandler;

    const controller = new ReplayController(
      mockStartHandler,
      mockGetHandler,
      mockStagingHandler,
      mockComparisonHandler,
    );
    const comparison = await controller.compareWithLive('run-xyz', 'U001');

    expect(mockComparisonHandler.execute).toHaveBeenCalled();
    expect(comparison.customerId).toBe('U001');
    expect(comparison.discrepancy.pointsDifference).toBe(-42);
    expect(comparison.live?.points).toBe(82);
    expect(comparison.staged?.points).toBe(40);
  });
});
