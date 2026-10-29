import { describe, expect, it, vi } from 'vitest';
import { StartBoundedReplayHandler } from '../../../src/replay/application/commands/start-bounded-replay.handler';
import { GetReplayRunHandler } from '../../../src/replay/application/queries/get-replay-run.handler';
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

    const controller = new ReplayController(mockStartHandler, mockGetHandler);
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

    const controller = new ReplayController(mockStartHandler, mockGetHandler);
    const response = await controller.getReplayRun('run-xyz');

    expect(mockGetHandler.execute).toHaveBeenCalled();
    expect(response.id).toBe('run-xyz');
    expect(response.status).toBe('COMPLETED');
  });
});
