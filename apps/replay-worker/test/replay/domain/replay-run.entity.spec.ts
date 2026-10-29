import { describe, expect, it } from 'vitest';
import { ReplayRun } from '../../../src/replay/domain/replay-run.entity';

describe('ReplayRun Entity', () => {
  it('creates a new pending replay run with boundaries', () => {
    const run = ReplayRun.create({
      id: 'run-1',
      groupId: 'replay-run-1',
      topic: 'purchase.events',
      startOffsets: { 0: '0', 1: '0' },
      highWatermarks: { 0: '5', 1: '10' },
    });

    expect(run.id).toBe('run-1');
    expect(run.groupId).toBe('replay-run-1');
    expect(run.status).toBe('PENDING');
    expect(run.startOffsets).toEqual({ 0: '0', 1: '0' });
    expect(run.highWatermarks).toEqual({ 0: '5', 1: '10' });
    expect(run.scannedCount).toBe(0);
    expect(run.startedAt).toBeUndefined();
    expect(run.completedAt).toBeUndefined();
  });

  it('transitions through lifecycle: PENDING -> RUNNING -> COMPLETED', () => {
    const run = ReplayRun.create({
      id: 'run-2',
      groupId: 'replay-run-2',
      topic: 'purchase.events',
      startOffsets: { 0: '0' },
      highWatermarks: { 0: '3' },
    });

    run.start();
    expect(run.status).toBe('RUNNING');
    expect(run.startedAt).toBeDefined();

    run.recordScan(3);
    expect(run.scannedCount).toBe(3);

    run.complete();
    expect(run.status).toBe('COMPLETED');
    expect(run.completedAt).toBeDefined();
  });

  it('fails with reason when error occurs', () => {
    const run = ReplayRun.create({
      id: 'run-3',
      groupId: 'replay-run-3',
      topic: 'purchase.events',
      startOffsets: { 0: '0' },
      highWatermarks: { 0: '3' },
    });

    run.start();
    run.fail('Kafka broker timeout');

    expect(run.status).toBe('FAILED');
    expect(run.errorMessage).toBe('Kafka broker timeout');
    expect(run.completedAt).toBeDefined();
  });
});
