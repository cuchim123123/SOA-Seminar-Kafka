import { describe, expect, it, vi } from 'vitest';
import { LoyaltyConsumerControlPort } from '../../../src/loyalty/application/ports/consumer-control.port';
import { LoyaltyConsumerControlController } from '../../../src/loyalty/interfaces/http/loyalty-consumer-control.controller';

describe('LoyaltyConsumerControlController', () => {
  it('handles pause, resume, delay, and status correctly', async () => {
    const mockConsumerControl: LoyaltyConsumerControlPort = {
      pause: vi.fn().mockResolvedValue(undefined),
      resume: vi.fn().mockResolvedValue(undefined),
      pausePartition: vi.fn().mockResolvedValue(undefined),
      resumePartition: vi.fn().mockResolvedValue(undefined),
      setProcessingDelay: vi.fn(),
      getProcessingDelay: vi.fn().mockReturnValue(250),
      getStatus: vi.fn().mockResolvedValue({
        groupId: 'loyalty-v1',
        topic: 'purchase.events',
        isPaused: true,
        pausedPartitions: [0],
        processingDelayMs: 250,
        instanceCount: 1,
        instances: [
          {
            instanceId: 'loyalty-worker-primary',
            clientId: 'loyalty-worker',
            isPrimary: true,
            status: 'RUNNING',
            assignedPartitions: [0, 1, 2],
          },
        ],
        totalRebalanceCount: 1,
      }),
      scaleInstances: vi.fn().mockResolvedValue({
        targetCount: 2,
        instances: [],
      }),
      killReplicaInstance: vi.fn().mockResolvedValue({
        killedInstanceId: 'loyalty-worker-replica-1',
        remainingCount: 1,
      }),
      getRebalanceHistory: vi.fn().mockReturnValue([]),
    };

    const controller = new LoyaltyConsumerControlController(mockConsumerControl);

    const pauseRes = await controller.pause();
    expect(mockConsumerControl.pause).toHaveBeenCalled();
    expect(pauseRes).toEqual({ status: 'paused', isPaused: true });

    const resumeRes = await controller.resume();
    expect(mockConsumerControl.resume).toHaveBeenCalled();
    expect(resumeRes).toEqual({ status: 'resumed', isPaused: false });

    const pausePRes = await controller.pausePartition(2);
    expect(mockConsumerControl.pausePartition).toHaveBeenCalledWith(2);
    expect(pausePRes).toEqual({ status: 'paused', partition: 2, isPaused: true });

    const resumePRes = await controller.resumePartition(2);
    expect(mockConsumerControl.resumePartition).toHaveBeenCalledWith(2);
    expect(resumePRes).toEqual({ status: 'resumed', partition: 2, isPaused: false });

    const delayRes = await controller.setDelay({ delayMs: 250 });
    expect(mockConsumerControl.setProcessingDelay).toHaveBeenCalledWith(250);
    expect(delayRes).toEqual({ status: 'updated', processingDelayMs: 250 });

    const statusRes = await controller.getStatus();
    expect(mockConsumerControl.getStatus).toHaveBeenCalled();
    expect(statusRes.processingDelayMs).toBe(250);
    expect(statusRes.isPaused).toBe(true);

    const scaleRes = await controller.scale({ targetInstances: 2 });
    expect(mockConsumerControl.scaleInstances).toHaveBeenCalledWith(2);
    expect(scaleRes.targetCount).toBe(2);

    const killRes = await controller.killReplica({ instanceId: 'loyalty-worker-replica-1' });
    expect(mockConsumerControl.killReplicaInstance).toHaveBeenCalledWith(
      'loyalty-worker-replica-1',
    );
    expect(killRes.killedInstanceId).toBe('loyalty-worker-replica-1');

    const historyRes = controller.getRebalanceHistory();
    expect(mockConsumerControl.getRebalanceHistory).toHaveBeenCalled();
    expect(historyRes).toEqual([]);
  });
});
