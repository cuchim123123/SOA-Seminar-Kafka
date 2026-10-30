import { describe, expect, it, vi } from 'vitest';
import { LoyaltyConsumerControlPort } from '../../../src/loyalty/application/ports/consumer-control.port';
import { LoyaltyConsumerControlController } from '../../../src/loyalty/interfaces/http/loyalty-consumer-control.controller';

describe('LoyaltyConsumerControlController', () => {
  it('handles pause, resume, delay, and status correctly', async () => {
    const mockConsumerControl: LoyaltyConsumerControlPort = {
      pause: vi.fn().mockResolvedValue(undefined),
      resume: vi.fn().mockResolvedValue(undefined),
      setProcessingDelay: vi.fn(),
      getProcessingDelay: vi.fn().mockReturnValue(250),
      getStatus: vi.fn().mockResolvedValue({
        groupId: 'loyalty-v1',
        topic: 'purchase.events',
        isPaused: true,
        pausedPartitions: [0],
        processingDelayMs: 250,
      }),
    };

    const controller = new LoyaltyConsumerControlController(mockConsumerControl);

    const pauseRes = await controller.pause();
    expect(mockConsumerControl.pause).toHaveBeenCalled();
    expect(pauseRes).toEqual({ status: 'paused', isPaused: true });

    const resumeRes = await controller.resume();
    expect(mockConsumerControl.resume).toHaveBeenCalled();
    expect(resumeRes).toEqual({ status: 'resumed', isPaused: false });

    const delayRes = await controller.setDelay({ delayMs: 250 });
    expect(mockConsumerControl.setProcessingDelay).toHaveBeenCalledWith(250);
    expect(delayRes).toEqual({ status: 'updated', processingDelayMs: 250 });

    const statusRes = await controller.getStatus();
    expect(mockConsumerControl.getStatus).toHaveBeenCalled();
    expect(statusRes.processingDelayMs).toBe(250);
    expect(statusRes.isPaused).toBe(true);
  });
});
