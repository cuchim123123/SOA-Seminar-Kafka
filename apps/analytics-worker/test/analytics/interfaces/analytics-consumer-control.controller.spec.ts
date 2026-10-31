import { describe, expect, it, vi } from 'vitest';
import { PauseAnalyticsConsumerHandler } from '../../../src/analytics/application/commands/pause-analytics-consumer';
import { ResumeAnalyticsConsumerHandler } from '../../../src/analytics/application/commands/resume-analytics-consumer';
import { ConsumerControlPort } from '../../../src/analytics/application/ports/consumer-control.port';
import { GetConsumerControlStatusHandler } from '../../../src/analytics/application/queries/get-consumer-control-status';
import { AnalyticsConsumerControlController } from '../../../src/analytics/interfaces/http/analytics-consumer-control.controller';

describe('AnalyticsConsumerControlController', () => {
  it('handles pause, resume, and status endpoints correctly', async () => {
    const mockPauseHandler = {
      execute: vi.fn().mockResolvedValue({ isPaused: true }),
    } as unknown as PauseAnalyticsConsumerHandler;

    const mockResumeHandler = {
      execute: vi.fn().mockResolvedValue({ isPaused: false }),
    } as unknown as ResumeAnalyticsConsumerHandler;

    const mockStatusHandler = {
      execute: vi.fn().mockResolvedValue({
        groupId: 'analytics-v1',
        topic: 'purchase.events',
        isPaused: true,
        pausedPartitions: [0],
      }),
    } as unknown as GetConsumerControlStatusHandler;

    const mockConsumerControl = {
      setProcessingDelay: vi.fn(),
      getProcessingDelay: vi.fn().mockReturnValue(150),
      pausePartition: vi.fn().mockResolvedValue(undefined),
      resumePartition: vi.fn().mockResolvedValue(undefined),
    } as unknown as ConsumerControlPort;

    const controller = new AnalyticsConsumerControlController(
      mockPauseHandler,
      mockResumeHandler,
      mockStatusHandler,
      mockConsumerControl,
    );

    const pauseResult = await controller.pause();
    expect(pauseResult).toEqual({ status: 'paused', isPaused: true });

    const resumeResult = await controller.resume();
    expect(resumeResult).toEqual({ status: 'resumed', isPaused: false });

    const pausePartitionResult = await controller.pausePartition(2);
    expect(mockConsumerControl.pausePartition).toHaveBeenCalledWith(2);
    expect(pausePartitionResult).toEqual({ status: 'paused', partition: 2, isPaused: true });

    const resumePartitionResult = await controller.resumePartition(2);
    expect(mockConsumerControl.resumePartition).toHaveBeenCalledWith(2);
    expect(resumePartitionResult).toEqual({ status: 'resumed', partition: 2, isPaused: false });

    const delayResult = await controller.setDelay({ delayMs: 150 });
    expect(mockConsumerControl.setProcessingDelay).toHaveBeenCalledWith(150);
    expect(delayResult).toEqual({ status: 'updated', processingDelayMs: 150 });

    const statusResult = await controller.getStatus();
    expect(statusResult).toEqual({
      groupId: 'analytics-v1',
      topic: 'purchase.events',
      isPaused: true,
      pausedPartitions: [0],
    });
  });
});
