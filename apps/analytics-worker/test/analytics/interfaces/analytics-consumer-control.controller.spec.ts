import { describe, expect, it, vi } from 'vitest';
import { PauseAnalyticsConsumerHandler } from '../../../src/analytics/application/commands/pause-analytics-consumer.handler';
import { ResumeAnalyticsConsumerHandler } from '../../../src/analytics/application/commands/resume-analytics-consumer.handler';
import { GetConsumerControlStatusHandler } from '../../../src/analytics/application/queries/get-consumer-control-status.handler';
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

    const controller = new AnalyticsConsumerControlController(
      mockPauseHandler,
      mockResumeHandler,
      mockStatusHandler,
    );

    const pauseResult = await controller.pause();
    expect(pauseResult).toEqual({ status: 'paused', isPaused: true });

    const resumeResult = await controller.resume();
    expect(resumeResult).toEqual({ status: 'resumed', isPaused: false });

    const statusResult = await controller.getStatus();
    expect(statusResult).toEqual({
      groupId: 'analytics-v1',
      topic: 'purchase.events',
      isPaused: true,
      pausedPartitions: [0],
    });
  });
});
