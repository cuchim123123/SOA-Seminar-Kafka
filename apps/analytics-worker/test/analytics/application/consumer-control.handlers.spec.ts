import { describe, expect, it, vi } from 'vitest';
import { PauseAnalyticsConsumerCommand } from '../../../src/analytics/application/commands/pause-analytics-consumer.command';
import { PauseAnalyticsConsumerHandler } from '../../../src/analytics/application/commands/pause-analytics-consumer.handler';
import { ResumeAnalyticsConsumerCommand } from '../../../src/analytics/application/commands/resume-analytics-consumer.command';
import { ResumeAnalyticsConsumerHandler } from '../../../src/analytics/application/commands/resume-analytics-consumer.handler';
import { KafkaConsumerControlPort } from '../../../src/analytics/application/ports/kafka-consumer-control.port';
import { GetConsumerControlStatusHandler } from '../../../src/analytics/application/queries/get-consumer-control-status.handler';
import { GetConsumerControlStatusQuery } from '../../../src/analytics/application/queries/get-consumer-control-status.query';

describe('Consumer Control Handlers', () => {
  it('PauseAnalyticsConsumerHandler invokes pause on control port', async () => {
    const mockControl: KafkaConsumerControlPort = {
      pause: vi.fn().mockResolvedValue(undefined),
      resume: vi.fn().mockResolvedValue(undefined),
      getStatus: vi.fn(),
    };

    const handler = new PauseAnalyticsConsumerHandler(mockControl);
    const result = await handler.execute(new PauseAnalyticsConsumerCommand());

    expect(mockControl.pause).toHaveBeenCalledTimes(1);
    expect(result).toEqual({ isPaused: true });
  });

  it('ResumeAnalyticsConsumerHandler invokes resume on control port', async () => {
    const mockControl: KafkaConsumerControlPort = {
      pause: vi.fn().mockResolvedValue(undefined),
      resume: vi.fn().mockResolvedValue(undefined),
      getStatus: vi.fn(),
    };

    const handler = new ResumeAnalyticsConsumerHandler(mockControl);
    const result = await handler.execute(new ResumeAnalyticsConsumerCommand());

    expect(mockControl.resume).toHaveBeenCalledTimes(1);
    expect(result).toEqual({ isPaused: false });
  });

  it('GetConsumerControlStatusHandler returns status from control port', async () => {
    const mockStatus = {
      groupId: 'analytics-v1',
      topic: 'purchase.events',
      isPaused: true,
      pausedPartitions: [0, 1],
    };

    const mockControl: KafkaConsumerControlPort = {
      pause: vi.fn(),
      resume: vi.fn(),
      getStatus: vi.fn().mockResolvedValue(mockStatus),
    };

    const handler = new GetConsumerControlStatusHandler(mockControl);
    const result = await handler.execute(new GetConsumerControlStatusQuery());

    expect(mockControl.getStatus).toHaveBeenCalledTimes(1);
    expect(result).toEqual(mockStatus);
  });
});
