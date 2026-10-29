import { Inject, Injectable } from '@nestjs/common';
import { CONSUMER_CONTROL_PORT, ConsumerControlPort } from '../ports/consumer-control.port';
import { PauseAnalyticsConsumerCommand } from './pause-analytics-consumer.command';

@Injectable()
export class PauseAnalyticsConsumerHandler {
  constructor(
    @Inject(CONSUMER_CONTROL_PORT)
    private readonly control: ConsumerControlPort,
  ) {}

  async execute(_command: PauseAnalyticsConsumerCommand): Promise<{ isPaused: boolean }> {
    await this.control.pause();
    return { isPaused: true };
  }
}
