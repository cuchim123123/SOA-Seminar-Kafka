import { Inject, Injectable } from '@nestjs/common';
import { CONSUMER_CONTROL_PORT, ConsumerControlPort } from '../ports/consumer-control.port';
import { ResumeAnalyticsConsumerCommand } from './resume-analytics-consumer.command';

@Injectable()
export class ResumeAnalyticsConsumerHandler {
  constructor(
    @Inject(CONSUMER_CONTROL_PORT)
    private readonly control: ConsumerControlPort,
  ) {}

  async execute(_command: ResumeAnalyticsConsumerCommand): Promise<{ isPaused: boolean }> {
    await this.control.resume();
    return { isPaused: false };
  }
}
