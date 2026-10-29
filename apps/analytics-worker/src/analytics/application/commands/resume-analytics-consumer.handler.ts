import { Inject, Injectable } from '@nestjs/common';
import {
  KAFKA_CONSUMER_CONTROL_PORT,
  KafkaConsumerControlPort,
} from '../ports/kafka-consumer-control.port';
import { ResumeAnalyticsConsumerCommand } from './resume-analytics-consumer.command';

@Injectable()
export class ResumeAnalyticsConsumerHandler {
  constructor(
    @Inject(KAFKA_CONSUMER_CONTROL_PORT)
    private readonly control: KafkaConsumerControlPort,
  ) {}

  async execute(_command: ResumeAnalyticsConsumerCommand): Promise<{ isPaused: boolean }> {
    await this.control.resume();
    return { isPaused: false };
  }
}
