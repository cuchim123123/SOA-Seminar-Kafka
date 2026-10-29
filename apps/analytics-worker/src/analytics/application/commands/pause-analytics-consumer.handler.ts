import { Inject, Injectable } from '@nestjs/common';
import {
  KAFKA_CONSUMER_CONTROL_PORT,
  KafkaConsumerControlPort,
} from '../ports/kafka-consumer-control.port';
import { PauseAnalyticsConsumerCommand } from './pause-analytics-consumer.command';

@Injectable()
export class PauseAnalyticsConsumerHandler {
  constructor(
    @Inject(KAFKA_CONSUMER_CONTROL_PORT)
    private readonly control: KafkaConsumerControlPort,
  ) {}

  async execute(_command: PauseAnalyticsConsumerCommand): Promise<{ isPaused: boolean }> {
    await this.control.pause();
    return { isPaused: true };
  }
}
