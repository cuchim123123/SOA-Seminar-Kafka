import { Inject, Injectable } from '@nestjs/common';
import {
  ConsumerControlStatus,
  KAFKA_CONSUMER_CONTROL_PORT,
  KafkaConsumerControlPort,
} from '../ports/kafka-consumer-control.port';
import { GetConsumerControlStatusQuery } from './get-consumer-control-status.query';

@Injectable()
export class GetConsumerControlStatusHandler {
  constructor(
    @Inject(KAFKA_CONSUMER_CONTROL_PORT)
    private readonly control: KafkaConsumerControlPort,
  ) {}

  async execute(_query: GetConsumerControlStatusQuery): Promise<ConsumerControlStatus> {
    return this.control.getStatus();
  }
}
