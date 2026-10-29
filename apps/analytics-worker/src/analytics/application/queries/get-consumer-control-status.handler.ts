import { Inject, Injectable } from '@nestjs/common';
import {
  ConsumerControlStatus,
  CONSUMER_CONTROL_PORT,
  ConsumerControlPort,
} from '../ports/consumer-control.port';
import { GetConsumerControlStatusQuery } from './get-consumer-control-status.query';

@Injectable()
export class GetConsumerControlStatusHandler {
  constructor(
    @Inject(CONSUMER_CONTROL_PORT)
    private readonly control: ConsumerControlPort,
  ) {}

  async execute(_query: GetConsumerControlStatusQuery): Promise<ConsumerControlStatus> {
    return this.control.getStatus();
  }
}
