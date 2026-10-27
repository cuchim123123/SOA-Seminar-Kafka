import { IntegrationEventEnvelopeV1 } from '@lab/contracts';

export interface PublishResult {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly eventId: string;
  readonly correlationId: string;
}

export const EVENT_PUBLISHER_PORT = Symbol('EVENT_PUBLISHER_PORT');

export interface EventPublisherPort {
  publish(envelope: IntegrationEventEnvelopeV1): Promise<PublishResult>;
}
