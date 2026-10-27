import { IntegrationEventEnvelopeV1 } from './envelope.v1';

/**
 * Accessor for the authoritative stream key / partition key.
 * Always resolves to aggregateId to guarantee consistency with Kafka record routing.
 */
export function getEventPartitionKey(envelope: IntegrationEventEnvelopeV1): string {
  return envelope.aggregateId;
}

/**
 * Accessor for the customer ID of an event.
 * Always resolves to aggregateId as the single authoritative stream root.
 */
export function getEventCustomerId(envelope: IntegrationEventEnvelopeV1): string {
  return envelope.aggregateId;
}
