import { describe, expect, it, vi } from 'vitest';
import { IntegrationEventEnvelopeV1 } from '@lab/contracts';
import { CreatePurchaseCommand } from '../../../src/purchases/application/commands/create-purchase.command';
import { CreatePurchaseHandler } from '../../../src/purchases/application/commands/create-purchase.handler';
import {
  EventPublisherPort,
  PublishResult,
} from '../../../src/purchases/application/ports/event-publisher.port';
import { SequenceGeneratorPort } from '../../../src/purchases/application/ports/sequence-generator.port';

describe('CreatePurchaseHandler', () => {
  it('successfully creates purchase, requests sequence, and publishes envelope to Kafka', async () => {
    let publishedEnvelope: IntegrationEventEnvelopeV1 | undefined;

    const mockPublisher: EventPublisherPort = {
      publish: vi.fn(async (envelope: IntegrationEventEnvelopeV1): Promise<PublishResult> => {
        publishedEnvelope = envelope;
        return {
          topic: 'purchase.events',
          partition: 0,
          offset: '42',
          eventId: envelope.eventId,
          correlationId: envelope.correlationId,
        };
      }),
    };

    const mockSequenceGenerator: SequenceGeneratorPort = {
      nextSequence: vi.fn(async (_customerId: string): Promise<number> => Promise.resolve(1)),
    };

    const handler = new CreatePurchaseHandler(mockPublisher, mockSequenceGenerator);

    const command = new CreatePurchaseCommand('CUST_U001', 3500);
    const result = await handler.execute(command);

    expect(mockSequenceGenerator.nextSequence).toHaveBeenCalledWith('CUST_U001');
    expect(mockPublisher.publish).toHaveBeenCalledTimes(1);

    expect(result.customerId).toBe('CUST_U001');
    expect(result.amountCents).toBe(3500);
    expect(result.customerSequence).toBe(1);
    expect(result.purchaseId).toMatch(/^pur_/);
    expect(result.kafkaMetadata).toEqual({
      topic: 'purchase.events',
      partition: 0,
      offset: '42',
    });

    expect(publishedEnvelope).toBeDefined();
    expect(publishedEnvelope?.schemaVersion).toBe(1);
    expect(publishedEnvelope?.eventType).toBe('PurchaseCompleted');
    expect(publishedEnvelope?.aggregateType).toBe('CustomerPurchaseStream');
    expect(publishedEnvelope?.aggregateId).toBe('CUST_U001');
    expect(publishedEnvelope?.customerSequence).toBe(1);
    expect(publishedEnvelope?.payload).toEqual({
      purchaseId: result.purchaseId,
      customerId: 'CUST_U001',
      amountCents: 3500,
    });
  });

  it('preserves provided correlationId across envelope and result', async () => {
    const fixedCorrelationId = 'c0000000-0000-4000-8000-000000000001';

    const mockPublisher: EventPublisherPort = {
      publish: vi.fn(async (envelope: IntegrationEventEnvelopeV1): Promise<PublishResult> => ({
        topic: 'purchase.events',
        partition: 0,
        offset: '10',
        eventId: envelope.eventId,
        correlationId: envelope.correlationId,
      })),
    };

    const mockSequenceGenerator: SequenceGeneratorPort = {
      nextSequence: vi.fn(async () => Promise.resolve(3)),
    };

    const handler = new CreatePurchaseHandler(mockPublisher, mockSequenceGenerator);

    const command = new CreatePurchaseCommand('CUST_U002', 1200, fixedCorrelationId);
    const result = await handler.execute(command);

    expect(result.correlationId).toBe(fixedCorrelationId);
    expect(result.customerSequence).toBe(3);
  });
});
