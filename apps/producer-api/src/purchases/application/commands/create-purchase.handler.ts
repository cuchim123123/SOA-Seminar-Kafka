import { Inject, Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { createPurchaseCompletedEnvelope } from '@lab/contracts';
import { Purchase } from '../../domain/purchase.entity';
import { EVENT_PUBLISHER_PORT, EventPublisherPort } from '../ports/event-publisher.port';
import { SEQUENCE_GENERATOR_PORT, SequenceGeneratorPort } from '../ports/sequence-generator.port';
import { CreatePurchaseCommand, CreatePurchaseResult } from './create-purchase.command';

@Injectable()
export class CreatePurchaseHandler {
  private readonly logger = new Logger(CreatePurchaseHandler.name);

  constructor(
    @Inject(EVENT_PUBLISHER_PORT)
    private readonly eventPublisher: EventPublisherPort,
    @Inject(SEQUENCE_GENERATOR_PORT)
    private readonly sequenceGenerator: SequenceGeneratorPort,
  ) {}

  async execute(command: CreatePurchaseCommand): Promise<CreatePurchaseResult> {
    const purchaseId = `pur_${randomUUID()}`;
    const eventId = randomUUID();
    const correlationId = command.correlationId ?? randomUUID();

    const customerSequence = await this.sequenceGenerator.nextSequence(command.customerId);

    // Validate domain invariants
    const purchase = Purchase.create({
      purchaseId,
      customerId: command.customerId,
      amountCents: command.amountCents,
      customerSequence,
    });

    // Create typed event envelope via contracts single-source-of-truth factory
    const envelope = createPurchaseCompletedEnvelope({
      eventId,
      customerId: purchase.customerId,
      purchaseId: purchase.purchaseId,
      amountCents: purchase.amountCents,
      customerSequence: purchase.customerSequence,
      correlationId,
    });

    // Await publisher broker confirmation
    const publishResult = await this.eventPublisher.publish(envelope);

    this.logger.log(
      `Published PurchaseCompleted [purchaseId=${purchaseId}, customer=${purchase.customerId}, seq=${customerSequence}] -> topic=${publishResult.topic}, partition=${publishResult.partition}, offset=${publishResult.offset}`,
    );

    return {
      purchaseId: purchase.purchaseId,
      customerId: purchase.customerId,
      amountCents: purchase.amountCents,
      customerSequence: purchase.customerSequence,
      eventId,
      correlationId,
      kafkaMetadata: {
        topic: publishResult.topic,
        partition: publishResult.partition,
        offset: publishResult.offset,
      },
    };
  }
}
