import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import {
  createPurchaseInvalidatedEnvelope,
  PurchaseInvalidatedReasonV1,
  uuidv7,
} from '@lab/contracts';
import { UNIT_OF_WORK_PORT, UnitOfWorkPort } from '../ports/unit-of-work.port';
import {
  InvalidateSyntheticPurchaseCommand,
  InvalidateSyntheticPurchaseResult,
} from './invalidate-synthetic-purchase.command';

@Injectable()
export class InvalidateSyntheticPurchaseHandler {
  constructor(
    @Inject(UNIT_OF_WORK_PORT)
    private readonly unitOfWork: UnitOfWorkPort,
  ) {}

  async execute(
    command: InvalidateSyntheticPurchaseCommand,
  ): Promise<InvalidateSyntheticPurchaseResult> {
    const { purchaseId, reason = 'DEMO_INVALIDATION', correlationId } = command.data;
    const resolvedCorrelationId = correlationId ?? uuidv7();

    return await this.unitOfWork.execute(async (ctx) => {
      // 1. Find existing purchase under exclusive row lock (FOR UPDATE) to serialize concurrent invalidations
      const purchase = await ctx.purchaseRepository.findById(purchaseId, { forUpdate: true });
      if (!purchase) {
        throw new NotFoundException(`Purchase with ID '${purchaseId}' not found`);
      }

      // 2. Check if already invalidated (Idempotency guarantee with original event metadata)
      if (purchase.status === 'INVALIDATED') {
        return {
          purchaseId: purchase.purchaseId,
          customerId: purchase.customerId,
          status: 'INVALIDATED',
          customerSequence: purchase.invalidationSequence ?? purchase.customerSequence,
          eventId: purchase.invalidationEventId ?? purchase.purchaseId,
          reason: (purchase.invalidationReason as PurchaseInvalidatedReasonV1) ?? reason,
          isAlreadyInvalidated: true,
        };
      }

      // 3. Allocate the next monotonic customer sequence under row lock
      const customerSequence = await ctx.sequenceGenerator.nextSequence(purchase.customerId);

      // 4. Create typed integration envelope via contracts single-source-of-truth factory
      const eventId = uuidv7();
      const envelope = createPurchaseInvalidatedEnvelope({
        eventId,
        customerId: purchase.customerId,
        purchaseId: purchase.purchaseId,
        reason,
        customerSequence,
        correlationId: resolvedCorrelationId,
      });

      // 5. Invalidate domain entity with complete audit details and persist to database
      const invalidatedPurchase = purchase.invalidate({
        sequence: customerSequence,
        eventId,
        reason,
      });
      await ctx.purchaseRepository.save(invalidatedPurchase);

      // 6. Atomically append to Outbox inside Unit of Work
      await ctx.outboxRepository.save({
        eventId,
        aggregateId: purchase.customerId,
        customerSequence,
        eventType: envelope.eventType,
        envelope,
        status: 'PENDING',
        attemptCount: 0,
        createdAt: new Date().toISOString(),
      });

      return {
        purchaseId: purchase.purchaseId,
        customerId: purchase.customerId,
        status: 'INVALIDATED',
        customerSequence,
        eventId,
        reason,
        isAlreadyInvalidated: false,
      };
    });
  }
}
