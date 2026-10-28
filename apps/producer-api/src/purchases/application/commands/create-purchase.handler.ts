import { Inject, Injectable } from '@nestjs/common';
import { createPurchaseCompletedEnvelope, uuidv7 } from '@lab/contracts';
import { Purchase } from '../../domain/purchase.entity';
import { UNIT_OF_WORK_PORT, UnitOfWorkPort } from '../ports/unit-of-work.port';
import { CreatePurchaseCommand, CreatePurchaseResult } from './create-purchase.command';

@Injectable()
export class CreatePurchaseHandler {
  constructor(
    @Inject(UNIT_OF_WORK_PORT)
    private readonly unitOfWork: UnitOfWorkPort,
  ) {}

  async execute(command: CreatePurchaseCommand): Promise<CreatePurchaseResult> {
    const { customerId, amountCents, correlationId, idempotencyKey } = command.data;

    return await this.unitOfWork.execute(async (ctx) => {
      // 1. Monotonic customer sequence allocated under transactional lock
      const customerSequence = await ctx.sequenceGenerator.nextSequence(customerId);

      const purchaseId = `pur_${uuidv7()}`;
      const eventId = uuidv7();
      const resolvedCorrelationId = correlationId ?? uuidv7();

      // 2. Instantiate and validate domain entity invariants
      const purchase = Purchase.create({
        purchaseId,
        customerId,
        amountCents,
        customerSequence,
      });

      // 3. Create typed integration envelope via contracts single-source-of-truth factory
      const envelope = createPurchaseCompletedEnvelope({
        eventId,
        customerId: purchase.customerId,
        purchaseId: purchase.purchaseId,
        amountCents: purchase.amountCents,
        customerSequence: purchase.customerSequence,
        correlationId: resolvedCorrelationId,
      });

      // 4. Atomically persist Purchase entity inside Unit of Work
      await ctx.purchaseRepository.save(purchase);

      // 5. Atomically append integration event to Outbox inside Unit of Work
      await ctx.outboxRepository.save({
        eventId,
        aggregateId: purchase.customerId,
        customerSequence: purchase.customerSequence,
        eventType: envelope.eventType,
        envelope,
        status: 'PENDING',
        attemptCount: 0,
        createdAt: new Date().toISOString(),
      });

      return {
        purchaseId: purchase.purchaseId,
        customerId: purchase.customerId,
        amountCents: purchase.amountCents,
        customerSequence: purchase.customerSequence,
        eventId,
        correlationId: resolvedCorrelationId,
        ...(idempotencyKey ? { idempotencyKey } : {}),
      };
    });
  }
}
