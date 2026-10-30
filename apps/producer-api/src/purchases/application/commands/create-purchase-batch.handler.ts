import { Inject, Injectable } from '@nestjs/common';
import { createPurchaseCompletedEnvelope, uuidv7 } from '@lab/contracts';
import { Purchase } from '../../domain/purchase.entity';
import { UNIT_OF_WORK_PORT, UnitOfWorkPort } from '../ports/unit-of-work.port';
import {
  BatchPurchaseItem,
  CreatePurchaseBatchCommand,
  CreatePurchaseBatchResult,
} from './create-purchase-batch.command';

const DEFAULT_STRESS_CUSTOMERS = [
  'cust_alpha',
  'cust_beta',
  'cust_gamma',
  'cust_delta',
  'cust_epsilon',
];

@Injectable()
export class CreatePurchaseBatchHandler {
  constructor(
    @Inject(UNIT_OF_WORK_PORT)
    private readonly unitOfWork: UnitOfWorkPort,
  ) {}

  async execute(command: CreatePurchaseBatchCommand): Promise<CreatePurchaseBatchResult> {
    const startTime = Date.now();
    const count = Math.min(500, Math.max(1, command.data.count));
    const customerPool = command.data.customerId?.trim()
      ? [command.data.customerId.trim()]
      : DEFAULT_STRESS_CUSTOMERS;

    const minAmount = Math.max(100, command.data.minAmountCents ?? 1000);
    const maxAmount = Math.max(minAmount, command.data.maxAmountCents ?? 25000);
    const correlationId = command.data.correlationId ?? uuidv7();

    const createdPurchases: BatchPurchaseItem[] = [];
    const customerDistribution: Record<string, number> = {};

    await this.unitOfWork.execute(async (ctx) => {
      for (let i = 0; i < count; i++) {
        const targetCustomer = customerPool[i % customerPool.length];
        customerDistribution[targetCustomer] = (customerDistribution[targetCustomer] || 0) + 1;

        const amountCents = Math.floor(Math.random() * (maxAmount - minAmount + 1)) + minAmount;
        const customerSequence = await ctx.sequenceGenerator.nextSequence(targetCustomer);

        const purchaseId = `pur_${uuidv7()}`;
        const eventId = uuidv7();

        const purchase = Purchase.create({
          purchaseId,
          customerId: targetCustomer,
          amountCents,
          customerSequence,
        });

        const envelope = createPurchaseCompletedEnvelope({
          eventId,
          customerId: targetCustomer,
          purchaseId,
          amountCents,
          customerSequence,
          correlationId,
        });

        await ctx.purchaseRepository.save(purchase);

        await ctx.outboxRepository.save({
          eventId,
          aggregateId: targetCustomer,
          customerSequence,
          eventType: envelope.eventType,
          envelope,
          status: 'PENDING',
          attemptCount: 0,
          createdAt: new Date().toISOString(),
        });

        createdPurchases.push({
          purchaseId,
          customerId: targetCustomer,
          amountCents,
          customerSequence,
          eventId,
        });
      }
    });

    const durationMs = Date.now() - startTime;

    return {
      totalCreated: createdPurchases.length,
      durationMs,
      customerDistribution,
      purchases: createdPurchases,
    };
  }
}
