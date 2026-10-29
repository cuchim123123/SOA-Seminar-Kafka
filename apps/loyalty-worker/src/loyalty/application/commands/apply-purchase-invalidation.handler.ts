import { Inject, Injectable } from '@nestjs/common';
import { LoyaltyAccount } from '../../domain/loyalty-account.entity';
import {
  LOYALTY_UNIT_OF_WORK_PORT,
  LoyaltyUnitOfWorkPort,
} from '../ports/loyalty-unit-of-work.port';
import {
  ApplyPurchaseInvalidationCommand,
  ApplyPurchaseInvalidationResult,
} from './apply-purchase-invalidation.command';

@Injectable()
export class ApplyPurchaseInvalidationHandler {
  constructor(
    @Inject(LOYALTY_UNIT_OF_WORK_PORT)
    private readonly unitOfWork: LoyaltyUnitOfWorkPort,
  ) {}

  async execute(
    command: ApplyPurchaseInvalidationCommand,
  ): Promise<ApplyPurchaseInvalidationResult> {
    const { customerId, eventId, purchaseId, customerSequence } = command.data;

    return await this.unitOfWork.execute(async (ctx) => {
      // 1. Consumer idempotency check: attempt to insert into processed_events
      const isNew = await ctx.idempotencyRepository.markProcessedIfNew('loyalty-v1', eventId);

      if (!isNew) {
        // Idempotent duplicate event detected! Do not mutate business state.
        const existing = await ctx.loyaltyRepository.findByCustomerId(customerId);
        return {
          eventId,
          customerId,
          purchaseId,
          correctionRequired: existing?.correctionRequired ?? false,
          lastCustomerSequence: existing?.lastCustomerSequence ?? 0,
          isDuplicate: true,
        };
      }

      // 2. Fetch or create loyalty account
      let account = await ctx.loyaltyRepository.findByCustomerId(customerId);
      if (!account) {
        account = LoyaltyAccount.create(customerId);
      }

      // 3. Apply invalidation: marks correction_required and advances customerSequence atomically
      account.applyInvalidation(customerSequence);
      await ctx.loyaltyRepository.save(account);

      return {
        eventId,
        customerId,
        purchaseId,
        correctionRequired: account.correctionRequired,
        lastCustomerSequence: account.lastCustomerSequence,
        isDuplicate: false,
      };
    });
  }
}
