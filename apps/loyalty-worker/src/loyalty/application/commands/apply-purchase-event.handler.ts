import { Inject, Injectable } from '@nestjs/common';
import { LoyaltyAccount } from '../../domain/loyalty-account.entity';
import {
  LOYALTY_UNIT_OF_WORK_PORT,
  LoyaltyUnitOfWorkPort,
} from '../ports/loyalty-unit-of-work.port';
import {
  ApplyPurchaseEventCommand,
  ApplyPurchaseEventResult,
} from './apply-purchase-event.command';

@Injectable()
export class ApplyPurchaseEventHandler {
  constructor(
    @Inject(LOYALTY_UNIT_OF_WORK_PORT)
    private readonly unitOfWork: LoyaltyUnitOfWorkPort,
  ) {}

  async execute(command: ApplyPurchaseEventCommand): Promise<ApplyPurchaseEventResult> {
    const { customerId, amountCents, eventId, purchaseId, customerSequence } = command.data;

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
          pointsEarned: 0,
          currentTier: existing?.tier ?? 'BRONZE',
          totalPoints: existing?.pointsBalance ?? 0,
          totalSpendCents: existing?.totalEligibleSpendCents ?? 0,
          isDuplicate: true,
        };
      }

      // 2. Fetch or create loyalty account
      let account = await ctx.loyaltyRepository.findByCustomerId(customerId);
      if (!account) {
        account = LoyaltyAccount.create(customerId);
      }

      // 3. Apply purchase with sequence validation
      const outcome = account.applyPurchase(amountCents, customerSequence);
      await ctx.loyaltyRepository.save(account);

      return {
        eventId,
        customerId,
        purchaseId,
        pointsEarned: outcome.pointsEarned,
        currentTier: outcome.newTier,
        totalPoints: outcome.totalPoints,
        totalSpendCents: outcome.totalSpendCents,
        isDuplicate: false,
      };
    });
  }
}
