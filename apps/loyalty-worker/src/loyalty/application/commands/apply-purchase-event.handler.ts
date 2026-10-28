import { Inject, Injectable, Logger } from '@nestjs/common';
import { LoyaltyAccount } from '../../domain/loyalty-account.entity';
import {
  LOYALTY_ACCOUNT_REPOSITORY_PORT,
  LoyaltyAccountRepositoryPort,
} from '../ports/loyalty-account-repository.port';
import {
  ApplyPurchaseEventCommand,
  ApplyPurchaseEventResult,
} from './apply-purchase-event.command';

@Injectable()
export class ApplyPurchaseEventHandler {
  private readonly logger = new Logger(ApplyPurchaseEventHandler.name);

  constructor(
    @Inject(LOYALTY_ACCOUNT_REPOSITORY_PORT)
    private readonly repository: LoyaltyAccountRepositoryPort,
  ) {}

  async execute(command: ApplyPurchaseEventCommand): Promise<ApplyPurchaseEventResult> {
    const { customerId, amountCents, eventId, purchaseId } = command.data;

    let account = await this.repository.findByCustomerId(customerId);
    if (!account) {
      account = LoyaltyAccount.initializeNew(customerId);
    }

    const outcome = account.applyPurchase(amountCents);
    await this.repository.save(account);

    this.logger.log(
      `[LoyaltyWorker] Processed eventId=${eventId} for customer=${customerId} -> earnedPoints=${outcome.pointsEarned}, currentTier=${outcome.newTier}, totalPoints=${outcome.totalPoints}`,
    );

    return {
      eventId,
      customerId,
      purchaseId,
      pointsEarned: outcome.pointsEarned,
      currentTier: outcome.newTier,
      totalPoints: outcome.totalPoints,
      totalSpendCents: outcome.totalSpendCents,
    };
  }
}
