import { Inject, Injectable } from '@nestjs/common';
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
  constructor(
    @Inject(LOYALTY_ACCOUNT_REPOSITORY_PORT)
    private readonly repository: LoyaltyAccountRepositoryPort,
  ) {}

  async execute(command: ApplyPurchaseEventCommand): Promise<ApplyPurchaseEventResult> {
    const { customerId, amountCents, eventId, purchaseId } = command.data;

    let account = await this.repository.findByCustomerId(customerId);
    if (!account) {
      account = LoyaltyAccount.create(customerId);
    }

    const outcome = account.applyPurchase(amountCents);
    await this.repository.save(account);

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
