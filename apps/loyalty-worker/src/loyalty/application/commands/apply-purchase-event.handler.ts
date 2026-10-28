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
    let account = await this.repository.findByCustomerId(command.customerId);
    if (!account) {
      account = LoyaltyAccount.initializeNew(command.customerId);
    }

    const outcome = account.applyPurchase(command.amountCents);
    await this.repository.save(account);

    this.logger.log(
      `[LoyaltyWorker] Processed eventId=${command.eventId} for customer=${command.customerId} from topic=${command.topic} [p=${command.partition}, offset=${command.offset}] -> earnedPoints=${outcome.pointsEarned}, currentTier=${outcome.newTier}, totalPoints=${outcome.totalPoints}`,
    );

    return {
      eventId: command.eventId,
      customerId: command.customerId,
      purchaseId: command.purchaseId,
      pointsEarned: outcome.pointsEarned,
      currentTier: outcome.newTier,
      totalPoints: outcome.totalPoints,
      totalSpendCents: outcome.totalSpendCents,
      kafkaMetadata: {
        topic: command.topic,
        partition: command.partition,
        offset: command.offset,
      },
    };
  }
}
