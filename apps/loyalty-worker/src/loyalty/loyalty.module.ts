import { Module } from '@nestjs/common';
import { ApplyPurchaseEventHandler } from './application/commands/apply-purchase-event.handler';
import { LOYALTY_ACCOUNT_READ_DAO_PORT } from './application/ports/loyalty-account-read-dao.port';
import { LOYALTY_ACCOUNT_REPOSITORY_PORT } from './application/ports/loyalty-account-repository.port';
import { GetLoyaltyAccountHandler } from './application/queries/get-loyalty-account.handler';
import { InMemoryLoyaltyAccountRepositoryAdapter } from './infrastructure/adapters/in-memory-loyalty-account-repository.adapter';
import { KafkaLoyaltyConsumerAdapter } from './infrastructure/adapters/kafka-loyalty-consumer.adapter';
import { LoyaltyController } from './interfaces/http/loyalty.controller';

@Module({
  controllers: [LoyaltyController],
  providers: [
    ApplyPurchaseEventHandler,
    GetLoyaltyAccountHandler,
    InMemoryLoyaltyAccountRepositoryAdapter,
    {
      provide: LOYALTY_ACCOUNT_REPOSITORY_PORT,
      useExisting: InMemoryLoyaltyAccountRepositoryAdapter,
    },
    {
      provide: LOYALTY_ACCOUNT_READ_DAO_PORT,
      useExisting: InMemoryLoyaltyAccountRepositoryAdapter,
    },
    KafkaLoyaltyConsumerAdapter,
  ],
  exports: [
    ApplyPurchaseEventHandler,
    GetLoyaltyAccountHandler,
    LOYALTY_ACCOUNT_REPOSITORY_PORT,
    LOYALTY_ACCOUNT_READ_DAO_PORT,
  ],
})
export class LoyaltyModule {}
