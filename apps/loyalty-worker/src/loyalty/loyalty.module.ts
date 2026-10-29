import { Module } from '@nestjs/common';
import { ApplyPurchaseEventHandler } from './application/commands/apply-purchase-event.handler';
import { LOYALTY_ACCOUNT_READ_DAO_PORT } from './application/ports/loyalty-account-read-dao.port';
import { LOYALTY_ACCOUNT_REPOSITORY_PORT } from './application/ports/loyalty-account-repository.port';
import { LOYALTY_UNIT_OF_WORK_PORT } from './application/ports/loyalty-unit-of-work.port';
import { GetLoyaltyAccountHandler } from './application/queries/get-loyalty-account.handler';
import { KafkaLoyaltyConsumerAdapter } from './infrastructure/adapters/kafka-loyalty-consumer.adapter';
import { PostgresLoyaltyAccountReadDaoAdapter } from './infrastructure/adapters/postgres-loyalty-account-read-dao.adapter';
import { PostgresLoyaltyAccountRepositoryAdapter } from './infrastructure/adapters/postgres-loyalty-account-repository.adapter';
import { PostgresLoyaltyUnitOfWorkAdapter } from './infrastructure/adapters/postgres-loyalty-unit-of-work.adapter';
import { LoyaltyController } from './interfaces/http/loyalty.controller';

@Module({
  controllers: [LoyaltyController],
  providers: [
    ApplyPurchaseEventHandler,
    GetLoyaltyAccountHandler,
    PostgresLoyaltyAccountRepositoryAdapter,
    {
      provide: LOYALTY_ACCOUNT_REPOSITORY_PORT,
      useExisting: PostgresLoyaltyAccountRepositoryAdapter,
    },
    PostgresLoyaltyUnitOfWorkAdapter,
    {
      provide: LOYALTY_UNIT_OF_WORK_PORT,
      useExisting: PostgresLoyaltyUnitOfWorkAdapter,
    },
    PostgresLoyaltyAccountReadDaoAdapter,
    {
      provide: LOYALTY_ACCOUNT_READ_DAO_PORT,
      useExisting: PostgresLoyaltyAccountReadDaoAdapter,
    },
    KafkaLoyaltyConsumerAdapter,
  ],
  exports: [
    ApplyPurchaseEventHandler,
    GetLoyaltyAccountHandler,
    LOYALTY_ACCOUNT_REPOSITORY_PORT,
    LOYALTY_UNIT_OF_WORK_PORT,
    LOYALTY_ACCOUNT_READ_DAO_PORT,
  ],
})
export class LoyaltyModule {}
