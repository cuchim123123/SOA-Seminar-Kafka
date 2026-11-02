import { Client } from 'pg';
import { PostgresChangeFeed } from '@lab/observability';
import { AppConfigService } from '../config/app-config.service';
import { LoyaltyChangesController } from './interfaces/http/loyalty-changes.controller';
import { EVENT_RECEIPT_READ_PORT } from './application/ports/event-receipt-read.port';
import { GetEventReceiptsHandler } from './application/queries/get-event-receipts.handler';
import { PostgresEventReceiptReadAdapter } from './infrastructure/adapters/postgres-event-receipt-read.adapter';
import { EventReceiptsController } from './interfaces/http/event-receipts.controller';
import { Module } from '@nestjs/common';
import { ApplyPurchaseEventHandler } from './application/commands/apply-purchase-event/apply-purchase-event.handler';
import { ApplyPurchaseInvalidationHandler } from './application/commands/apply-purchase-invalidation/apply-purchase-invalidation.handler';
import { LOYALTY_CONSUMER_CONTROL_PORT } from './application/ports/consumer-control.port';
import { LOYALTY_ACCOUNT_READ_DAO_PORT } from './application/ports/loyalty-account-read-dao.port';
import { LOYALTY_ACCOUNT_REPOSITORY_PORT } from './application/ports/loyalty-account-repository.port';
import { LOYALTY_UNIT_OF_WORK_PORT } from './application/ports/loyalty-unit-of-work.port';
import { GetLoyaltyAccountHandler } from './application/queries/get-loyalty-account/get-loyalty-account.handler';
import { KafkaLoyaltyConsumerAdapter } from './infrastructure/adapters/kafka-loyalty-consumer.adapter';
import { PostgresLoyaltyAccountReadDaoAdapter } from './infrastructure/adapters/postgres-loyalty-account-read-dao.adapter';
import { PostgresLoyaltyAccountRepositoryAdapter } from './infrastructure/adapters/postgres-loyalty-account-repository.adapter';
import { PostgresLoyaltyUnitOfWorkAdapter } from './infrastructure/adapters/postgres-loyalty-unit-of-work.adapter';
import { LoyaltyConsumerControlController } from './interfaces/http/loyalty-consumer-control.controller';
import { LoyaltyController } from './interfaces/http/loyalty.controller';

@Module({
  controllers: [
    LoyaltyChangesController,
    EventReceiptsController,
    LoyaltyController,
    LoyaltyConsumerControlController,
  ],
  providers: [
    {
      provide: PostgresChangeFeed,
      inject: [AppConfigService],
      useFactory: (config: AppConfigService) =>
        new PostgresChangeFeed(
          config.changeFeedOptions,
          () =>
            new Client({
              connectionString: config.databaseUrl,
              connectionTimeoutMillis: config.changeFeedOptions.reconnectMs,
            }),
        ),
    },
    GetEventReceiptsHandler,
    { provide: EVENT_RECEIPT_READ_PORT, useClass: PostgresEventReceiptReadAdapter },
    ApplyPurchaseEventHandler,
    ApplyPurchaseInvalidationHandler,
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
    {
      provide: LOYALTY_CONSUMER_CONTROL_PORT,
      useExisting: KafkaLoyaltyConsumerAdapter,
    },
  ],
  exports: [
    ApplyPurchaseEventHandler,
    ApplyPurchaseInvalidationHandler,
    GetLoyaltyAccountHandler,
    LOYALTY_ACCOUNT_REPOSITORY_PORT,
    LOYALTY_UNIT_OF_WORK_PORT,
    LOYALTY_ACCOUNT_READ_DAO_PORT,
    LOYALTY_CONSUMER_CONTROL_PORT,
  ],
})
export class LoyaltyModule {}
