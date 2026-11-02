import { Client } from 'pg';
import { PostgresChangeFeed } from '@lab/observability';
import { AppConfigService } from '../config/app-config.service';
import { PurchaseChangesController } from './interfaces/http/purchase-changes.controller';
import { PURCHASE_HISTORY_PORT } from './application/ports/purchase-history.port';
import { ListPurchasesHandler } from './application/queries/list-purchases.handler';
import { PostgresPurchaseHistoryAdapter } from './infrastructure/adapters/postgres-purchase-history.adapter';
import { PurchaseHistoryController } from './interfaces/http/purchase-history.controller';
import { Module } from '@nestjs/common';
import { CreatePurchaseBatchHandler } from './application/commands/create-purchase-batch/create-purchase-batch.handler';
import { CreatePurchaseHandler } from './application/commands/create-purchase/create-purchase.handler';
import { InvalidateSyntheticPurchaseHandler } from './application/commands/invalidate-synthetic-purchase/invalidate-synthetic-purchase.handler';
import { CLIENT_IDEMPOTENCY_REPOSITORY_PORT } from './application/ports/client-idempotency-repository.port';
import { OUTBOX_REPOSITORY_PORT } from './application/ports/outbox-repository.port';
import { PURCHASE_READ_DAO_PORT } from './application/ports/purchase-read-dao.port';
import { PURCHASE_REPOSITORY_PORT } from './application/ports/purchase-repository.port';
import { SEQUENCE_GENERATOR_PORT } from './application/ports/sequence-generator.port';
import { UNIT_OF_WORK_PORT } from './application/ports/unit-of-work.port';
import { GetPurchaseHandler } from './application/queries/get-purchase/get-purchase.handler';
import { PostgresHttpIdempotencyRepositoryAdapter } from './infrastructure/adapters/postgres-http-idempotency-repository.adapter';
import { PostgresOutboxRepositoryAdapter } from './infrastructure/adapters/postgres-outbox-repository.adapter';
import { PostgresPurchaseReadDaoAdapter } from './infrastructure/adapters/postgres-purchase-read-dao.adapter';
import { PostgresPurchaseRepositoryAdapter } from './infrastructure/adapters/postgres-purchase-repository.adapter';
import { PostgresSequenceGeneratorAdapter } from './infrastructure/adapters/postgres-sequence-generator.adapter';
import { PostgresUnitOfWorkAdapter } from './infrastructure/adapters/postgres-unit-of-work.adapter';
import { PurchasesController } from './interfaces/http/purchases.controller';

@Module({
  controllers: [PurchaseChangesController, PurchaseHistoryController, PurchasesController],
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
    ListPurchasesHandler,
    { provide: PURCHASE_HISTORY_PORT, useClass: PostgresPurchaseHistoryAdapter },
    CreatePurchaseHandler,
    CreatePurchaseBatchHandler,
    InvalidateSyntheticPurchaseHandler,
    GetPurchaseHandler,
    PostgresPurchaseRepositoryAdapter,
    PostgresOutboxRepositoryAdapter,
    PostgresSequenceGeneratorAdapter,
    PostgresHttpIdempotencyRepositoryAdapter,
    PostgresPurchaseReadDaoAdapter,
    PostgresUnitOfWorkAdapter,
    {
      provide: PURCHASE_REPOSITORY_PORT,
      useClass: PostgresPurchaseRepositoryAdapter,
    },
    {
      provide: OUTBOX_REPOSITORY_PORT,
      useClass: PostgresOutboxRepositoryAdapter,
    },
    {
      provide: SEQUENCE_GENERATOR_PORT,
      useClass: PostgresSequenceGeneratorAdapter,
    },
    {
      provide: CLIENT_IDEMPOTENCY_REPOSITORY_PORT,
      useClass: PostgresHttpIdempotencyRepositoryAdapter,
    },
    {
      provide: PURCHASE_READ_DAO_PORT,
      useClass: PostgresPurchaseReadDaoAdapter,
    },
    {
      provide: UNIT_OF_WORK_PORT,
      useClass: PostgresUnitOfWorkAdapter,
    },
  ],
  exports: [
    CreatePurchaseHandler,
    CreatePurchaseBatchHandler,
    InvalidateSyntheticPurchaseHandler,
    GetPurchaseHandler,
    UNIT_OF_WORK_PORT,
    PURCHASE_REPOSITORY_PORT,
    OUTBOX_REPOSITORY_PORT,
    PURCHASE_READ_DAO_PORT,
    CLIENT_IDEMPOTENCY_REPOSITORY_PORT,
  ],
})
export class PurchasesModule {}
