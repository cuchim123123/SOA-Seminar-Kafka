import { Module } from '@nestjs/common';
import { CreatePurchaseHandler } from './application/commands/create-purchase.handler';
import { EVENT_PUBLISHER_PORT } from './application/ports/event-publisher.port';
import { OUTBOX_REPOSITORY_PORT } from './application/ports/outbox-repository.port';
import { PURCHASE_REPOSITORY_PORT } from './application/ports/purchase-repository.port';
import { SEQUENCE_GENERATOR_PORT } from './application/ports/sequence-generator.port';
import { UNIT_OF_WORK_PORT } from './application/ports/unit-of-work.port';
import { PostgresOutboxRepositoryAdapter } from './infrastructure/adapters/postgres-outbox-repository.adapter';
import { PostgresPurchaseRepositoryAdapter } from './infrastructure/adapters/postgres-purchase-repository.adapter';
import { PostgresSequenceGeneratorAdapter } from './infrastructure/adapters/postgres-sequence-generator.adapter';
import { PostgresUnitOfWorkAdapter } from './infrastructure/adapters/postgres-unit-of-work.adapter';
import { KafkaEventPublisherAdapter } from './infrastructure/adapters/kafka-event-publisher.adapter';
import { OutboxRelayService } from './infrastructure/services/outbox-relay.service';
import { PurchasesController } from './interfaces/http/purchases.controller';

@Module({
  controllers: [PurchasesController],
  providers: [
    CreatePurchaseHandler,
    OutboxRelayService,
    PostgresPurchaseRepositoryAdapter,
    PostgresOutboxRepositoryAdapter,
    PostgresSequenceGeneratorAdapter,
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
      provide: EVENT_PUBLISHER_PORT,
      useClass: KafkaEventPublisherAdapter,
    },
    {
      provide: UNIT_OF_WORK_PORT,
      useClass: PostgresUnitOfWorkAdapter,
    },
  ],
  exports: [
    CreatePurchaseHandler,
    UNIT_OF_WORK_PORT,
    PURCHASE_REPOSITORY_PORT,
    OUTBOX_REPOSITORY_PORT,
    OutboxRelayService,
  ],
})
export class PurchasesModule {}
