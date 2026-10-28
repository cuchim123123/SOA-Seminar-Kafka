import { Module } from '@nestjs/common';
import { CreatePurchaseHandler } from './application/commands/create-purchase.handler';
import { EVENT_PUBLISHER_PORT } from './application/ports/event-publisher.port';
import { OUTBOX_REPOSITORY_PORT } from './application/ports/outbox-repository.port';
import { PURCHASE_REPOSITORY_PORT } from './application/ports/purchase-repository.port';
import { SEQUENCE_GENERATOR_PORT } from './application/ports/sequence-generator.port';
import { UNIT_OF_WORK_PORT } from './application/ports/unit-of-work.port';
import { InMemoryOutboxRepositoryAdapter } from './infrastructure/adapters/in-memory-outbox-repository.adapter';
import { InMemoryPurchaseRepositoryAdapter } from './infrastructure/adapters/in-memory-purchase-repository.adapter';
import { InMemorySequenceGeneratorAdapter } from './infrastructure/adapters/in-memory-sequence-generator.adapter';
import { InMemoryUnitOfWorkAdapter } from './infrastructure/adapters/in-memory-unit-of-work.adapter';
import { KafkaEventPublisherAdapter } from './infrastructure/adapters/kafka-event-publisher.adapter';
import { OutboxRelayService } from './infrastructure/services/outbox-relay.service';
import { PurchasesController } from './interfaces/http/purchases.controller';

@Module({
  controllers: [PurchasesController],
  providers: [
    CreatePurchaseHandler,
    OutboxRelayService,
    {
      provide: PURCHASE_REPOSITORY_PORT,
      useClass: InMemoryPurchaseRepositoryAdapter,
    },
    {
      provide: OUTBOX_REPOSITORY_PORT,
      useClass: InMemoryOutboxRepositoryAdapter,
    },
    {
      provide: SEQUENCE_GENERATOR_PORT,
      useClass: InMemorySequenceGeneratorAdapter,
    },
    {
      provide: EVENT_PUBLISHER_PORT,
      useClass: KafkaEventPublisherAdapter,
    },
    {
      provide: UNIT_OF_WORK_PORT,
      useClass: InMemoryUnitOfWorkAdapter,
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
