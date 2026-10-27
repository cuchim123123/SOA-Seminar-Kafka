import { Module } from '@nestjs/common';
import { CreatePurchaseHandler } from './application/commands/create-purchase.handler';
import { EVENT_PUBLISHER_PORT } from './application/ports/event-publisher.port';
import { SEQUENCE_GENERATOR_PORT } from './application/ports/sequence-generator.port';
import { InMemorySequenceGeneratorAdapter } from './infrastructure/adapters/in-memory-sequence-generator.adapter';
import { KafkaEventPublisherAdapter } from './infrastructure/adapters/kafka-event-publisher.adapter';
import { PurchasesController } from './interfaces/http/purchases.controller';

@Module({
  controllers: [PurchasesController],
  providers: [
    CreatePurchaseHandler,
    {
      provide: EVENT_PUBLISHER_PORT,
      useClass: KafkaEventPublisherAdapter,
    },
    {
      provide: SEQUENCE_GENERATOR_PORT,
      useClass: InMemorySequenceGeneratorAdapter,
    },
  ],
  exports: [CreatePurchaseHandler],
})
export class PurchasesModule {}
