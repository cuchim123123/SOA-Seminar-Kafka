import { Module } from '@nestjs/common';
import { PrismaService } from '../database/prisma.service';
import { EVENT_PUBLISHER_PORT } from './application/ports/event-publisher.port';
import { OUTBOX_PUBLISHER_REPOSITORY_PORT } from './application/ports/outbox-publisher-repository.port';
import { OutboxPublisherService } from './application/services/outbox-publisher.service';
import { KafkaEventPublisherAdapter } from './infrastructure/adapters/kafka-event-publisher.adapter';
import { PostgresOutboxPublisherRepositoryAdapter } from './infrastructure/adapters/postgres-outbox-publisher-repository.adapter';
import { HealthController } from './interfaces/http/health.controller';

@Module({
  controllers: [HealthController],
  providers: [
    PrismaService,
    {
      provide: OUTBOX_PUBLISHER_REPOSITORY_PORT,
      useClass: PostgresOutboxPublisherRepositoryAdapter,
    },
    {
      provide: EVENT_PUBLISHER_PORT,
      useClass: KafkaEventPublisherAdapter,
    },
    OutboxPublisherService,
  ],
  exports: [OutboxPublisherService, OUTBOX_PUBLISHER_REPOSITORY_PORT, EVENT_PUBLISHER_PORT],
})
export class PublisherModule {}
