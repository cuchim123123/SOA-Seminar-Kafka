import { Inject, Injectable } from '@nestjs/common';
import { IntegrationEventEnvelopeV1 } from '@lab/contracts';
import { PrismaService } from '../../../database/prisma.service';
import {
  OutboxPublisherRecord,
  OutboxPublisherRepositoryPort,
} from '../../application/ports/outbox-publisher-repository.port';

@Injectable()
export class PostgresOutboxPublisherRepositoryAdapter implements OutboxPublisherRepositoryPort {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
  ) {}

  async findPending(limit = 100): Promise<OutboxPublisherRecord[]> {
    const records = await this.prisma.outboxEvent.findMany({
      where: { status: 'PENDING' },
      orderBy: [{ customerSequence: 'asc' }, { createdAt: 'asc' }],
      take: limit,
    });

    return records.map((r) => ({
      eventId: r.eventId,
      aggregateId: r.aggregateId,
      customerSequence: Number(r.customerSequence),
      eventType: r.eventType as 'PurchaseCompleted' | 'PurchaseInvalidated',
      envelope: r.payloadJson as unknown as IntegrationEventEnvelopeV1,
      status: r.status as 'PENDING' | 'PUBLISHED' | 'FAILED',
      attemptCount: r.attemptCount,
      createdAt: r.createdAt.toISOString(),
      publishedAt: r.publishedAt?.toISOString(),
    }));
  }

  async markPublished(eventId: string, publishedAt: string): Promise<void> {
    await this.prisma.outboxEvent.update({
      where: { eventId },
      data: {
        status: 'PUBLISHED',
        publishedAt: new Date(publishedAt),
      },
    });
  }

  async markFailed(eventId: string, _error: string): Promise<void> {
    await this.prisma.outboxEvent.update({
      where: { eventId },
      data: {
        status: 'PENDING',
        attemptCount: { increment: 1 },
      },
    });
  }
}
