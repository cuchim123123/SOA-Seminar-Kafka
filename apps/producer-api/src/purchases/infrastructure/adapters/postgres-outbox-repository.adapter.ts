import { Inject, Injectable } from '@nestjs/common';
import { Prisma, PrismaClient } from '@prisma/client';
import { IntegrationEventEnvelopeV1 } from '@lab/contracts';
import { PrismaService } from '../../../database/prisma.service';
import { OutboxRecord, OutboxRepositoryPort } from '../../application/ports/outbox-repository.port';

@Injectable()
export class PostgresOutboxRepositoryAdapter implements OutboxRepositoryPort {
  constructor(
    @Inject(PrismaService)
    private readonly client: Prisma.TransactionClient | PrismaClient,
  ) {}

  async save(record: OutboxRecord): Promise<void> {
    await this.client.outboxEvent.upsert({
      where: { eventId: record.eventId },
      update: {
        status: record.status,
        attemptCount: record.attemptCount,
        publishedAt: record.publishedAt ? new Date(record.publishedAt) : null,
      },
      create: {
        eventId: record.eventId,
        aggregateId: record.aggregateId,
        customerSequence: BigInt(record.customerSequence),
        eventType: record.eventType,
        payloadJson: record.envelope as unknown as Prisma.InputJsonValue,
        status: record.status,
        attemptCount: record.attemptCount,
        createdAt: new Date(record.createdAt),
        publishedAt: record.publishedAt ? new Date(record.publishedAt) : null,
      },
    });
  }

  async findPending(limit = 100): Promise<OutboxRecord[]> {
    const records = await this.client.outboxEvent.findMany({
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
    await this.client.outboxEvent.update({
      where: { eventId },
      data: {
        status: 'PUBLISHED',
        publishedAt: new Date(publishedAt),
      },
    });
  }

  async markFailed(eventId: string, _error: string): Promise<void> {
    await this.client.outboxEvent.update({
      where: { eventId },
      data: {
        status: 'FAILED',
        attemptCount: { increment: 1 },
      },
    });
  }
}
