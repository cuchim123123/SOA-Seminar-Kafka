import { Inject, Injectable } from '@nestjs/common';
import { Prisma, PrismaClient } from '@prisma/client';
import { IntegrationEventEnvelopeV1 } from '@lab/contracts';
import { PrismaService } from '../../../database/prisma.service';
import { OutboxRecord, OutboxRepositoryPort } from '../../application/ports/outbox-repository.port';

/**
 * Append-only outbox persistence for Debezium CDC (ADR-017).
 *
 * Rows are inserted once with stable event IDs; legacy `status`,
 * `attempt_count` and `published_at` columns keep their database defaults and
 * are never transitioned by the application. Debezium owns publication state.
 */
@Injectable()
export class PostgresOutboxRepositoryAdapter implements OutboxRepositoryPort {
  constructor(
    @Inject(PrismaService)
    private readonly client: Prisma.TransactionClient | PrismaClient,
  ) {}

  async save(record: OutboxRecord): Promise<void> {
    await this.client.outboxEvent.upsert({
      where: { eventId: record.eventId },
      // Stable event ID already appended: keep the original row untouched.
      update: {},
      create: {
        eventId: record.eventId,
        aggregateId: record.aggregateId,
        customerSequence: BigInt(record.customerSequence),
        eventType: record.eventType,
        payloadJson: record.envelope as unknown as Prisma.InputJsonValue,
        createdAt: new Date(record.createdAt),
      },
    });
  }

  async saveMany(records: OutboxRecord[]): Promise<void> {
    if (records.length === 0) return;
    // Stable event IDs: a repeated save inside one batch must not mutate or fail.
    await this.client.outboxEvent.createMany({
      data: records.map((record) => ({
        eventId: record.eventId,
        aggregateId: record.aggregateId,
        customerSequence: BigInt(record.customerSequence),
        eventType: record.eventType,
        payloadJson: record.envelope as unknown as Prisma.InputJsonValue,
        createdAt: new Date(record.createdAt),
      })),
      skipDuplicates: true,
    });
  }

  async findByAggregateAndSequence(
    aggregateId: string,
    customerSequence: number,
  ): Promise<OutboxRecord | null> {
    const r = await this.client.outboxEvent.findUnique({
      where: {
        aggregateId_customerSequence: {
          aggregateId,
          customerSequence: BigInt(customerSequence),
        },
      },
    });

    return r ? this.toRecord(r) : null;
  }

  private toRecord(r: {
    eventId: string;
    aggregateId: string;
    customerSequence: bigint;
    eventType: string;
    payloadJson: Prisma.JsonValue;
    createdAt: Date;
  }): OutboxRecord {
    return {
      eventId: r.eventId,
      aggregateId: r.aggregateId,
      customerSequence: Number(r.customerSequence),
      eventType: r.eventType,
      envelope: r.payloadJson as unknown as IntegrationEventEnvelopeV1,
      createdAt: r.createdAt.toISOString(),
    };
  }
}
