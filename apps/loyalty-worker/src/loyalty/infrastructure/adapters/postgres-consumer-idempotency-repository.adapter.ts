import { Injectable } from '@nestjs/common';
import { Prisma, PrismaClient } from '@prisma/client-loyalty';
import { ConsumerIdempotencyRepositoryPort } from '../../application/ports/consumer-idempotency-repository.port';

type PrismaLoyaltyClient = PrismaClient | Prisma.TransactionClient;

@Injectable()
export class PostgresConsumerIdempotencyRepositoryAdapter implements ConsumerIdempotencyRepositoryPort {
  constructor(private readonly prisma: PrismaLoyaltyClient) {}

  async markProcessedIfNew(consumerName: string, eventId: string): Promise<boolean> {
    const rows = await this.prisma.$queryRaw<Array<{ event_id: string }>>`
      INSERT INTO processed_events (consumer_name, event_id, processed_at)
      VALUES (${consumerName}, ${eventId}, NOW())
      ON CONFLICT (consumer_name, event_id) DO NOTHING
      RETURNING event_id;
    `;
    return rows.length > 0;
  }
}
