import { Inject, Injectable } from '@nestjs/common';
import { Prisma, PrismaClient } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { SequenceGeneratorPort } from '../../application/ports/sequence-generator.port';

@Injectable()
export class PostgresSequenceGeneratorAdapter implements SequenceGeneratorPort {
  constructor(
    @Inject(PrismaService)
    private readonly client: Prisma.TransactionClient | PrismaClient,
  ) {}

  /**
   * Allocates the next monotonic customer sequence under PostgreSQL row lock.
   * Uses raw SQL with row-level locking to guarantee atomic serialization across concurrent requests.
   */
  async nextSequence(customerId: string): Promise<number> {
    const rows = await this.client.$queryRawUnsafe<Array<{ assigned_sequence: bigint }>>(
      `INSERT INTO purchase_customers (customer_id, next_event_sequence)
       VALUES ($1, 2)
       ON CONFLICT (customer_id)
       DO UPDATE SET next_event_sequence = purchase_customers.next_event_sequence + 1
       RETURNING (purchase_customers.next_event_sequence - 1)::bigint AS assigned_sequence`,
      customerId,
    );

    if (!rows || rows.length === 0 || rows[0]?.assigned_sequence === undefined) {
      throw new Error(`Failed to allocate customer sequence for customer '${customerId}'`);
    }

    return Number(rows[0].assigned_sequence);
  }
}
