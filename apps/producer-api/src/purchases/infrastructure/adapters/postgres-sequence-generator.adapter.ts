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
    // 1. Attempt initial insertion for new customer (sequence 1 assigned, next is 2)
    const insertResult = await this.client.$executeRawUnsafe(
      `INSERT INTO purchase_customers (customer_id, next_event_sequence)
       VALUES ($1, 2)
       ON CONFLICT (customer_id) DO NOTHING`,
      customerId,
    );

    if (insertResult === 1) {
      return 1;
    }

    // 2. Existing customer: atomically lock row, increment sequence, and return previously assigned sequence
    const rows = await this.client.$queryRawUnsafe<Array<{ assigned_sequence: bigint }>>(
      `UPDATE purchase_customers
       SET next_event_sequence = next_event_sequence + 1
       WHERE customer_id = $1
       RETURNING (next_event_sequence - 1)::bigint AS assigned_sequence`,
      customerId,
    );

    if (!rows || rows.length === 0) {
      throw new Error(`Failed to allocate customer sequence for customer '${customerId}'`);
    }

    return Number(rows[0].assigned_sequence);
  }
}
