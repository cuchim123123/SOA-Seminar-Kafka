import { Inject, Injectable } from '@nestjs/common';
import { Prisma, PrismaClient } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { Purchase } from '../../domain/purchase.entity';
import {
  FindPurchaseOptions,
  PurchaseRepositoryPort,
} from '../../application/ports/purchase-repository.port';

@Injectable()
export class PostgresPurchaseRepositoryAdapter implements PurchaseRepositoryPort {
  constructor(
    @Inject(PrismaService)
    private readonly client: Prisma.TransactionClient | PrismaClient,
  ) {}

  async save(purchase: Purchase): Promise<void> {
    await this.client.purchase.upsert({
      where: { purchaseId: purchase.purchaseId },
      update: {
        amountCents: BigInt(purchase.amountCents),
        status: purchase.status,
        completionSequence: BigInt(purchase.customerSequence),
        invalidationSequence: purchase.invalidationSequence
          ? BigInt(purchase.invalidationSequence)
          : null,
        invalidationEventId: purchase.invalidationEventId ?? null,
        invalidationReason: purchase.invalidationReason ?? null,
      },
      create: {
        purchaseId: purchase.purchaseId,
        customerId: purchase.customerId,
        amountCents: BigInt(purchase.amountCents),
        status: purchase.status,
        completionSequence: BigInt(purchase.customerSequence),
        invalidationSequence: purchase.invalidationSequence
          ? BigInt(purchase.invalidationSequence)
          : null,
        invalidationEventId: purchase.invalidationEventId ?? null,
        invalidationReason: purchase.invalidationReason ?? null,
      },
    });
  }

  async findById(purchaseId: string, options?: FindPurchaseOptions): Promise<Purchase | null> {
    if (options?.forUpdate) {
      const records = await this.client.$queryRawUnsafe<
        Array<{
          purchase_id: string;
          customer_id: string;
          amount_cents: bigint;
          status: string;
          completion_sequence: bigint;
          invalidation_sequence: bigint | null;
          invalidation_event_id: string | null;
          invalidation_reason: string | null;
          created_at: Date;
        }>
      >(
        `SELECT purchase_id, customer_id, amount_cents, status, completion_sequence,
                invalidation_sequence, invalidation_event_id, invalidation_reason, created_at
         FROM purchases
         WHERE purchase_id = $1
         FOR UPDATE`,
        purchaseId,
      );

      if (!records || records.length === 0) {
        return null;
      }

      const record = records[0];
      return Purchase.reconstitute({
        purchaseId: record.purchase_id,
        customerId: record.customer_id,
        amountCents: Number(record.amount_cents),
        customerSequence: Number(record.completion_sequence),
        invalidationSequence: record.invalidation_sequence
          ? Number(record.invalidation_sequence)
          : undefined,
        invalidationEventId: record.invalidation_event_id ?? undefined,
        invalidationReason: record.invalidation_reason ?? undefined,
        status: record.status as 'COMPLETED' | 'INVALIDATED',
        createdAt: record.created_at.toISOString(),
      });
    }

    const record = await this.client.purchase.findUnique({
      where: { purchaseId },
    });

    if (!record) {
      return null;
    }

    return Purchase.reconstitute({
      purchaseId: record.purchaseId,
      customerId: record.customerId,
      amountCents: Number(record.amountCents),
      customerSequence: Number(record.completionSequence),
      invalidationSequence: record.invalidationSequence
        ? Number(record.invalidationSequence)
        : undefined,
      invalidationEventId: record.invalidationEventId ?? undefined,
      invalidationReason: record.invalidationReason ?? undefined,
      status: record.status as 'COMPLETED' | 'INVALIDATED',
      createdAt: record.createdAt.toISOString(),
    });
  }
}
