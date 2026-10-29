import { Inject, Injectable } from '@nestjs/common';
import { Prisma, PrismaClient } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import { Purchase } from '../../domain/purchase.entity';
import { PurchaseRepositoryPort } from '../../application/ports/purchase-repository.port';

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
      },
      create: {
        purchaseId: purchase.purchaseId,
        customerId: purchase.customerId,
        amountCents: BigInt(purchase.amountCents),
        status: purchase.status,
        completionSequence: BigInt(purchase.customerSequence),
      },
    });
  }

  async findById(purchaseId: string): Promise<Purchase | null> {
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
      status: record.status as 'COMPLETED' | 'INVALIDATED',
      createdAt: record.createdAt.toISOString(),
    });
  }
}
