import { Inject, Injectable } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import {
  PurchaseReadDaoPort,
  PurchaseReadModel,
} from '../../application/ports/purchase-read-dao.port';

@Injectable()
export class PostgresPurchaseReadDaoAdapter implements PurchaseReadDaoPort {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
  ) {}

  async findById(purchaseId: string): Promise<PurchaseReadModel | null> {
    const purchase = await this.prisma.purchase.findUnique({
      where: { purchaseId },
    });

    if (!purchase) {
      return null;
    }

    const outboxEvent = await this.prisma.outboxEvent.findFirst({
      where: {
        aggregateId: purchase.customerId,
        customerSequence: purchase.completionSequence,
      },
    });

    return {
      purchaseId: purchase.purchaseId,
      customerId: purchase.customerId,
      amountCents: Number(purchase.amountCents),
      status: purchase.status as 'COMPLETED' | 'INVALIDATED',
      customerSequence: Number(purchase.completionSequence),
      createdAt: purchase.createdAt.toISOString(),
      outboxStatus: (outboxEvent?.status as 'PENDING' | 'PUBLISHED' | 'FAILED') ?? 'UNKNOWN',
      publishedAt: outboxEvent?.publishedAt ? outboxEvent.publishedAt.toISOString() : null,
      eventId: outboxEvent?.eventId ?? null,
    };
  }
}
