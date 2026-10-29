import { Inject, Injectable } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import {
  SalesSummaryReadDaoPort,
  SalesSummaryReadModel,
} from '../../application/ports/sales-summary-read-dao.port';

@Injectable()
export class PostgresSalesSummaryReadDaoAdapter implements SalesSummaryReadDaoPort {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
  ) {}

  async getSummary(): Promise<SalesSummaryReadModel> {
    const row = await this.prisma.analyticsSummary.findUnique({
      where: { id: 'global' },
    });

    if (!row) {
      return {
        totalRevenueCents: 0,
        purchaseCount: 0,
        uniqueCustomerCount: 0,
        lastEventId: null,
        lastProcessedAt: null,
      };
    }

    const customerIds = Array.isArray(row.customerIds) ? row.customerIds : [];

    return {
      totalRevenueCents: Number(row.totalRevenueCents),
      purchaseCount: Number(row.purchaseCount),
      uniqueCustomerCount: customerIds.length,
      lastEventId: row.lastEventId,
      lastProcessedAt: row.lastProcessedAt ? row.lastProcessedAt.toISOString() : null,
    };
  }
}
