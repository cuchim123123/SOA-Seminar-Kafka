import { Inject, Injectable } from '@nestjs/common';
import { Prisma, PrismaClient } from '@prisma/client-analytics';
import { PrismaService } from '../../../database/prisma.service';
import { SalesSummaryRepositoryPort } from '../../application/ports/sales-summary-repository.port';
import { SalesSummary } from '../../domain/sales-summary.entity';

type PrismaAnalyticsClient = PrismaClient | Prisma.TransactionClient;

interface RawAnalyticsSummary {
  id: string;
  total_revenue_cents: bigint;
  purchase_count: bigint;
  customer_ids: string[];
  last_event_id: string | null;
  last_processed_at: Date | null;
}

@Injectable()
export class PostgresSalesSummaryRepositoryAdapter implements SalesSummaryRepositoryPort {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaAnalyticsClient,
  ) {}

  async get(): Promise<SalesSummary> {
    // Lock the row with FOR UPDATE to prevent race conditions during concurrent events
    const rows = await this.prisma.$queryRaw<RawAnalyticsSummary[]>`
      SELECT id, total_revenue_cents, purchase_count, customer_ids, last_event_id, last_processed_at
      FROM analytics_summary
      WHERE id = 'global'
      FOR UPDATE;
    `;

    if (rows.length === 0) {
      // First access: initialize the global aggregate
      await this.prisma.analyticsSummary.upsert({
        where: { id: 'global' },
        create: {
          id: 'global',
          totalRevenueCents: 0n,
          purchaseCount: 0n,
          customerIds: [],
          lastEventId: null,
          lastProcessedAt: null,
        },
        update: {},
      });
      return SalesSummary.create();
    }

    const row = rows[0];
    const customerIdsArray = Array.isArray(row.customer_ids) ? row.customer_ids : [];

    return SalesSummary.reconstitute({
      totalRevenueCents: Number(row.total_revenue_cents),
      purchaseCount: Number(row.purchase_count),
      uniqueCustomerIds: new Set(customerIdsArray),
      lastEventId: row.last_event_id,
      lastProcessedAt: row.last_processed_at ? new Date(row.last_processed_at) : null,
    });
  }

  async save(summary: SalesSummary): Promise<void> {
    const customerIdsArray = Array.from(summary.uniqueCustomerIds);
    await this.prisma.analyticsSummary.upsert({
      where: { id: 'global' },
      create: {
        id: 'global',
        totalRevenueCents: BigInt(summary.totalRevenueCents),
        purchaseCount: BigInt(summary.purchaseCount),
        customerIds: customerIdsArray,
        lastEventId: summary.lastEventId,
        lastProcessedAt: summary.lastProcessedAt,
      },
      update: {
        totalRevenueCents: BigInt(summary.totalRevenueCents),
        purchaseCount: BigInt(summary.purchaseCount),
        customerIds: customerIdsArray,
        lastEventId: summary.lastEventId,
        lastProcessedAt: summary.lastProcessedAt,
      },
    });
  }
}
