import { Injectable } from '@nestjs/common';
import {
  SalesSummaryReadDaoPort,
  SalesSummaryReadModel,
} from '../../application/ports/sales-summary-read-dao.port';
import { SalesSummaryRepositoryPort } from '../../application/ports/sales-summary-repository.port';
import { SalesSummary } from '../../domain/sales-summary.entity';

/**
 * Phase 3 In-Memory Sales Summary Storage Adapter.
 *
 * Implements both:
 * 1. SalesSummaryRepositoryPort (Command side: protects invariants, reconstitutes Domain Aggregate)
 * 2. SalesSummaryReadDaoPort (Query side: directly penetrates storage to return flat read projections)
 *
 * Clearly labeled as an in-process fixture until Phase 4 introduces PostgreSQL durability.
 */
@Injectable()
export class InMemorySalesSummaryRepositoryAdapter
  implements SalesSummaryRepositoryPort, SalesSummaryReadDaoPort
{
  private totalRevenueCents = 0;
  private purchaseCount = 0;
  private uniqueCustomerIds = new Set<string>();
  private lastEventId: string | null = null;
  private lastProcessedAt: Date | null = null;

  async get(): Promise<SalesSummary> {
    return SalesSummary.reconstitute({
      totalRevenueCents: this.totalRevenueCents,
      purchaseCount: this.purchaseCount,
      uniqueCustomerIds: new Set(this.uniqueCustomerIds),
      lastEventId: this.lastEventId,
      lastProcessedAt: this.lastProcessedAt,
    });
  }

  async save(summary: SalesSummary): Promise<void> {
    this.totalRevenueCents = summary.totalRevenueCents;
    this.purchaseCount = summary.purchaseCount;
    this.uniqueCustomerIds = summary.uniqueCustomerIds;
    this.lastEventId = summary.lastEventId;
    this.lastProcessedAt = summary.lastProcessedAt;
  }

  async getSummary(): Promise<SalesSummaryReadModel> {
    return {
      totalRevenueCents: this.totalRevenueCents,
      purchaseCount: this.purchaseCount,
      uniqueCustomerCount: this.uniqueCustomerIds.size,
      lastEventId: this.lastEventId,
      lastProcessedAt: this.lastProcessedAt ? this.lastProcessedAt.toISOString() : null,
    };
  }

  // Helper for test fixtures
  clear(): void {
    this.totalRevenueCents = 0;
    this.purchaseCount = 0;
    this.uniqueCustomerIds.clear();
    this.lastEventId = null;
    this.lastProcessedAt = null;
  }
}
