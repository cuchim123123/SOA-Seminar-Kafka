import { Inject, Injectable } from '@nestjs/common';
import {
  OUTBOX_REPOSITORY_PORT,
  OutboxRepositoryPort,
} from '../../application/ports/outbox-repository.port';
import {
  PurchaseReadDaoPort,
  PurchaseReadModel,
} from '../../application/ports/purchase-read-dao.port';
import {
  PURCHASE_REPOSITORY_PORT,
  PurchaseRepositoryPort,
} from '../../application/ports/purchase-repository.port';
import { InMemoryOutboxRepositoryAdapter } from './in-memory-outbox-repository.adapter';

@Injectable()
export class InMemoryPurchaseReadDaoAdapter implements PurchaseReadDaoPort {
  constructor(
    @Inject(PURCHASE_REPOSITORY_PORT)
    private readonly purchaseRepository: PurchaseRepositoryPort,
    @Inject(OUTBOX_REPOSITORY_PORT)
    private readonly outboxRepository: OutboxRepositoryPort,
  ) {}

  async findById(purchaseId: string): Promise<PurchaseReadModel | null> {
    const purchase = await this.purchaseRepository.findById(purchaseId);
    if (!purchase) {
      return null;
    }

    let outboxStatus: 'PENDING' | 'PUBLISHED' | 'FAILED' | 'UNKNOWN' = 'UNKNOWN';
    let publishedAt: string | null = null;
    let eventId: string | null = null;

    if (this.outboxRepository instanceof InMemoryOutboxRepositoryAdapter) {
      const pendingEvents = await this.outboxRepository.findPending(100);
      const matched = pendingEvents.find((e) => e.envelope.payload.purchaseId === purchaseId);
      if (matched) {
        outboxStatus = matched.status;
        publishedAt = matched.publishedAt ?? null;
        eventId = matched.eventId;
      }
    }

    return {
      purchaseId: purchase.purchaseId,
      customerId: purchase.customerId,
      amountCents: purchase.amountCents,
      status: purchase.status,
      customerSequence: purchase.customerSequence,
      createdAt: purchase.createdAt,
      outboxStatus,
      publishedAt,
      eventId,
    };
  }
}
