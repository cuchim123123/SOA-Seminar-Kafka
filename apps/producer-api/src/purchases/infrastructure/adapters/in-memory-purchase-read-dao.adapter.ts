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

/**
 * In-memory read DAO mirroring the PostgreSQL adapter semantics: the outbox
 * row's existence means the event is enqueued for Debezium CDC. CDC does not
 * write broker acknowledgments back into source rows, so `publishedAt` stays
 * null and `outboxStatus` is only ever ENQUEUED or UNKNOWN.
 */
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

    const outboxRecord = await this.outboxRepository.findByAggregateAndSequence(
      purchase.customerId,
      purchase.customerSequence,
    );

    return {
      purchaseId: purchase.purchaseId,
      customerId: purchase.customerId,
      amountCents: purchase.amountCents,
      status: purchase.status,
      customerSequence: purchase.customerSequence,
      createdAt: purchase.createdAt,
      outboxStatus: outboxRecord ? 'ENQUEUED' : 'UNKNOWN',
      publishedAt: null,
      eventId: outboxRecord?.eventId ?? null,
    };
  }
}
