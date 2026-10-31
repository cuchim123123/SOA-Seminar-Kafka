import { Inject, Injectable } from '@nestjs/common';
import {
  CLIENT_IDEMPOTENCY_REPOSITORY_PORT,
  ClientIdempotencyRepositoryPort,
} from '../../application/ports/client-idempotency-repository.port';
import {
  OUTBOX_REPOSITORY_PORT,
  OutboxRepositoryPort,
} from '../../application/ports/outbox-repository.port';
import {
  PURCHASE_REPOSITORY_PORT,
  PurchaseRepositoryPort,
} from '../../application/ports/purchase-repository.port';
import {
  SEQUENCE_GENERATOR_PORT,
  SequenceGeneratorPort,
} from '../../application/ports/sequence-generator.port';
import { UnitOfWorkContext, UnitOfWorkPort } from '../../application/ports/unit-of-work.port';

@Injectable()
export class InMemoryUnitOfWorkAdapter implements UnitOfWorkPort {
  constructor(
    @Inject(PURCHASE_REPOSITORY_PORT)
    private readonly purchaseRepository: PurchaseRepositoryPort,
    @Inject(OUTBOX_REPOSITORY_PORT)
    private readonly outboxRepository: OutboxRepositoryPort,
    @Inject(SEQUENCE_GENERATOR_PORT)
    private readonly sequenceGenerator: SequenceGeneratorPort,
    @Inject(CLIENT_IDEMPOTENCY_REPOSITORY_PORT)
    private readonly idempotencyRepository: ClientIdempotencyRepositoryPort,
  ) {}

  async execute<T>(work: (context: UnitOfWorkContext) => Promise<T>): Promise<T> {
    const context: UnitOfWorkContext = {
      purchaseRepository: this.purchaseRepository,
      outboxRepository: this.outboxRepository,
      sequenceGenerator: this.sequenceGenerator,
      idempotencyRepository: this.idempotencyRepository,
    };

    // 1. Execute transactional boundary across aggregate state and outbox
    const result = await work(context);

    return result;
  }
}
