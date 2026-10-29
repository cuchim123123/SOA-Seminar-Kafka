import { Inject, Injectable } from '@nestjs/common';
import { StructuredLogger } from '@lab/observability';
import {
  HTTP_IDEMPOTENCY_REPOSITORY_PORT,
  HttpIdempotencyRepositoryPort,
} from '../../application/ports/http-idempotency-repository.port';
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
import { OutboxRelayService } from '../services/outbox-relay.service';

@Injectable()
export class InMemoryUnitOfWorkAdapter implements UnitOfWorkPort {
  private readonly logger = new StructuredLogger(InMemoryUnitOfWorkAdapter.name, {
    serviceName: 'producer-api',
  });

  constructor(
    @Inject(PURCHASE_REPOSITORY_PORT)
    private readonly purchaseRepository: PurchaseRepositoryPort,
    @Inject(OUTBOX_REPOSITORY_PORT)
    private readonly outboxRepository: OutboxRepositoryPort,
    @Inject(SEQUENCE_GENERATOR_PORT)
    private readonly sequenceGenerator: SequenceGeneratorPort,
    @Inject(HTTP_IDEMPOTENCY_REPOSITORY_PORT)
    private readonly idempotencyRepository: HttpIdempotencyRepositoryPort,
    @Inject(OutboxRelayService)
    private readonly outboxRelay: OutboxRelayService,
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

    // 2. Transaction committed successfully: trigger asynchronous outbox relay to Kafka
    // Notice: The command returns immediately without waiting for Kafka broker acknowledgment!
    setImmediate(() => {
      this.outboxRelay.processPending().catch((err: unknown) => {
        this.logger.error(`Error in outbox relay post-commit hook: ${err}`);
      });
    });

    return result;
  }
}
