import { Inject, Injectable, Optional } from '@nestjs/common';
import { StructuredLogger } from '@lab/observability';
import { AppConfigService } from '../../../config/app-config.service';
import { PrismaService } from '../../../database/prisma.service';
import { UnitOfWorkContext, UnitOfWorkPort } from '../../application/ports/unit-of-work.port';
import { OutboxRelayService } from '../services/outbox-relay.service';
import { PostgresHttpIdempotencyRepositoryAdapter } from './postgres-http-idempotency-repository.adapter';
import { PostgresOutboxRepositoryAdapter } from './postgres-outbox-repository.adapter';
import { PostgresPurchaseRepositoryAdapter } from './postgres-purchase-repository.adapter';
import { PostgresSequenceGeneratorAdapter } from './postgres-sequence-generator.adapter';

@Injectable()
export class PostgresUnitOfWorkAdapter implements UnitOfWorkPort {
  private readonly logger = new StructuredLogger(PostgresUnitOfWorkAdapter.name, {
    serviceName: 'producer-api',
  });

  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
    @Inject(OutboxRelayService)
    private readonly outboxRelay: OutboxRelayService,
    @Optional()
    @Inject(AppConfigService)
    private readonly config?: AppConfigService,
  ) {}

  async execute<T>(work: (context: UnitOfWorkContext) => Promise<T>): Promise<T> {
    const result = await this.prisma.$transaction(
      async (tx) => {
        const context: UnitOfWorkContext = {
          purchaseRepository: new PostgresPurchaseRepositoryAdapter(tx),
          outboxRepository: new PostgresOutboxRepositoryAdapter(tx),
          sequenceGenerator: new PostgresSequenceGeneratorAdapter(tx),
          idempotencyRepository: new PostgresHttpIdempotencyRepositoryAdapter(tx),
        };

        return await work(context);
      },
      {
        maxWait: 5000,
        timeout: 10000,
      },
    );

    if (this.config?.outboxRelayInProcess ?? true) {
      setImmediate(() => {
        this.outboxRelay.processPending().catch((err: unknown) => {
          this.logger.error(`Error in outbox relay post-commit hook: ${err}`);
        });
      });
    }

    return result;
  }
}
