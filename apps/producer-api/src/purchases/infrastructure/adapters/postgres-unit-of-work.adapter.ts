import { Inject, Injectable } from '@nestjs/common';
import { StructuredLogger } from '@lab/observability';
import { PrismaService } from '../../../database/prisma.service';
import { UnitOfWorkContext, UnitOfWorkPort } from '../../application/ports/unit-of-work.port';
import { OutboxRelayService } from '../services/outbox-relay.service';
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
  ) {}

  async execute<T>(work: (context: UnitOfWorkContext) => Promise<T>): Promise<T> {
    const result = await this.prisma.$transaction(
      async (tx) => {
        const context: UnitOfWorkContext = {
          purchaseRepository: new PostgresPurchaseRepositoryAdapter(tx),
          outboxRepository: new PostgresOutboxRepositoryAdapter(tx),
          sequenceGenerator: new PostgresSequenceGeneratorAdapter(tx),
        };

        return await work(context);
      },
      {
        maxWait: 5000,
        timeout: 10000,
      },
    );

    // Post-commit hook: trigger asynchronous outbox relay to Kafka after DB commit
    setImmediate(() => {
      this.outboxRelay.processPending().catch((err: unknown) => {
        this.logger.error(`Error in outbox relay post-commit hook: ${err}`);
      });
    });

    return result;
  }
}
