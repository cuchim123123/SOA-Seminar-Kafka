import { Inject, Injectable } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import { AppConfigService } from '../../../config/app-config.service';
import { UnitOfWorkContext, UnitOfWorkPort } from '../../application/ports/unit-of-work.port';
import { PostgresHttpIdempotencyRepositoryAdapter } from './postgres-http-idempotency-repository.adapter';
import { PostgresOutboxRepositoryAdapter } from './postgres-outbox-repository.adapter';
import { PostgresPurchaseRepositoryAdapter } from './postgres-purchase-repository.adapter';
import { PostgresSequenceGeneratorAdapter } from './postgres-sequence-generator.adapter';

@Injectable()
export class PostgresUnitOfWorkAdapter implements UnitOfWorkPort {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
    @Inject(AppConfigService) private readonly config: AppConfigService,
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

        const value = await work(context);
        // Transient UI invalidation, delivered only on COMMIT. Never replaces CDC
        // publication or asserts that a consumer has processed an event.
        await tx.$executeRaw`SELECT pg_notify(${this.config.changeFeedOptions.channel}, '')`;
        return value;
      },
      {
        maxWait: 10000,
        timeout: 30000,
      },
    );

    return result;
  }
}
