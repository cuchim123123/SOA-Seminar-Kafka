import { Inject, Injectable } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
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
        maxWait: 10000,
        timeout: 30000,
      },
    );

    return result;
  }
}
