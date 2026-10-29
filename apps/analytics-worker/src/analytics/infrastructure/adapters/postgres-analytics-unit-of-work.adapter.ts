import { Inject, Injectable } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import {
  AnalyticsUnitOfWorkContext,
  AnalyticsUnitOfWorkPort,
} from '../../application/ports/analytics-unit-of-work.port';
import { PostgresConsumerIdempotencyRepositoryAdapter } from './postgres-consumer-idempotency-repository.adapter';
import { PostgresSalesSummaryRepositoryAdapter } from './postgres-sales-summary-repository.adapter';

@Injectable()
export class PostgresAnalyticsUnitOfWorkAdapter implements AnalyticsUnitOfWorkPort {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
  ) {}

  async execute<T>(work: (context: AnalyticsUnitOfWorkContext) => Promise<T>): Promise<T> {
    return await this.prisma.$transaction(
      async (tx) => {
        const context: AnalyticsUnitOfWorkContext = {
          salesSummaryRepository: new PostgresSalesSummaryRepositoryAdapter(tx),
          idempotencyRepository: new PostgresConsumerIdempotencyRepositoryAdapter(tx),
        };
        return await work(context);
      },
      {
        maxWait: 5000,
        timeout: 10000,
      },
    );
  }
}
