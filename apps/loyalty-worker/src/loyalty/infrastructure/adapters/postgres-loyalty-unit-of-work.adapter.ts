import { Inject, Injectable } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import {
  LoyaltyUnitOfWorkContext,
  LoyaltyUnitOfWorkPort,
} from '../../application/ports/loyalty-unit-of-work.port';
import { PostgresConsumerIdempotencyRepositoryAdapter } from './postgres-consumer-idempotency-repository.adapter';
import { PostgresLoyaltyAccountRepositoryAdapter } from './postgres-loyalty-account-repository.adapter';

@Injectable()
export class PostgresLoyaltyUnitOfWorkAdapter implements LoyaltyUnitOfWorkPort {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
  ) {}

  async execute<T>(work: (context: LoyaltyUnitOfWorkContext) => Promise<T>): Promise<T> {
    return await this.prisma.$transaction(
      async (tx) => {
        const context: LoyaltyUnitOfWorkContext = {
          loyaltyRepository: new PostgresLoyaltyAccountRepositoryAdapter(tx),
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
