import { AppConfigService } from '../../../config/app-config.service';
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
    @Inject(AppConfigService) private readonly config: AppConfigService,
  ) {}

  async execute<T>(work: (context: LoyaltyUnitOfWorkContext) => Promise<T>): Promise<T> {
    return await this.prisma.$transaction(
      async (tx) => {
        const repository = new PostgresLoyaltyAccountRepositoryAdapter(tx);
        const context: LoyaltyUnitOfWorkContext = {
          loyaltyRepository: {
            findByCustomerId: async (customerId) => {
              // Shared protocol with replay cutover. Unlike a row lock, this also
              // serializes first-account creation. Held through commit/rollback.
              await tx.$executeRaw`SELECT pg_advisory_xact_lock(hashtextextended(${'loyalty-account:' + customerId}, 0))`;
              return repository.findByCustomerId(customerId);
            },
            save: (account) => repository.save(account),
          },
          idempotencyRepository: new PostgresConsumerIdempotencyRepositoryAdapter(tx),
        };
        const result = await work(context);
        await tx.$executeRaw`SELECT pg_notify(${this.config.changeFeedOptions.channel}, '')`;
        return result;
      },
      {
        maxWait: 5000,
        timeout: 10000,
      },
    );
  }
}
