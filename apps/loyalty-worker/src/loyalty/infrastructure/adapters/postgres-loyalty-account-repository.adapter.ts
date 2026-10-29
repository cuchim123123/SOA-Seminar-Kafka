import { Inject, Injectable } from '@nestjs/common';
import { Prisma, PrismaClient } from '@prisma/client-loyalty';
import { PrismaService } from '../../../database/prisma.service';
import { LoyaltyAccountRepositoryPort } from '../../application/ports/loyalty-account-repository.port';
import { LoyaltyAccount } from '../../domain/loyalty-account.entity';
import { Tier } from '../../domain/loyalty-policy';

type PrismaLoyaltyClient = PrismaClient | Prisma.TransactionClient;

@Injectable()
export class PostgresLoyaltyAccountRepositoryAdapter implements LoyaltyAccountRepositoryPort {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaLoyaltyClient,
  ) {}

  async findByCustomerId(customerId: string): Promise<LoyaltyAccount | null> {
    const record = await this.prisma.loyaltyAccount.findUnique({
      where: { customerId },
    });
    if (!record) {
      return null;
    }
    return LoyaltyAccount.reconstitute({
      customerId: record.customerId,
      tier: record.tier as Tier,
      totalEligibleSpendCents: Number(record.eligibleSpendCents),
      pointsBalance: Number(record.points),
      lastCustomerSequence: Number(record.lastCustomerSequence),
      correctionRequired: record.correctionRequired,
    });
  }

  async save(account: LoyaltyAccount): Promise<void> {
    await this.prisma.loyaltyAccount.upsert({
      where: { customerId: account.customerId },
      create: {
        customerId: account.customerId,
        eligibleSpendCents: BigInt(account.totalEligibleSpendCents),
        points: BigInt(account.pointsBalance),
        tier: account.tier,
        lastCustomerSequence: BigInt(account.lastCustomerSequence),
        correctionRequired: account.correctionRequired,
      },
      update: {
        eligibleSpendCents: BigInt(account.totalEligibleSpendCents),
        points: BigInt(account.pointsBalance),
        tier: account.tier,
        lastCustomerSequence: BigInt(account.lastCustomerSequence),
        correctionRequired: account.correctionRequired,
      },
    });
  }
}
