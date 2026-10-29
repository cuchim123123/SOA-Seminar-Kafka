import { Inject, Injectable } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import {
  LoyaltyAccountReadDaoPort,
  LoyaltyAccountReadModel,
} from '../../application/ports/loyalty-account-read-dao.port';
import { Tier } from '../../domain/loyalty-policy';

@Injectable()
export class PostgresLoyaltyAccountReadDaoAdapter implements LoyaltyAccountReadDaoPort {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
  ) {}

  async findById(customerId: string): Promise<LoyaltyAccountReadModel | null> {
    const record = await this.prisma.loyaltyAccount.findUnique({
      where: { customerId },
    });
    if (!record) {
      return null;
    }
    return {
      customerId: record.customerId,
      tier: record.tier as Tier,
      totalEligibleSpendCents: Number(record.eligibleSpendCents),
      pointsBalance: Number(record.points),
    };
  }
}
