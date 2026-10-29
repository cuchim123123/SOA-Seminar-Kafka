import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import { CompareReplayWithLiveQuery } from './compare-replay-with-live.query';

export interface CustomerLiveStagingComparisonDto {
  readonly runId: string;
  readonly customerId: string;
  readonly live: {
    readonly points: number;
    readonly tier: string;
    readonly eligibleSpendCents: number;
    readonly lastCustomerSequence: number;
    readonly correctionRequired: boolean;
  } | null;
  readonly staged: {
    readonly points: number;
    readonly tier: string;
    readonly eligibleSpendCents: number;
    readonly lastCustomerSequence: number;
    readonly scannedEventCount: number;
  } | null;
  readonly discrepancy: {
    readonly pointsDifference: number;
    readonly spendDifferenceCents: number;
    readonly tierChanged: boolean;
  };
}

@Injectable()
export class CompareReplayWithLiveHandler {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
  ) {}

  async execute(query: CompareReplayWithLiveQuery): Promise<CustomerLiveStagingComparisonDto> {
    const { runId, customerId } = query;

    const liveRecord = await this.prisma.loyaltyAccount.findUnique({
      where: { customerId },
    });

    const stagedRecord = await this.prisma.replayStagingRecord.findUnique({
      where: {
        runId_customerId: {
          runId,
          customerId,
        },
      },
    });

    if (!liveRecord && !stagedRecord) {
      throw new NotFoundException(
        `Customer '${customerId}' has neither live nor staged replay records for run '${runId}'`,
      );
    }

    const livePoints = liveRecord ? Number(liveRecord.points) : 0;
    const stagedPoints = stagedRecord ? Number(stagedRecord.points) : 0;
    const liveSpend = liveRecord ? Number(liveRecord.eligibleSpendCents) : 0;
    const stagedSpend = stagedRecord ? Number(stagedRecord.eligibleSpendCents) : 0;
    const liveTier = liveRecord?.tier ?? 'BRONZE';
    const stagedTier = stagedRecord?.tier ?? 'BRONZE';

    return {
      runId,
      customerId,
      live: liveRecord
        ? {
            points: livePoints,
            tier: liveRecord.tier,
            eligibleSpendCents: liveSpend,
            lastCustomerSequence: Number(liveRecord.lastCustomerSequence),
            correctionRequired: liveRecord.correctionRequired,
          }
        : null,
      staged: stagedRecord
        ? {
            points: stagedPoints,
            tier: stagedRecord.tier,
            eligibleSpendCents: stagedSpend,
            lastCustomerSequence: Number(stagedRecord.lastCustomerSequence),
            scannedEventCount: stagedRecord.scannedEventCount,
          }
        : null,
      discrepancy: {
        pointsDifference: stagedPoints - livePoints,
        spendDifferenceCents: stagedSpend - liveSpend,
        tierChanged: liveTier !== stagedTier,
      },
    };
  }
}
