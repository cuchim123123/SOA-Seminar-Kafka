import { Inject, Injectable } from '@nestjs/common';
import {
  REPLAY_STAGING_REPOSITORY_PORT,
  ReplayStagingRepositoryPort,
} from '../ports/replay-staging-repository.port';
import { GetReplayStagingQuery } from './get-replay-staging.query';

export interface StagingCustomerDto {
  readonly id: string;
  readonly runId: string;
  readonly customerId: string;
  readonly points: number;
  readonly tier: string;
  readonly eligibleSpendCents: number;
  readonly scannedEventCount: number;
  readonly lastCustomerSequence: number;
  readonly createdAt: string;
}

@Injectable()
export class GetReplayStagingHandler {
  constructor(
    @Inject(REPLAY_STAGING_REPOSITORY_PORT)
    private readonly stagingRepository: ReplayStagingRepositoryPort,
  ) {}

  async execute(query: GetReplayStagingQuery): Promise<StagingCustomerDto[]> {
    const records = await this.stagingRepository.findByRunId(query.runId);

    return records.map((r) => ({
      id: r.id,
      runId: r.runId,
      customerId: r.customerId,
      points: Number(r.points),
      tier: r.tier,
      eligibleSpendCents: Number(r.eligibleSpendCents),
      scannedEventCount: r.scannedEventCount,
      lastCustomerSequence: Number(r.lastCustomerSequence),
      createdAt: r.createdAt.toISOString(),
    }));
  }
}
