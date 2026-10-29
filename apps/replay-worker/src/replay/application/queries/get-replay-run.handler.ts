import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import {
  REPLAY_RUN_REPOSITORY_PORT,
  ReplayRunRepositoryPort,
} from '../ports/replay-run-repository.port';
import { GetReplayRunQuery } from './get-replay-run.query';

export interface ReplayRunDetails {
  readonly id: string;
  readonly groupId: string;
  readonly topic: string;
  readonly status: string;
  readonly startOffsets: Record<number, string>;
  readonly highWatermarks: Record<number, string>;
  readonly scannedCount: number;
  readonly errorMessage?: string | null;
  readonly startedAt?: Date | null;
  readonly completedAt?: Date | null;
  readonly createdAt: Date;
  readonly updatedAt: Date;
}

@Injectable()
export class GetReplayRunHandler {
  constructor(
    @Inject(REPLAY_RUN_REPOSITORY_PORT)
    private readonly runRepository: ReplayRunRepositoryPort,
  ) {}

  async execute(query: GetReplayRunQuery): Promise<ReplayRunDetails> {
    const run = await this.runRepository.findById(query.runId);
    if (!run) {
      throw new NotFoundException(`Replay run '${query.runId}' not found`);
    }

    return {
      id: run.id,
      groupId: run.groupId,
      topic: run.topic,
      status: run.status,
      startOffsets: run.startOffsets,
      highWatermarks: run.highWatermarks,
      scannedCount: run.scannedCount,
      errorMessage: run.errorMessage,
      startedAt: run.startedAt,
      completedAt: run.completedAt,
      createdAt: run.createdAt,
      updatedAt: run.updatedAt,
    };
  }
}
