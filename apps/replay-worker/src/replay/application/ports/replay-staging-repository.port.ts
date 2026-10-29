import { ReplayStagingRecord } from '../../domain/replay-staging-record.entity';

export const REPLAY_STAGING_REPOSITORY_PORT = Symbol('REPLAY_STAGING_REPOSITORY_PORT');

export interface ReplayStagingRepositoryPort {
  upsertRecord(record: ReplayStagingRecord): Promise<void>;
  findByRunId(runId: string): Promise<ReplayStagingRecord[]>;
}
