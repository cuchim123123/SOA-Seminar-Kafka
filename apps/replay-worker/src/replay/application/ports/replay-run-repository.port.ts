import { ReplayRun } from '../../domain/replay-run.entity';

export const REPLAY_RUN_REPOSITORY_PORT = Symbol('REPLAY_RUN_REPOSITORY_PORT');

export interface ReplayRunRepositoryPort {
  save(run: ReplayRun): Promise<void>;
  findById(id: string): Promise<ReplayRun | null>;
  findLatest(): Promise<ReplayRun | null>;
}
