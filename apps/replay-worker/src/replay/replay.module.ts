import { Module } from '@nestjs/common';
import { StartBoundedReplayHandler } from './application/commands/start-bounded-replay.handler';
import { BOUNDED_REPLAY_SCANNER_PORT } from './application/ports/bounded-replay-scanner.port';
import { REPLAY_RUN_REPOSITORY_PORT } from './application/ports/replay-run-repository.port';
import { REPLAY_STAGING_REPOSITORY_PORT } from './application/ports/replay-staging-repository.port';
import { TOPIC_BOUNDARY_INSPECTOR_PORT } from './application/ports/topic-boundary-inspector.port';
import { GetReplayRunHandler } from './application/queries/get-replay-run.handler';
import { KafkaJsBoundedReplayScannerAdapter } from './infrastructure/adapters/kafkajs-bounded-replay-scanner.adapter';
import { KafkaJsTopicBoundaryInspectorAdapter } from './infrastructure/adapters/kafkajs-topic-boundary-inspector.adapter';
import { PostgresReplayRunRepositoryAdapter } from './infrastructure/adapters/postgres-replay-run-repository.adapter';
import { PostgresReplayStagingRepositoryAdapter } from './infrastructure/adapters/postgres-replay-staging-repository.adapter';
import { ReplayController } from './interfaces/http/replay.controller';

@Module({
  controllers: [ReplayController],
  providers: [
    StartBoundedReplayHandler,
    GetReplayRunHandler,
    {
      provide: REPLAY_RUN_REPOSITORY_PORT,
      useClass: PostgresReplayRunRepositoryAdapter,
    },
    {
      provide: REPLAY_STAGING_REPOSITORY_PORT,
      useClass: PostgresReplayStagingRepositoryAdapter,
    },
    {
      provide: TOPIC_BOUNDARY_INSPECTOR_PORT,
      useClass: KafkaJsTopicBoundaryInspectorAdapter,
    },
    {
      provide: BOUNDED_REPLAY_SCANNER_PORT,
      useClass: KafkaJsBoundedReplayScannerAdapter,
    },
  ],
  exports: [
    StartBoundedReplayHandler,
    GetReplayRunHandler,
    REPLAY_RUN_REPOSITORY_PORT,
    REPLAY_STAGING_REPOSITORY_PORT,
  ],
})
export class ReplayModule {}
