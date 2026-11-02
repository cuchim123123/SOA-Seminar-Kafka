import { Client } from 'pg';
import { PostgresChangeFeed } from '@lab/observability';
import { AppConfigService } from '../config/app-config.service';
import { ReplayChangesController } from './interfaces/http/replay-changes.controller';
import { Module } from '@nestjs/common';
import { CutoverReplayRunHandler } from './application/commands/cutover-replay-run';
import { StartBoundedReplayHandler } from './application/commands/start-bounded-replay';
import { BOUNDED_REPLAY_SCANNER_PORT } from './application/ports/bounded-replay-scanner.port';
import { REPLAY_RUN_REPOSITORY_PORT } from './application/ports/replay-run-repository.port';
import { REPLAY_STAGING_REPOSITORY_PORT } from './application/ports/replay-staging-repository.port';
import { TOPIC_BOUNDARY_INSPECTOR_PORT } from './application/ports/topic-boundary-inspector.port';
import { CompareReplayWithLiveHandler } from './application/queries/compare-replay-with-live';
import { GetReplayRunHandler } from './application/queries/get-replay-run';
import { GetReplayStagingHandler } from './application/queries/get-replay-staging';
import { KafkaJsBoundedReplayScannerAdapter } from './infrastructure/adapters/kafkajs-bounded-replay-scanner.adapter';
import { KafkaJsTopicBoundaryInspectorAdapter } from './infrastructure/adapters/kafkajs-topic-boundary-inspector.adapter';
import { PostgresReplayRunRepositoryAdapter } from './infrastructure/adapters/postgres-replay-run-repository.adapter';
import { PostgresReplayStagingRepositoryAdapter } from './infrastructure/adapters/postgres-replay-staging-repository.adapter';
import { ReplayController } from './interfaces/http/replay.controller';

@Module({
  controllers: [ReplayChangesController, ReplayController],
  providers: [
    {
      provide: PostgresChangeFeed,
      inject: [AppConfigService],
      useFactory: (config: AppConfigService) =>
        new PostgresChangeFeed(
          config.changeFeedOptions,
          () =>
            new Client({
              connectionString: config.databaseUrl,
              connectionTimeoutMillis: config.changeFeedOptions.reconnectMs,
            }),
        ),
    },
    StartBoundedReplayHandler,
    GetReplayRunHandler,
    GetReplayStagingHandler,
    CompareReplayWithLiveHandler,
    CutoverReplayRunHandler,
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
    GetReplayStagingHandler,
    CompareReplayWithLiveHandler,
    CutoverReplayRunHandler,
    REPLAY_RUN_REPOSITORY_PORT,
    REPLAY_STAGING_REPOSITORY_PORT,
  ],
})
export class ReplayModule {}
