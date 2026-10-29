import { Inject, Injectable } from '@nestjs/common';
import { uuidv7 } from 'uuidv7';
import { ReplayCustomerCorrector, ReplayInputEvent } from '../../domain/replay-customer-corrector';
import { ReplayRun } from '../../domain/replay-run.entity';
import {
  BOUNDED_REPLAY_SCANNER_PORT,
  BoundedReplayScannerPort,
} from '../ports/bounded-replay-scanner.port';
import {
  REPLAY_RUN_REPOSITORY_PORT,
  ReplayRunRepositoryPort,
} from '../ports/replay-run-repository.port';
import {
  REPLAY_STAGING_REPOSITORY_PORT,
  ReplayStagingRepositoryPort,
} from '../ports/replay-staging-repository.port';
import {
  TOPIC_BOUNDARY_INSPECTOR_PORT,
  TopicBoundaryInspectorPort,
} from '../ports/topic-boundary-inspector.port';
import { StartBoundedReplayCommand } from './start-bounded-replay.command';

export interface StartBoundedReplayResult {
  readonly id: string;
  readonly groupId: string;
  readonly topic: string;
  readonly status: string;
  readonly startOffsets: Record<number, string>;
  readonly highWatermarks: Record<number, string>;
  readonly scannedCount: number;
  readonly stagingCount: number;
  readonly finalCommittedOffsets: Record<number, string>;
  readonly startedAt?: Date | null;
  readonly completedAt?: Date | null;
}

@Injectable()
export class StartBoundedReplayHandler {
  constructor(
    @Inject(REPLAY_RUN_REPOSITORY_PORT)
    private readonly runRepository: ReplayRunRepositoryPort,
    @Inject(REPLAY_STAGING_REPOSITORY_PORT)
    private readonly stagingRepository: ReplayStagingRepositoryPort,
    @Inject(TOPIC_BOUNDARY_INSPECTOR_PORT)
    private readonly boundaryInspector: TopicBoundaryInspectorPort,
    @Inject(BOUNDED_REPLAY_SCANNER_PORT)
    private readonly replayScanner: BoundedReplayScannerPort,
  ) {}

  async execute(command: StartBoundedReplayCommand): Promise<StartBoundedReplayResult> {
    const runId = command.runId || uuidv7();
    const groupId = `replay-${runId}`;
    const topic = command.topic || 'purchase.events';

    // 1. Capture per-partition log-end high-watermarks and start offsets (fixed boundary)
    const boundaries = await this.boundaryInspector.getTopicBoundaries(topic);

    // 2. Initialize domain aggregate
    const run = ReplayRun.create({
      id: runId,
      groupId,
      topic,
      startOffsets: boundaries.startOffsets,
      highWatermarks: boundaries.highWatermarks,
    });

    await this.runRepository.save(run);

    try {
      // 3. Mark run as running
      run.start();
      await this.runRepository.save(run);

      const scannedEvents: ReplayInputEvent[] = [];

      // 4. Execute bounded scan with the dedicated on-demand consumer group
      const scanResult = await this.replayScanner.scanBoundedTopic({
        topic,
        groupId,
        startOffsets: boundaries.startOffsets,
        highWatermarks: boundaries.highWatermarks,
        onRecord: async (record) => {
          run.recordScan(1);

          if (record.value && typeof record.value === 'object') {
            const env = record.value as Record<string, unknown>;
            const eventType = String(env.eventType || '');
            const aggregateId = String(env.aggregateId || record.key || '');
            const customerSequence =
              typeof env.customerSequence === 'number'
                ? env.customerSequence
                : Number(env.customerSequence || 0);
            const payload = (
              env.payload && typeof env.payload === 'object' ? env.payload : {}
            ) as Record<string, unknown>;

            scannedEvents.push({
              eventType,
              aggregateId,
              customerSequence,
              payload: {
                customerId: payload.customerId ? String(payload.customerId) : aggregateId,
                purchaseId: payload.purchaseId ? String(payload.purchaseId) : undefined,
                amountCents:
                  typeof payload.amountCents === 'number' ? payload.amountCents : undefined,
                reason: payload.reason ? String(payload.reason) : undefined,
              },
            });
          }
        },
      });

      // 5. Execute two-pass stream correction
      const correctionResult = ReplayCustomerCorrector.executeTwoPassCorrection({
        runId: run.id,
        events: scannedEvents,
      });

      // 6. Persist recomputed customer state into isolated staging
      for (const stagingRecord of correctionResult.stagingRecords) {
        await this.stagingRepository.upsertRecord(stagingRecord);
      }

      // 7. Mark run as completed
      run.complete();
      await this.runRepository.save(run);

      return {
        id: run.id,
        groupId: run.groupId,
        topic: run.topic,
        status: run.status,
        startOffsets: run.startOffsets,
        highWatermarks: run.highWatermarks,
        scannedCount: scanResult.scannedCount,
        stagingCount: correctionResult.stagingRecords.length,
        finalCommittedOffsets: scanResult.finalCommittedOffsets,
        startedAt: run.startedAt,
        completedAt: run.completedAt,
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      run.fail(errorMessage);
      await this.runRepository.save(run);
      throw error;
    }
  }
}
