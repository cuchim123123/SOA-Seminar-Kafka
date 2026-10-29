import { Body, Controller, Get, Inject, Param, Post } from '@nestjs/common';
import {
  CutoverReplayRunCommand,
  CutoverReplayRunResult,
} from '../../application/commands/cutover-replay-run.command';
import { CutoverReplayRunHandler } from '../../application/commands/cutover-replay-run.handler';
import { StartBoundedReplayCommand } from '../../application/commands/start-bounded-replay.command';
import { StartBoundedReplayHandler } from '../../application/commands/start-bounded-replay.handler';
import {
  CompareReplayWithLiveHandler,
  CustomerLiveStagingComparisonDto,
} from '../../application/queries/compare-replay-with-live.handler';
import { CompareReplayWithLiveQuery } from '../../application/queries/compare-replay-with-live.query';
import { GetReplayRunHandler } from '../../application/queries/get-replay-run.handler';
import { GetReplayRunQuery } from '../../application/queries/get-replay-run.query';
import {
  GetReplayStagingHandler,
  StagingCustomerDto,
} from '../../application/queries/get-replay-staging.handler';
import { GetReplayStagingQuery } from '../../application/queries/get-replay-staging.query';
import { ReplayRunResponseDto } from './dto/replay-run-response.dto';
import { StartReplayDto } from './dto/start-replay.dto';

@Controller('api/v1/replay')
export class ReplayController {
  constructor(
    @Inject(StartBoundedReplayHandler)
    private readonly startHandler: StartBoundedReplayHandler,
    @Inject(GetReplayRunHandler)
    private readonly getHandler: GetReplayRunHandler,
    @Inject(GetReplayStagingHandler)
    private readonly stagingHandler: GetReplayStagingHandler,
    @Inject(CompareReplayWithLiveHandler)
    private readonly comparisonHandler: CompareReplayWithLiveHandler,
    @Inject(CutoverReplayRunHandler)
    private readonly cutoverHandler: CutoverReplayRunHandler,
  ) {}

  @Post('runs')
  async startReplay(@Body() dto?: StartReplayDto): Promise<ReplayRunResponseDto> {
    const requiredStartOffsets = dto?.requiredStartOffsets
      ? Object.fromEntries(
          Object.entries(dto.requiredStartOffsets).map(([k, v]) => [Number(k), String(v)]),
        )
      : undefined;

    const result = await this.startHandler.execute(
      new StartBoundedReplayCommand({
        runId: dto?.runId,
        topic: dto?.topic,
        requiredStartOffsets,
      }),
    );

    return {
      id: result.id,
      groupId: result.groupId,
      topic: result.topic,
      status: result.status,
      startOffsets: result.startOffsets,
      highWatermarks: result.highWatermarks,
      scannedCount: result.scannedCount,
      finalCommittedOffsets: result.finalCommittedOffsets,
      startedAt: result.startedAt ? result.startedAt.toISOString() : null,
      completedAt: result.completedAt ? result.completedAt.toISOString() : null,
    };
  }

  @Get('runs/:runId')
  async getReplayRun(@Param('runId') runId: string): Promise<ReplayRunResponseDto> {
    const result = await this.getHandler.execute(new GetReplayRunQuery(runId));

    return {
      id: result.id,
      groupId: result.groupId,
      topic: result.topic,
      status: result.status,
      startOffsets: result.startOffsets,
      highWatermarks: result.highWatermarks,
      scannedCount: result.scannedCount,
      errorMessage: result.errorMessage,
      startedAt: result.startedAt ? result.startedAt.toISOString() : null,
      completedAt: result.completedAt ? result.completedAt.toISOString() : null,
    };
  }

  @Get('runs/:runId/staging')
  async getStagingRecords(@Param('runId') runId: string): Promise<StagingCustomerDto[]> {
    return this.stagingHandler.execute(new GetReplayStagingQuery(runId));
  }

  @Get('runs/:runId/comparison/:customerId')
  async compareWithLive(
    @Param('runId') runId: string,
    @Param('customerId') customerId: string,
  ): Promise<CustomerLiveStagingComparisonDto> {
    return this.comparisonHandler.execute(new CompareReplayWithLiveQuery(runId, customerId));
  }

  @Post('runs/:runId/cutover')
  async cutoverReplayRun(@Param('runId') runId: string): Promise<CutoverReplayRunResult> {
    return this.cutoverHandler.execute(new CutoverReplayRunCommand(runId));
  }
}
