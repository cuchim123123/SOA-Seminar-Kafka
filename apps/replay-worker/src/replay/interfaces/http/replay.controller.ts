import { Body, Controller, Get, Inject, Param, Post } from '@nestjs/common';
import { StartBoundedReplayCommand } from '../../application/commands/start-bounded-replay.command';
import { StartBoundedReplayHandler } from '../../application/commands/start-bounded-replay.handler';
import { GetReplayRunHandler } from '../../application/queries/get-replay-run.handler';
import { GetReplayRunQuery } from '../../application/queries/get-replay-run.query';
import { ReplayRunResponseDto } from './dto/replay-run-response.dto';
import { StartReplayDto } from './dto/start-replay.dto';

@Controller('api/v1/replay')
export class ReplayController {
  constructor(
    @Inject(StartBoundedReplayHandler)
    private readonly startHandler: StartBoundedReplayHandler,
    @Inject(GetReplayRunHandler)
    private readonly getHandler: GetReplayRunHandler,
  ) {}

  @Post('runs')
  async startReplay(@Body() dto?: StartReplayDto): Promise<ReplayRunResponseDto> {
    const result = await this.startHandler.execute(
      new StartBoundedReplayCommand({
        runId: dto?.runId,
        topic: dto?.topic,
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
}
