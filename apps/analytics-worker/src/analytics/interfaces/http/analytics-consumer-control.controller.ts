import { UseInterceptors } from '@nestjs/common';
import { ChangeFeedMutationInterceptor } from '@lab/observability';
import { Body, Controller, Get, Inject, Param, ParseIntPipe, Post } from '@nestjs/common';
import {
  PauseAnalyticsConsumerCommand,
  PauseAnalyticsConsumerHandler,
} from '../../application/commands/pause-analytics-consumer';
import {
  ResumeAnalyticsConsumerCommand,
  ResumeAnalyticsConsumerHandler,
} from '../../application/commands/resume-analytics-consumer';
import {
  CONSUMER_CONTROL_PORT,
  ConsumerControlPort,
  ConsumerControlStatus,
} from '../../application/ports/consumer-control.port';
import {
  GetConsumerControlStatusHandler,
  GetConsumerControlStatusQuery,
} from '../../application/queries/get-consumer-control-status';

@UseInterceptors(ChangeFeedMutationInterceptor)
@Controller('api/v1/analytics/consumer')
export class AnalyticsConsumerControlController {
  constructor(
    @Inject(PauseAnalyticsConsumerHandler)
    private readonly pauseHandler: PauseAnalyticsConsumerHandler,
    @Inject(ResumeAnalyticsConsumerHandler)
    private readonly resumeHandler: ResumeAnalyticsConsumerHandler,
    @Inject(GetConsumerControlStatusHandler)
    private readonly statusHandler: GetConsumerControlStatusHandler,
    @Inject(CONSUMER_CONTROL_PORT)
    private readonly consumerControl: ConsumerControlPort,
  ) {}

  @Post('pause')
  async pause(): Promise<{ status: string; isPaused: boolean }> {
    const result = await this.pauseHandler.execute(new PauseAnalyticsConsumerCommand({}));
    return { status: 'paused', isPaused: result.isPaused };
  }

  @Post('resume')
  async resume(): Promise<{ status: string; isPaused: boolean }> {
    const result = await this.resumeHandler.execute(new ResumeAnalyticsConsumerCommand({}));
    return { status: 'resumed', isPaused: result.isPaused };
  }

  @Post('partitions/:partition/pause')
  async pausePartition(
    @Param('partition', ParseIntPipe) partition: number,
  ): Promise<{ status: string; partition: number; isPaused: boolean }> {
    await this.consumerControl.pausePartition(partition);
    return { status: 'paused', partition, isPaused: true };
  }

  @Post('partitions/:partition/resume')
  async resumePartition(
    @Param('partition', ParseIntPipe) partition: number,
  ): Promise<{ status: string; partition: number; isPaused: boolean }> {
    await this.consumerControl.resumePartition(partition);
    return { status: 'resumed', partition, isPaused: false };
  }

  @Post('delay')
  async setDelay(
    @Body() body: { delayMs: number },
  ): Promise<{ status: string; processingDelayMs: number }> {
    const delay = typeof body?.delayMs === 'number' ? Math.max(0, body.delayMs) : 0;
    this.consumerControl.setProcessingDelay(delay);
    return { status: 'updated', processingDelayMs: delay };
  }

  @Get('status')
  async getStatus(): Promise<ConsumerControlStatus> {
    return this.statusHandler.execute(new GetConsumerControlStatusQuery({}));
  }
}
