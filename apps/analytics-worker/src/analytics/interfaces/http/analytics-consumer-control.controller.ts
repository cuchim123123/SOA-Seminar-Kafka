import { Body, Controller, Get, Inject, Post } from '@nestjs/common';
import { PauseAnalyticsConsumerCommand } from '../../application/commands/pause-analytics-consumer.command';
import { PauseAnalyticsConsumerHandler } from '../../application/commands/pause-analytics-consumer.handler';
import { ResumeAnalyticsConsumerCommand } from '../../application/commands/resume-analytics-consumer.command';
import { ResumeAnalyticsConsumerHandler } from '../../application/commands/resume-analytics-consumer.handler';
import {
  CONSUMER_CONTROL_PORT,
  ConsumerControlPort,
  ConsumerControlStatus,
} from '../../application/ports/consumer-control.port';
import { GetConsumerControlStatusHandler } from '../../application/queries/get-consumer-control-status.handler';
import { GetConsumerControlStatusQuery } from '../../application/queries/get-consumer-control-status.query';

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
