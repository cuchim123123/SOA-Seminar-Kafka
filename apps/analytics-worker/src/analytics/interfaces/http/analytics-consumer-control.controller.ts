import { Controller, Get, Inject, Post } from '@nestjs/common';
import { PauseAnalyticsConsumerCommand } from '../../application/commands/pause-analytics-consumer.command';
import { PauseAnalyticsConsumerHandler } from '../../application/commands/pause-analytics-consumer.handler';
import { ResumeAnalyticsConsumerCommand } from '../../application/commands/resume-analytics-consumer.command';
import { ResumeAnalyticsConsumerHandler } from '../../application/commands/resume-analytics-consumer.handler';
import { ConsumerControlStatus } from '../../application/ports/consumer-control.port';
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

  @Get('status')
  async getStatus(): Promise<ConsumerControlStatus> {
    return this.statusHandler.execute(new GetConsumerControlStatusQuery({}));
  }
}
