import { Body, Controller, Get, Inject, Post } from '@nestjs/common';
import {
  LOYALTY_CONSUMER_CONTROL_PORT,
  LoyaltyConsumerControlPort,
  LoyaltyConsumerControlStatus,
} from '../../application/ports/consumer-control.port';

@Controller('api/v1/loyalty/consumer')
export class LoyaltyConsumerControlController {
  constructor(
    @Inject(LOYALTY_CONSUMER_CONTROL_PORT)
    private readonly consumerControl: LoyaltyConsumerControlPort,
  ) {}

  @Post('pause')
  async pause(): Promise<{ status: string; isPaused: boolean }> {
    await this.consumerControl.pause();
    return { status: 'paused', isPaused: true };
  }

  @Post('resume')
  async resume(): Promise<{ status: string; isPaused: boolean }> {
    await this.consumerControl.resume();
    return { status: 'resumed', isPaused: false };
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
  async getStatus(): Promise<LoyaltyConsumerControlStatus> {
    return this.consumerControl.getStatus();
  }
}
