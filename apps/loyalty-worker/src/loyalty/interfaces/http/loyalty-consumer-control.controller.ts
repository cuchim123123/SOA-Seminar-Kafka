import { Body, Controller, Get, Inject, Param, ParseIntPipe, Post } from '@nestjs/common';
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
  async getStatus(): Promise<LoyaltyConsumerControlStatus> {
    return this.consumerControl.getStatus();
  }

  @Post('scale')
  async scale(
    @Body() body: { targetInstances: number },
  ): Promise<{ status: string; targetCount: number; instances: unknown[] }> {
    const target = typeof body?.targetInstances === 'number' ? body.targetInstances : 1;
    const res = await this.consumerControl.scaleInstances(target);
    return { status: 'scaled', targetCount: res.targetCount, instances: res.instances };
  }

  @Post('kill-replica')
  async killReplica(
    @Body() body?: { instanceId?: string },
  ): Promise<{ status: string; killedInstanceId: string; remainingCount: number }> {
    const res = await this.consumerControl.killReplicaInstance(body?.instanceId);
    return {
      status: 'killed',
      killedInstanceId: res.killedInstanceId,
      remainingCount: res.remainingCount,
    };
  }

  @Get('rebalance-history')
  getRebalanceHistory() {
    return this.consumerControl.getRebalanceHistory();
  }
}
