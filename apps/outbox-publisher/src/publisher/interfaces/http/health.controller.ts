import { Controller, Get, HttpCode, HttpStatus, Inject, Post } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import {
  OutboxPublisherService,
  PublisherStats,
} from '../../application/services/outbox-publisher.service';

export interface HealthResponse {
  status: 'ok' | 'degraded';
  service: string;
  timestamp: string;
}

export interface ReadinessResponse {
  status: 'ready' | 'not_ready';
  service: string;
  database: 'connected' | 'disconnected';
  stats: PublisherStats;
  timestamp: string;
}

@Controller()
export class HealthController {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
    @Inject(OutboxPublisherService)
    private readonly publisherService: OutboxPublisherService,
  ) {}

  @Get('health')
  getHealth(): HealthResponse {
    return {
      status: 'ok',
      service: 'outbox-publisher',
      timestamp: new Date().toISOString(),
    };
  }

  @Get('health/ready')
  async getReadiness(): Promise<ReadinessResponse> {
    let dbStatus: 'connected' | 'disconnected' = 'disconnected';
    try {
      await this.prisma.$queryRaw`SELECT 1`;
      dbStatus = 'connected';
    } catch {
      dbStatus = 'disconnected';
    }

    const stats = this.publisherService.getStats();
    return {
      status: dbStatus === 'connected' ? 'ready' : 'not_ready',
      service: 'outbox-publisher',
      database: dbStatus,
      stats,
      timestamp: new Date().toISOString(),
    };
  }

  @Post('ops/drain')
  @HttpCode(HttpStatus.OK)
  async drainPending(): Promise<{ processed: number; stats: PublisherStats }> {
    const processed = await this.publisherService.drainBatch();
    const stats = this.publisherService.getStats();
    return {
      processed,
      stats,
    };
  }
}
