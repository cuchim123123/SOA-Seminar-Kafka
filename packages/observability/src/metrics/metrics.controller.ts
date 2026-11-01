import { Controller, Get, Res } from '@nestjs/common';
import { Response } from 'express';
import { MetricsService } from './metrics.service';

@Controller()
export class MetricsController {
  constructor(private readonly metricsService: MetricsService) {}

  @Get('metrics')
  async getMetrics(@Res({ passthrough: true }) res: Response): Promise<string> {
    res.header('Content-Type', this.metricsService.getContentType());
    return this.metricsService.getMetrics();
  }
}
