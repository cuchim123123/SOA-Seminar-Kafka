import { Controller, Get } from '@nestjs/common';

export interface HealthResponse {
  status: 'ok';
  service: string;
  uptimeSeconds: number;
  timestamp: string;
}

@Controller('health')
export class HealthController {
  @Get('live')
  getLive(): HealthResponse {
    return {
      status: 'ok',
      service: 'loyalty-worker',
      uptimeSeconds: Math.floor(process.uptime()),
      timestamp: new Date().toISOString(),
    };
  }

  @Get('ready')
  getReady(): HealthResponse {
    return {
      status: 'ok',
      service: 'loyalty-worker',
      uptimeSeconds: Math.floor(process.uptime()),
      timestamp: new Date().toISOString(),
    };
  }
}
