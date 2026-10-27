import { Controller, Get } from '@nestjs/common';

export interface HealthResponse {
  status: 'ok';
  uptimeSeconds: number;
  timestamp: string;
}

@Controller('health')
export class HealthController {
  @Get('live')
  getLive(): HealthResponse {
    return {
      status: 'ok',
      uptimeSeconds: Math.floor(process.uptime()),
      timestamp: new Date().toISOString(),
    };
  }

  @Get('ready')
  getReady(): HealthResponse {
    return {
      status: 'ok',
      uptimeSeconds: Math.floor(process.uptime()),
      timestamp: new Date().toISOString(),
    };
  }
}
