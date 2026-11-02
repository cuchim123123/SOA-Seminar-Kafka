import { Controller, Get, Inject, Res } from '@nestjs/common';
import { PostgresChangeFeed } from '@lab/observability';
import { Response } from 'express';
@Controller('api/v1/analytics-changes')
export class AnalyticsChangesController {
  constructor(@Inject(PostgresChangeFeed) private readonly changes: PostgresChangeFeed) {}
  @Get()
  stream(@Res() response: Response): void {
    this.changes.attach(response);
  }
}
