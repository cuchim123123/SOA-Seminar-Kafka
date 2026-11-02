import { Controller, Get, Inject, Res } from '@nestjs/common';
import { PostgresChangeFeed } from '@lab/observability';
import { Response } from 'express';

// Separate route avoids the existing purchases/:purchaseId matcher.
@Controller('api/v1/purchase-changes')
export class PurchaseChangesController {
  constructor(@Inject(PostgresChangeFeed) private readonly changes: PostgresChangeFeed) {}
  @Get()
  stream(@Res() response: Response): void {
    this.changes.attach(response);
  }
}
