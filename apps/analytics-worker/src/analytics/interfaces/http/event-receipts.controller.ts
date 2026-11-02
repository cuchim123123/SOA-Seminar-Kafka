import { BadRequestException, Controller, Get, Inject, Query } from '@nestjs/common';
import { GetEventReceiptsHandler } from '../../application/queries/get-event-receipts.handler';
import { parseEventReceiptIds } from '@lab/contracts';
@Controller('api/v1/analytics/event-receipts')
export class EventReceiptsController {
  constructor(@Inject(GetEventReceiptsHandler) private readonly handler: GetEventReceiptsHandler) {}
  @Get()
  get(@Query('eventIds') raw: unknown) {
    const parsed = parseEventReceiptIds(raw);
    if (!parsed.success)
      throw new BadRequestException(
        'eventIds must contain 1 to 100 comma-separated event IDs (maximum 128 characters each)',
      );
    return this.handler.execute(parsed.data);
  }
}
