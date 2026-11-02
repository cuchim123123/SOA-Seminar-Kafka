import { Inject, Injectable } from '@nestjs/common';
import { EVENT_RECEIPT_READ_PORT, EventReceiptReadPort } from '../ports/event-receipt-read.port';
@Injectable()
export class GetEventReceiptsHandler {
  constructor(@Inject(EVENT_RECEIPT_READ_PORT) private readonly receipts: EventReceiptReadPort) {}
  async execute(eventIds: string[]) {
    return { receipts: await this.receipts.find(eventIds) };
  }
}
