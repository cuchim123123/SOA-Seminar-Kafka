import { Inject, Injectable } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import { EventReceiptReadPort } from '../../application/ports/event-receipt-read.port';
@Injectable()
export class PostgresEventReceiptReadAdapter implements EventReceiptReadPort {
  constructor(@Inject(PrismaService) private readonly prisma: PrismaService) {}
  async find(eventIds: string[]) {
    // Must match the persisted idempotency identity used by this context's handler,
    // not the transport's configurable group ID or another context's receipts.
    const rows = await this.prisma.processedEvent.findMany({
      where: { consumerName: 'loyalty-v1', eventId: { in: eventIds } },
      select: { eventId: true, processedAt: true },
      orderBy: { eventId: 'asc' },
      take: eventIds.length,
    });
    return rows.map((row) => ({
      eventId: row.eventId,
      processedAt: row.processedAt.toISOString(),
    }));
  }
}
