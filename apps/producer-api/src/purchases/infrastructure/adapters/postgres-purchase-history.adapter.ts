import { BadRequestException, Inject, Injectable } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import {
  PurchaseHistoryPage,
  PurchaseHistoryPort,
  PurchaseHistoryQuery,
} from '../../application/ports/purchase-history.port';

@Injectable()
export class PostgresPurchaseHistoryAdapter implements PurchaseHistoryPort {
  constructor(@Inject(PrismaService) private readonly prisma: PrismaService) {}

  async list(query: PurchaseHistoryQuery): Promise<PurchaseHistoryPage> {
    if (query.cursor) {
      const anchor = await this.prisma.purchase.findUnique({
        where: { purchaseId: query.cursor },
        select: { customerId: true },
      });
      if (!anchor || (query.customerId && anchor.customerId !== query.customerId)) {
        throw new BadRequestException(
          'Purchase history cursor is missing or belongs to another customer',
        );
      }
    }
    // Prisma resolves the cursor using the stored row timestamp, avoiding precision
    // loss from converting PostgreSQL timestamps to a browser timestamp cursor.
    const rows = await this.prisma.purchase.findMany({
      where: {
        ...(query.customerId ? { customerId: query.customerId } : {}),
        ...(query.purchaseId ? { purchaseId: query.purchaseId } : {}),
      },
      orderBy: [{ createdAt: 'desc' }, { purchaseId: 'desc' }],
      take: query.limit + 1,
      ...(query.cursor ? { cursor: { purchaseId: query.cursor }, skip: 1 } : {}),
    });
    const page = rows.slice(0, query.limit);
    const events = page.length
      ? await this.prisma.outboxEvent.findMany({
          where: {
            eventType: 'PurchaseCompleted',
            OR: page.map((row) => ({
              aggregateId: row.customerId,
              customerSequence: row.completionSequence,
            })),
          },
          select: { aggregateId: true, customerSequence: true, eventId: true },
        })
      : [];
    const eventIds = new Map(
      events.map((event) => [
        JSON.stringify([event.aggregateId, event.customerSequence.toString()]),
        event.eventId,
      ]),
    );
    return {
      items: page.map((row) => ({
        purchaseId: row.purchaseId,
        customerId: row.customerId,
        amountCents: row.amountCents.toString(),
        status: row.status,
        createdAt: row.createdAt.toISOString(),
        completionSequence: row.completionSequence.toString(),
        completionEventId:
          eventIds.get(JSON.stringify([row.customerId, row.completionSequence.toString()])) ?? null,
        invalidationSequence: row.invalidationSequence?.toString() ?? null,
        invalidationEventId: row.invalidationEventId,
        invalidationReason: row.invalidationReason,
      })),
      nextCursor: rows.length > query.limit ? page[page.length - 1].purchaseId : null,
    };
  }
}
