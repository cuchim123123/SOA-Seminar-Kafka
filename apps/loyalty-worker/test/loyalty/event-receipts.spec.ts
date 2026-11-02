import { describe, expect, it, vi } from 'vitest';
import { PrismaService } from '../../src/database/prisma.service';
import { PostgresEventReceiptReadAdapter } from '../../src/loyalty/infrastructure/adapters/postgres-event-receipt-read.adapter';
import { GetEventReceiptsHandler } from '../../src/loyalty/application/queries/get-event-receipts.handler';
import { EventReceiptsController } from '../../src/loyalty/interfaces/http/event-receipts.controller';

describe('loyalty receipt evidence', () => {
  it('queries only its persisted consumer identity and returns actual receipts', async () => {
    const findMany = vi
      .fn()
      .mockResolvedValue([{ eventId: 'e1', processedAt: new Date('2026-11-02T00:00:00Z') }]);
    const adapter = new PostgresEventReceiptReadAdapter({
      processedEvent: { findMany },
    } as unknown as PrismaService);
    const controller = new EventReceiptsController(new GetEventReceiptsHandler(adapter));
    expect(await controller.get('e1,e2,e1')).toEqual({
      receipts: [{ eventId: 'e1', processedAt: '2026-11-02T00:00:00.000Z' }],
    });
    expect(findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { consumerName: 'loyalty-v1', eventId: { in: ['e1', 'e2'] } },
        take: 2,
      }),
    );
  });
  it('rejects invalid queries before storage and propagates unavailable storage', async () => {
    const find = vi.fn().mockRejectedValue(new Error('database unavailable'));
    const controller = new EventReceiptsController(new GetEventReceiptsHandler({ find }));
    expect(() => controller.get('')).toThrow('eventIds');
    expect(find).not.toHaveBeenCalled();
    await expect(controller.get('e1')).rejects.toThrow('database unavailable');
  });
});
