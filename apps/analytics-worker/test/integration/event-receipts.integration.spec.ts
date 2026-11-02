import { ConfigModule } from '../../src/config/config.module';
import 'reflect-metadata';
import { Test } from '@nestjs/testing';
import { describe, expect, it } from 'vitest';
import { DatabaseModule } from '../../src/database/database.module';
import { PrismaService } from '../../src/database/prisma.service';
import { EVENT_RECEIPT_READ_PORT } from '../../src/analytics/application/ports/event-receipt-read.port';
import { GetEventReceiptsHandler } from '../../src/analytics/application/queries/get-event-receipts.handler';
import { PostgresEventReceiptReadAdapter } from '../../src/analytics/infrastructure/adapters/postgres-event-receipt-read.adapter';
import { EventReceiptsController } from '../../src/analytics/interfaces/http/event-receipts.controller';

describe('read-only analytics receipt HTTP integration', () => {
  it('serves only its persisted receipts without starting a consumer', async () => {
    const module = await Test.createTestingModule({
      imports: [ConfigModule, DatabaseModule],
      controllers: [EventReceiptsController],
      providers: [
        GetEventReceiptsHandler,
        { provide: EVENT_RECEIPT_READ_PORT, useClass: PostgresEventReceiptReadAdapter },
      ],
    }).compile();
    const app = module.createNestApplication();
    try {
      await app.listen(0, '127.0.0.1');
      const base = await app.getUrl();
      const row = await app
        .get(PrismaService)
        .processedEvent.findFirst({ where: { consumerName: 'analytics-v1' } });
      const id = row?.eventId ?? 'missing-real-ui-event';
      const response = await fetch(
        `${base}/api/v1/analytics/event-receipts?eventIds=${encodeURIComponent(id)}`,
      );
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({
        receipts: row ? [{ eventId: row.eventId, processedAt: row.processedAt.toISOString() }] : [],
      });
      expect((await fetch(`${base}/api/v1/analytics/event-receipts`)).status).toBe(400);
    } finally {
      await app.close();
    }
  }, 30000);
});
