import 'reflect-metadata';
import { Test } from '@nestjs/testing';
import { describe, expect, it } from 'vitest';
import { DatabaseModule } from '../../src/database/database.module';
import { PurchasesModule } from '../../src/purchases/purchases.module';

describe('read-only purchase history HTTP integration', () => {
  it('serves durable rows, validates queries and paginates without duplicate anchors', async () => {
    const module = await Test.createTestingModule({
      imports: [DatabaseModule, PurchasesModule],
    }).compile();
    const app = module.createNestApplication();
    try {
      await app.listen(0, '127.0.0.1');
      const base = await app.getUrl();
      const response = await fetch(`${base}/api/v1/purchases?limit=1`);
      expect(response.status).toBe(200);
      const page = await response.json();
      expect(Array.isArray(page.items)).toBe(true);
      if (page.items.length) {
        const item = page.items[0];
        expect(typeof item.completionSequence).toBe('string');
        const detail = await fetch(
          `${base}/api/v1/purchases/${encodeURIComponent(item.purchaseId)}`,
        );
        expect(detail.status).toBe(200);
        expect((await detail.json()).purchaseId).toBe(item.purchaseId);
        if (page.nextCursor) {
          const next = await (
            await fetch(
              `${base}/api/v1/purchases?limit=1&cursor=${encodeURIComponent(page.nextCursor)}`,
            )
          ).json();
          expect(
            next.items.every((row: { purchaseId: string }) => row.purchaseId !== item.purchaseId),
          ).toBe(true);
        }
      }
      expect((await fetch(`${base}/api/v1/purchases?limit=101`)).status).toBe(400);
      expect(
        (await fetch(`${base}/api/v1/purchases?cursor=nonexistent-real-ui-cursor`)).status,
      ).toBe(400);
    } finally {
      await app.close();
    }
  }, 30000);
});
