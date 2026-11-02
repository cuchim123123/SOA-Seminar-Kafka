import 'reflect-metadata';
import { randomUUID } from 'node:crypto';
import { Test } from '@nestjs/testing';
import { describe, expect, it } from 'vitest';
import { Client } from 'pg';
import { ConfigModule, RAW_ENV_TOKEN } from '../../src/config/config.module';
import { AppConfigService } from '../../src/config/app-config.service';
import { DatabaseModule } from '../../src/database/database.module';
import { PrismaService } from '../../src/database/prisma.service';
import { PurchasesModule } from '../../src/purchases/purchases.module';
import { PostgresUnitOfWorkAdapter } from '../../src/purchases/infrastructure/adapters/postgres-unit-of-work.adapter';

describe('PostgreSQL commit → SSE → authoritative HTTP history', () => {
  it('delivers committed changes, suppresses rollback and reconciles after reconnect', async () => {
    const channel = `test_ui_${randomUUID().replaceAll('-', '')}`;
    const module = await Test.createTestingModule({
      imports: [ConfigModule, DatabaseModule, PurchasesModule],
    })
      .overrideProvider(RAW_ENV_TOKEN)
      .useValue({ ...process.env, UI_PUSH_PURCHASE_CHANNEL: channel, UI_PUSH_COALESCE_MS: 0 })
      .compile();
    const app = module.createNestApplication();
    const abort = new AbortController();
    const frames: string[] = [];
    let readLoop: Promise<void> | undefined;
    let writer: Client | undefined;
    const waitFor = async (count: number) => {
      const deadline = Date.now() + 3000;
      while (frames.length < count && Date.now() < deadline)
        await new Promise((resolve) => setTimeout(resolve, 10));
      expect(frames.length).toBe(count);
    };
    try {
      await app.listen(0, '127.0.0.1');
      const base = await app.getUrl();
      const response = await fetch(`${base}/api/v1/purchase-changes`, { signal: abort.signal });
      expect(response.headers.get('content-type')).toContain('text/event-stream');
      const reader = response.body!.getReader();
      readLoop = (async () => {
        const decoder = new TextDecoder();
        let buffer = '';
        try {
          while (true) {
            const chunk = await reader.read();
            if (chunk.done) break;
            buffer += decoder.decode(chunk.value, { stream: true });
            let end: number;
            while ((end = buffer.indexOf('\n\n')) !== -1) {
              const frame = buffer.slice(0, end);
              buffer = buffer.slice(end + 2);
              if (frame.startsWith('event: resync-required')) frames.push(frame);
            }
          }
        } catch (error) {
          if (!abort.signal.aborted) throw error;
        }
      })();
      await waitFor(1);
      const config = app.get(AppConfigService);
      const prisma = app.get(PrismaService);
      // One explicitly synthetic purchase is retained with its append-only outbox
      // evidence, so cleanup never retracts a record already visible to CDC.
      const customerId = `real-ui-push-${randomUUID()}`;
      const created = await fetch(`${base}/api/v1/purchases`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ customerId, amountCents: 1000, idempotencyKey: randomUUID() }),
      });
      expect(created.status).toBe(202);
      const accepted = (await created.json()) as { purchaseId: string };
      await waitFor(2);
      const history = (await (
        await fetch(`${base}/api/v1/purchases?customerId=${encodeURIComponent(customerId)}`)
      ).json()) as { items: Array<{ purchaseId: string }> };
      expect(history.items.map((item) => item.purchaseId)).toEqual([accepted.purchaseId]);

      // A separate database session proves cross-process delivery and rollback silence.
      writer = new Client({ connectionString: config.databaseUrl });
      await writer.connect();
      await writer.query('BEGIN');
      await writer.query('SELECT pg_notify($1, $2)', [channel, '']);
      await writer.query('ROLLBACK');
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(frames).toHaveLength(2);
      await writer.query('SELECT pg_notify($1, $2)', [channel, '']);
      await waitFor(3);

      // The unit-of-work itself must not emit when work fails before commit.
      await expect(
        new PostgresUnitOfWorkAdapter(prisma, config).execute(async () => {
          throw new Error('rollback');
        }),
      ).rejects.toThrow('rollback');
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(frames).toHaveLength(3);
      abort.abort();
      await readLoop;
      const reconnectAbort = new AbortController();
      const reconnect = await fetch(`${base}/api/v1/purchase-changes`, {
        signal: reconnectAbort.signal,
      });
      const initial = await reconnect.body!.getReader().read();
      expect(new TextDecoder().decode(initial.value)).toContain('resync-required');
      reconnectAbort.abort();
    } finally {
      abort.abort();
      await readLoop?.catch(() => undefined);
      await writer?.end();
      await app.close();
    }
  }, 20000);
});
