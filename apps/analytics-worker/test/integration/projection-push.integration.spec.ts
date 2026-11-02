import 'reflect-metadata';
import { randomUUID } from 'node:crypto';
import { Test } from '@nestjs/testing';
import { PostgresChangeFeed } from '@lab/observability';
import { Client } from 'pg';
import { describe, expect, it } from 'vitest';
import { ConfigModule, RAW_ENV_TOKEN } from '../../src/config/config.module';
import { AppConfigService } from '../../src/config/app-config.service';
import { DatabaseModule } from '../../src/database/database.module';
import { PostgresAnalyticsUnitOfWorkAdapter } from '../../src/analytics/infrastructure/adapters/postgres-analytics-unit-of-work.adapter';
import { AnalyticsChangesController } from '../../src/analytics/interfaces/http/analytics-changes.controller';

describe('analytics transaction to HTTP SSE integration', () => {
  it('emits after a unit-of-work commit and closes on shutdown without starting consumers', async () => {
    const module = await Test.createTestingModule({
      imports: [ConfigModule, DatabaseModule],
      controllers: [AnalyticsChangesController],
      providers: [
        PostgresAnalyticsUnitOfWorkAdapter,
        {
          provide: PostgresChangeFeed,
          inject: [AppConfigService],
          useFactory: (config: AppConfigService) =>
            new PostgresChangeFeed(
              config.changeFeedOptions,
              () => new Client({ connectionString: config.databaseUrl }),
            ),
        },
      ],
    })
      .overrideProvider(RAW_ENV_TOKEN)
      .useValue({
        ...process.env,
        UI_PUSH_ANALYTICS_CHANNEL: `test_${randomUUID().replaceAll('-', '')}`,
        UI_PUSH_COALESCE_MS: 0,
      })
      .compile();
    const app = module.createNestApplication();
    const abort = new AbortController();
    try {
      await app.listen(0, '127.0.0.1');
      const response = await fetch(`${await app.getUrl()}/api/v1/analytics-changes`, {
        signal: abort.signal,
      });
      expect(response.status).toBe(200);
      const reader = response.body!.getReader();
      expect(new TextDecoder().decode((await reader.read()).value)).toContain('resync-required');
      await app.get(PostgresAnalyticsUnitOfWorkAdapter).execute(async () => undefined);
      const timer = setTimeout(() => abort.abort(), 3000);
      try {
        expect(new TextDecoder().decode((await reader.read()).value)).toContain('resync-required');
      } finally {
        clearTimeout(timer);
      }
    } finally {
      abort.abort();
      await app.close();
    }
  }, 15000);
});
