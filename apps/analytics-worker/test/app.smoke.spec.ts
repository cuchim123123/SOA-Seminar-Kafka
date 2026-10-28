import 'reflect-metadata';
import { Test, TestingModule } from '@nestjs/testing';
import { describe, expect, it } from 'vitest';
import { AppModule } from '../src/app.module';
import { AppConfigService } from '../src/config/app-config.service';
import { RAW_ENV_TOKEN } from '../src/config/config.module';
import { validateEnv } from '../src/config/env.validation';
import { HealthController } from '../src/health/health.controller';

describe('Analytics Worker Smoke Tests', () => {
  describe('AppModule, HealthController, and Config DI', () => {
    it('compiles testing module and resolves HealthController and AppConfigService', async () => {
      const moduleRef: TestingModule = await Test.createTestingModule({
        imports: [AppModule],
      })
        .overrideProvider(RAW_ENV_TOKEN)
        .useValue({
          NODE_ENV: 'test',
          PORT: '4002',
          KAFKA_GROUP_ID: 'test-analytics',
        })
        .compile();

      const controller = moduleRef.get<HealthController>(HealthController);
      expect(controller).toBeDefined();

      const configService = moduleRef.get<AppConfigService>(AppConfigService);
      expect(configService).toBeDefined();
      expect(configService.nodeEnv).toBe('test');
      expect(configService.port).toBe(4002);
      expect(configService.kafkaGroupId).toBe('test-analytics');

      const live = controller.getLive();
      expect(live.status).toBe('ok');
      expect(live.service).toBe('analytics-worker');

      const ready = controller.getReady();
      expect(ready.status).toBe('ok');
      expect(ready.service).toBe('analytics-worker');
    });
  });

  describe('Environment Validation Schema', () => {
    it('validates default environment configuration', () => {
      const config = validateEnv({});
      expect(config.NODE_ENV).toBe('development');
      expect(config.PORT).toBe(3002);
      expect(config.KAFKA_GROUP_ID).toBe('analytics-v1');
      expect(config.KAFKA_CLIENT_ID).toBe('analytics-worker');
    });

    it('rejects invalid PORT', () => {
      expect(() =>
        validateEnv({
          PORT: 'not-a-number',
        }),
      ).toThrowError(/Environment validation failed/);
    });
  });
});
