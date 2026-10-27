import 'reflect-metadata';
import { Test, TestingModule } from '@nestjs/testing';
import { describe, expect, it } from 'vitest';
import { AppModule } from '../src/app.module';
import { HealthController } from '../src/health/health.controller';
import { AppConfigService } from '../src/config/app-config.service';
import { RAW_ENV_TOKEN } from '../src/config/config.module';
import { validateEnv } from '../src/config/env.validation';

describe('P0-01 Smoke Tests', () => {
  describe('AppModule, HealthController, and Config DI', () => {
    it('compiles testing module and resolves HealthController and AppConfigService', async () => {
      const moduleRef: TestingModule = await Test.createTestingModule({
        imports: [AppModule],
      })
        .overrideProvider(RAW_ENV_TOKEN)
        .useValue({ NODE_ENV: 'test', PORT: '4000' })
        .compile();

      const controller = moduleRef.get<HealthController>(HealthController);
      expect(controller).toBeDefined();

      const configService = moduleRef.get<AppConfigService>(AppConfigService);
      expect(configService).toBeDefined();
      expect(configService.nodeEnv).toBe('test');
      expect(configService.port).toBe(4000);

      const live = controller.getLive();
      expect(live.status).toBe('ok');
      expect(typeof live.uptimeSeconds).toBe('number');
      expect(live.uptimeSeconds).toBeGreaterThanOrEqual(0);
      expect(new Date(live.timestamp).getTime()).not.toBeNaN();

      const ready = controller.getReady();
      expect(ready.status).toBe('ok');
      expect(new Date(ready.timestamp).getTime()).not.toBeNaN();
    });
  });

  describe('Environment Validation Schema', () => {
    it('validates default environment configuration', () => {
      const config = validateEnv({});
      expect(config.NODE_ENV).toBe('development');
      expect(config.PORT).toBe(3000);
    });

    it('validates custom environment configuration', () => {
      const config = validateEnv({
        NODE_ENV: 'production',
        PORT: '8080',
      });
      expect(config.NODE_ENV).toBe('production');
      expect(config.PORT).toBe(8080);
    });

    it('rejects invalid PORT', () => {
      expect(() =>
        validateEnv({
          PORT: 'not-a-number',
        }),
      ).toThrowError(/Environment validation failed/);

      expect(() =>
        validateEnv({
          PORT: '70000',
        }),
      ).toThrowError(/Environment validation failed/);
    });

    it('rejects invalid NODE_ENV', () => {
      expect(() =>
        validateEnv({
          NODE_ENV: 'staging',
        }),
      ).toThrowError(/Environment validation failed/);
    });
  });
});
