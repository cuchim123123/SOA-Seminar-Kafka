import 'reflect-metadata';
import { Test, TestingModule } from '@nestjs/testing';
import { describe, expect, it } from 'vitest';
import { AppModule } from '../src/app.module';
import { AppConfigService } from '../src/config/app-config.service';
import { RAW_ENV_TOKEN } from '../src/config/config.module';
import { validateEnv } from '../src/config/env.validation';
import { PrismaService } from '../src/database/prisma.service';
import { HealthController } from '../src/health/health.controller';
import { BOUNDED_REPLAY_SCANNER_PORT } from '../src/replay/application/ports/bounded-replay-scanner.port';
import { TOPIC_BOUNDARY_INSPECTOR_PORT } from '../src/replay/application/ports/topic-boundary-inspector.port';
import { ReplayController } from '../src/replay/interfaces/http/replay.controller';

describe('Replay Worker Smoke Tests', () => {
  describe('AppModule, HealthController, ReplayController, and Config DI', () => {
    it('compiles testing module and resolves controllers and AppConfigService', async () => {
      const mockPrisma = {
        $connect: () => Promise.resolve(),
        $disconnect: () => Promise.resolve(),
      };

      const moduleRef: TestingModule = await Test.createTestingModule({
        imports: [AppModule],
      })
        .overrideProvider(RAW_ENV_TOKEN)
        .useValue({
          NODE_ENV: 'test',
          PORT: '3003',
          REPLAY_WORKER_PORT: '3003',
        })
        .overrideProvider(PrismaService)
        .useValue(mockPrisma)
        .overrideProvider(TOPIC_BOUNDARY_INSPECTOR_PORT)
        .useValue({
          getTopicBoundaries: () =>
            Promise.resolve({
              topic: 'purchase.events',
              startOffsets: {},
              highWatermarks: {},
            }),
        })
        .overrideProvider(BOUNDED_REPLAY_SCANNER_PORT)
        .useValue({
          scanBoundedTopic: () =>
            Promise.resolve({
              scannedCount: 0,
              finalCommittedOffsets: {},
            }),
        })
        .compile();

      const healthController = moduleRef.get<HealthController>(HealthController);
      expect(healthController).toBeDefined();

      const replayController = moduleRef.get<ReplayController>(ReplayController);
      expect(replayController).toBeDefined();

      const configService = moduleRef.get<AppConfigService>(AppConfigService);
      expect(configService).toBeDefined();
      expect(configService.nodeEnv).toBe('test');
      expect(configService.port).toBe(3003);

      const health = healthController.check();
      expect(health.status).toBe('ok');
      expect(health.service).toBe('replay-worker');
    });
  });

  describe('Environment Validation Schema', () => {
    it('validates default environment configuration', () => {
      const config = validateEnv({});
      expect(config.NODE_ENV).toBe('development');
      expect(config.PORT).toBe(3004);
      expect(config.KAFKA_CLIENT_ID).toBe('replay-worker');
    });

    it('rejects invalid PORT', () => {
      expect(() =>
        validateEnv({
          PORT: 'invalid-port',
        }),
      ).toThrowError(/Environment validation failed/);
    });
  });
});
