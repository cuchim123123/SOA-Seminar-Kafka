import 'reflect-metadata';
import {
  GlobalHttpExceptionFilter,
  HttpLoggingInterceptor,
  StructuredLogger,
} from '@lab/observability';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { AppConfigService } from './config/app-config.service';

if (typeof process.loadEnvFile === 'function') {
  try {
    process.loadEnvFile('.env');
  } catch {
    try {
      process.loadEnvFile('../../.env');
    } catch {
      // .env fallback ignored
    }
  }
}

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  const config = app.get(AppConfigService);

  const structuredLogger = new StructuredLogger('ReplayWorker', {
    serviceName: 'replay-worker',
    format: config.nodeEnv === 'production' ? 'json' : 'pretty',
  });

  app.useLogger(structuredLogger);
  app.useGlobalFilters(new GlobalHttpExceptionFilter(structuredLogger));
  app.useGlobalInterceptors(new HttpLoggingInterceptor(structuredLogger));
  app.enableShutdownHooks();

  await app.listen(config.port);
  structuredLogger.info('Replay Worker started successfully', {
    port: config.port,
    environment: config.nodeEnv,
  });
}

bootstrap().catch((err: unknown) => {
  console.error('Fatal replay-worker bootstrap error:', err);
  process.exit(1);
});
