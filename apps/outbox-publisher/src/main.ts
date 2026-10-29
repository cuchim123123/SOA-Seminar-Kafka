import 'reflect-metadata';
import {
  GlobalHttpExceptionFilter,
  HttpLoggingInterceptor,
  StructuredLogger,
} from '@lab/observability';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { AppConfigService } from './config/app-config.service';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  const config = app.get(AppConfigService);

  const structuredLogger = new StructuredLogger('OutboxPublisher', {
    serviceName: 'outbox-publisher',
    format: config.nodeEnv === 'production' ? 'json' : 'pretty',
  });

  app.useLogger(structuredLogger);
  app.useGlobalFilters(new GlobalHttpExceptionFilter(structuredLogger));
  app.useGlobalInterceptors(new HttpLoggingInterceptor(structuredLogger));
  app.enableShutdownHooks();

  await app.listen(config.port);
  structuredLogger.info('Outbox Publisher daemon service started successfully', {
    port: config.port,
    environment: config.nodeEnv,
    pollIntervalMs: config.pollIntervalMs,
    batchSize: config.batchSize,
  });
}

bootstrap().catch((err: unknown) => {
  console.error('Fatal outbox-publisher bootstrap error:', err);
  process.exit(1);
});
