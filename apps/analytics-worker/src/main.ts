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

  const structuredLogger = new StructuredLogger('AnalyticsWorker', {
    serviceName: 'analytics-worker',
    format: config.nodeEnv === 'production' ? 'json' : 'pretty',
  });

  app.useLogger(structuredLogger);
  app.useGlobalFilters(new GlobalHttpExceptionFilter(structuredLogger));
  app.useGlobalInterceptors(new HttpLoggingInterceptor(structuredLogger));
  app.enableShutdownHooks();
  app.enableCors();

  await app.listen(config.port);
  structuredLogger.info('Analytics Worker started successfully', {
    port: config.port,
    environment: config.nodeEnv,
    groupId: config.kafkaGroupId,
  });
}

bootstrap().catch((err: unknown) => {
  console.error('Fatal analytics-worker bootstrap error:', err);
  process.exit(1);
});
