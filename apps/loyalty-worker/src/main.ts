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

  const structuredLogger = new StructuredLogger('LoyaltyWorker', {
    serviceName: 'loyalty-worker',
    format: config.nodeEnv === 'production' ? 'json' : 'pretty',
  });

  app.useLogger(structuredLogger);
  app.useGlobalFilters(new GlobalHttpExceptionFilter(structuredLogger));
  app.useGlobalInterceptors(new HttpLoggingInterceptor(structuredLogger));
  app.enableShutdownHooks();

  await app.listen(config.port);
  structuredLogger.info('Loyalty Worker started successfully', {
    port: config.port,
    environment: config.nodeEnv,
  });
}

bootstrap().catch((err: unknown) => {
  console.error('Fatal loyalty-worker bootstrap error:', err);
  process.exit(1);
});
