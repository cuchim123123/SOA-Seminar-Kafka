import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { AppConfigService } from './config/app-config.service';

async function bootstrap(): Promise<void> {
  const logger = new Logger('LoyaltyWorkerBootstrap');
  const app = await NestFactory.create(AppModule);

  app.enableShutdownHooks();

  const config = app.get(AppConfigService);
  await app.listen(config.port);
  logger.log(`Loyalty Worker listening on port ${config.port} in ${config.nodeEnv} mode`);
}

bootstrap().catch((err: unknown) => {
  console.error('Fatal loyalty-worker bootstrap error:', err);
  process.exit(1);
});
