import { Module } from '@nestjs/common';
import { ConfigModule } from './config/config.module';
import { DatabaseModule } from './database/database.module';
import { HealthModule } from './health/health.module';
import { ReplayModule } from './replay/replay.module';

@Module({
  imports: [ConfigModule, DatabaseModule, HealthModule, ReplayModule],
})
export class AppModule {}
