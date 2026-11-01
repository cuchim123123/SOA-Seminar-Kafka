import { Module } from '@nestjs/common';
import { MetricsModule } from '@lab/observability';
import { ConfigModule } from './config/config.module';
import { DatabaseModule } from './database/database.module';
import { HealthModule } from './health/health.module';
import { ReplayModule } from './replay/replay.module';

@Module({
  imports: [
    MetricsModule.forRoot({ serviceName: 'replay-worker' }),
    ConfigModule,
    DatabaseModule,
    HealthModule,
    ReplayModule,
  ],
})
export class AppModule {}
