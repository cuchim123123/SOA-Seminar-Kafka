import { Module } from '@nestjs/common';
import { MetricsModule } from '@lab/observability';
import { AnalyticsModule } from './analytics/analytics.module';
import { ConfigModule } from './config/config.module';
import { DatabaseModule } from './database/database.module';
import { HealthModule } from './health/health.module';

@Module({
  imports: [
    MetricsModule.forRoot({ serviceName: 'analytics-worker' }),
    ConfigModule,
    DatabaseModule,
    HealthModule,
    AnalyticsModule,
  ],
})
export class AppModule {}
