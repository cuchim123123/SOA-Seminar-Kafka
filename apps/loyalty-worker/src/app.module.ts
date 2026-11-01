import { Module } from '@nestjs/common';
import { MetricsModule } from '@lab/observability';
import { ConfigModule } from './config/config.module';
import { DatabaseModule } from './database/database.module';
import { HealthModule } from './health/health.module';
import { LoyaltyModule } from './loyalty/loyalty.module';

@Module({
  imports: [
    MetricsModule.forRoot({ serviceName: 'loyalty-worker' }),
    ConfigModule,
    DatabaseModule,
    HealthModule,
    LoyaltyModule,
  ],
})
export class AppModule {}
