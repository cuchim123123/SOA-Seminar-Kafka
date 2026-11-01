import { Module } from '@nestjs/common';
import { MetricsModule } from '@lab/observability';
import { ConfigModule } from './config/config.module';
import { DatabaseModule } from './database/database.module';
import { HealthModule } from './health/health.module';
import { OpsModule } from './ops/ops.module';
import { PurchasesModule } from './purchases/purchases.module';

@Module({
  imports: [
    MetricsModule.forRoot({ serviceName: 'producer-api' }),
    ConfigModule,
    DatabaseModule,
    HealthModule,
    PurchasesModule,
    OpsModule,
  ],
})
export class AppModule {}
