import { Module } from '@nestjs/common';
import { ConfigModule } from './config/config.module';
import { HealthModule } from './health/health.module';
import { OpsModule } from './ops/ops.module';
import { PurchasesModule } from './purchases/purchases.module';

@Module({
  imports: [ConfigModule, HealthModule, PurchasesModule, OpsModule],
})
export class AppModule {}
