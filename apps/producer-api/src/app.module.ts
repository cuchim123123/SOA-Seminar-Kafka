import { Module } from '@nestjs/common';
import { ConfigModule } from './config/config.module';
import { HealthModule } from './health/health.module';
import { PurchasesModule } from './purchases/purchases.module';

@Module({
  imports: [ConfigModule, HealthModule, PurchasesModule],
})
export class AppModule {}
