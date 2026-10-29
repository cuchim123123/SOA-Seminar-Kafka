import { Module } from '@nestjs/common';
import { ConfigModule } from './config/config.module';
import { DatabaseModule } from './database/database.module';
import { HealthModule } from './health/health.module';
import { LoyaltyModule } from './loyalty/loyalty.module';

@Module({
  imports: [ConfigModule, DatabaseModule, HealthModule, LoyaltyModule],
})
export class AppModule {}
