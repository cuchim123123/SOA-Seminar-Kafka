import { Module } from '@nestjs/common';
import { ConfigModule } from './config/config.module';
import { PublisherModule } from './publisher/publisher.module';

@Module({
  imports: [ConfigModule, PublisherModule],
})
export class AppModule {}
