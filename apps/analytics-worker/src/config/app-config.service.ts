import { Injectable } from '@nestjs/common';
import { EnvConfig } from './env.validation';

export interface IAppConfig {
  readonly nodeEnv: string;
  readonly port: number;
  readonly kafkaBrokers: string[];
  readonly kafkaClientId: string;
  readonly kafkaGroupId: string;
  readonly purchaseTopic: string;
  readonly databaseUrl: string;
}

@Injectable()
export class AppConfigService implements IAppConfig {
  constructor(private readonly config: EnvConfig) {}

  get nodeEnv(): string {
    return this.config.NODE_ENV;
  }

  get port(): number {
    return this.config.PORT;
  }

  get kafkaBrokers(): string[] {
    return this.config.KAFKA_BROKERS.split(',')
      .map((b) => b.trim())
      .filter(Boolean);
  }

  get kafkaClientId(): string {
    return this.config.KAFKA_CLIENT_ID;
  }

  get kafkaGroupId(): string {
    return this.config.KAFKA_GROUP_ID;
  }

  get purchaseTopic(): string {
    return this.config.PURCHASE_TOPIC;
  }

  get databaseUrl(): string {
    return this.config.ANALYTICS_DATABASE_URL || this.config.DATABASE_URL;
  }
}
