import { Injectable } from '@nestjs/common';
import { EnvConfig } from './env.validation';

export interface IAppConfig {
  readonly nodeEnv: string;
  readonly port: number;
  readonly kafkaBrokers: string[];
  readonly kafkaClientId: string;
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
    return this.config.REPLAY_WORKER_PORT ?? this.config.PORT;
  }

  get kafkaBrokers(): string[] {
    return this.config.KAFKA_BROKERS.split(',')
      .map((b) => b.trim())
      .filter(Boolean);
  }

  get kafkaClientId(): string {
    return this.config.KAFKA_CLIENT_ID;
  }

  get purchaseTopic(): string {
    return this.config.PURCHASE_TOPIC;
  }

  get databaseUrl(): string {
    return this.config.LOYALTY_DATABASE_URL || this.config.DATABASE_URL;
  }
  get changeFeedOptions() {
    return {
      channel: this.config.UI_PUSH_REPLAY_CHANNEL,
      heartbeatMs: this.config.UI_PUSH_HEARTBEAT_MS,
      reconnectMs: this.config.UI_PUSH_RECONNECT_MS,
      coalesceMs: this.config.UI_PUSH_COALESCE_MS,
      maxClients: this.config.UI_PUSH_MAX_CLIENTS,
    };
  }
  get loyaltyChangesChannel(): string {
    return this.config.UI_PUSH_LOYALTY_CHANNEL;
  }
}
