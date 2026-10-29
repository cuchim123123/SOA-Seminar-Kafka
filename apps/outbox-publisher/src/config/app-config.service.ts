import { Injectable } from '@nestjs/common';
import { EnvConfig, validateEnv } from './env.validation';

@Injectable()
export class AppConfigService {
  private readonly config: EnvConfig;

  constructor() {
    this.config = validateEnv();
  }

  get nodeEnv(): string {
    return this.config.NODE_ENV;
  }

  get port(): number {
    return this.config.PUBLISHER_PORT ?? this.config.PORT;
  }

  get kafkaBrokers(): string[] {
    return this.config.KAFKA_BROKERS.split(',').map((b) => b.trim());
  }

  get kafkaClientId(): string {
    return this.config.KAFKA_CLIENT_ID;
  }

  get purchaseTopic(): string {
    return this.config.PURCHASE_TOPIC;
  }

  get databaseUrl(): string {
    return this.config.PURCHASES_DATABASE_URL || this.config.DATABASE_URL;
  }

  get pollIntervalMs(): number {
    return this.config.POLL_INTERVAL_MS;
  }

  get batchSize(): number {
    return this.config.BATCH_SIZE;
  }

  get initialBackoffMs(): number {
    return this.config.INITIAL_BACKOFF_MS;
  }

  get maxBackoffMs(): number {
    return this.config.MAX_BACKOFF_MS;
  }

  get faultInjectionPoint(): string {
    return this.config.FAULT_INJECTION_POINT;
  }

  get faultTriggerEventId(): string | undefined {
    return this.config.FAULT_TRIGGER_EVENT_ID;
  }
}
