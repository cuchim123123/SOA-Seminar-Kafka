import { Inject, Injectable, Optional } from '@nestjs/common';
import { collectDefaultMetrics, Counter, Gauge, Histogram, Registry } from 'prom-client';
import {
  ConsumerState,
  METRIC_NAMES,
  METRICS_OPTIONS,
  MetricsModuleOptions,
} from './metrics.constants';

const ALL_CONSUMER_STATES: ConsumerState[] = [
  'idle',
  'polling',
  'processing',
  'paused',
  'rebalancing',
  'stopped',
];

@Injectable()
export class MetricsService {
  private readonly registry: Registry;
  private readonly serviceName: string;

  private sourceEventsCommittedTotal!: Counter<string>;
  private publishAttemptsTotal!: Counter<string>;
  private publishConfirmationsTotal!: Counter<string>;
  private confirmedUniqueEventsTotal!: Counter<string>;
  private confirmedPayloadBytesTotal!: Counter<string>;
  private deliveriesTotal!: Counter<string>;
  private handlerCommitsTotal!: Counter<string>;
  private duplicateDeliveriesTotal!: Counter<string>;
  private ackOperationsTotal!: Counter<string>;
  private processedUniqueEventsTotal!: Counter<string>;
  private processingFailuresTotal!: Counter<string>;
  private publishConfirmationDurationSeconds!: Histogram<string>;
  private deliveryToAckDurationSeconds!: Histogram<string>;
  private handlerDurationSeconds!: Histogram<string>;
  private sourceToHandlerDurationSeconds!: Histogram<string>;
  private httpRequestDurationSeconds!: Histogram<string>;
  private consumerState!: Gauge<string>;
  private replayRunsTotal!: Counter<string>;
  private replayDurationSeconds!: Histogram<string>;

  constructor(@Optional() @Inject(METRICS_OPTIONS) options?: MetricsModuleOptions) {
    this.serviceName = options?.serviceName ?? 'unknown-service';
    this.registry = new Registry();

    if (options?.defaultMetrics !== false) {
      collectDefaultMetrics({ register: this.registry });
    }

    this.initMetrics();
  }

  private initMetrics(): void {
    this.sourceEventsCommittedTotal = new Counter({
      name: METRIC_NAMES.SOURCE_EVENTS_COMMITTED_TOTAL,
      help: 'Total count of source events successfully committed to the database outbox',
      labelNames: ['service', 'event_type', 'mode'],
      registers: [this.registry],
    });

    this.publishAttemptsTotal = new Counter({
      name: METRIC_NAMES.PUBLISH_ATTEMPTS_TOTAL,
      help: 'Total count of transport publish attempts including retries',
      labelNames: ['service', 'transport', 'mode'],
      registers: [this.registry],
    });

    this.publishConfirmationsTotal = new Counter({
      name: METRIC_NAMES.PUBLISH_CONFIRMATIONS_TOTAL,
      help: 'Total count of observed transport publish confirmations',
      labelNames: ['service', 'transport', 'mode', 'status'],
      registers: [this.registry],
    });

    this.confirmedUniqueEventsTotal = new Counter({
      name: METRIC_NAMES.CONFIRMED_UNIQUE_EVENTS_TOTAL,
      help: 'Total count of unique events confirmed by transport',
      labelNames: ['service', 'transport', 'mode'],
      registers: [this.registry],
    });

    this.confirmedPayloadBytesTotal = new Counter({
      name: METRIC_NAMES.CONFIRMED_PAYLOAD_BYTES_TOTAL,
      help: 'Total canonical payload bytes of confirmed unique events',
      labelNames: ['service', 'transport', 'mode'],
      registers: [this.registry],
    });

    this.deliveriesTotal = new Counter({
      name: METRIC_NAMES.DELIVERIES_TOTAL,
      help: 'Total count of event deliveries received by consumers, including redeliveries',
      labelNames: ['service', 'consumer_role', 'transport', 'event_type'],
      registers: [this.registry],
    });

    this.handlerCommitsTotal = new Counter({
      name: METRIC_NAMES.HANDLER_COMMITS_TOTAL,
      help: 'Total count of successful business effect handler transactions committed',
      labelNames: ['service', 'consumer_role', 'event_type'],
      registers: [this.registry],
    });

    this.duplicateDeliveriesTotal = new Counter({
      name: METRIC_NAMES.DUPLICATE_DELIVERIES_TOTAL,
      help: 'Total count of duplicate event deliveries detected and deduplicated',
      labelNames: ['service', 'consumer_role', 'event_type'],
      registers: [this.registry],
    });

    this.ackOperationsTotal = new Counter({
      name: METRIC_NAMES.ACK_OPERATIONS_TOTAL,
      help: 'Total count of offset commit or message acknowledgement operations',
      labelNames: ['service', 'consumer_role', 'transport', 'status'],
      registers: [this.registry],
    });

    this.processedUniqueEventsTotal = new Counter({
      name: METRIC_NAMES.PROCESSED_UNIQUE_EVENTS_TOTAL,
      help: 'Total count of unique events reconciled as processed and acknowledged',
      labelNames: ['service', 'consumer_role', 'transport'],
      registers: [this.registry],
    });

    this.processingFailuresTotal = new Counter({
      name: METRIC_NAMES.PROCESSING_FAILURES_TOTAL,
      help: 'Total count of event processing failures by finite reason',
      labelNames: ['service', 'consumer_role', 'reason'],
      registers: [this.registry],
    });

    this.publishConfirmationDurationSeconds = new Histogram({
      name: METRIC_NAMES.PUBLISH_CONFIRMATION_DURATION_SECONDS,
      help: 'Duration in seconds from transport publish invocation to confirmation',
      labelNames: ['service', 'transport', 'mode'],
      buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
      registers: [this.registry],
    });

    this.deliveryToAckDurationSeconds = new Histogram({
      name: METRIC_NAMES.DELIVERY_TO_ACK_DURATION_SECONDS,
      help: 'Duration in seconds from delivery receipt to acknowledgement',
      labelNames: ['service', 'consumer_role', 'transport'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
      registers: [this.registry],
    });

    this.handlerDurationSeconds = new Histogram({
      name: METRIC_NAMES.HANDLER_DURATION_SECONDS,
      help: 'Duration in seconds of handler business logic processing',
      labelNames: ['service', 'consumer_role', 'status'],
      buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
      registers: [this.registry],
    });

    this.sourceToHandlerDurationSeconds = new Histogram({
      name: METRIC_NAMES.SOURCE_TO_HANDLER_DURATION_SECONDS,
      help: 'Duration in seconds from source event creation to handler transaction commit',
      labelNames: ['service', 'consumer_role'],
      buckets: [0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60],
      registers: [this.registry],
    });

    this.httpRequestDurationSeconds = new Histogram({
      name: METRIC_NAMES.HTTP_REQUEST_DURATION_SECONDS,
      help: 'HTTP request duration in seconds by route template, method, and status class',
      labelNames: ['service', 'method', 'route', 'status_code'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
      registers: [this.registry],
    });

    this.consumerState = new Gauge({
      name: METRIC_NAMES.CONSUMER_STATE,
      help: 'Lifecycle state of consumer (1 for active state, 0 otherwise)',
      labelNames: ['service', 'consumer_role', 'state'],
      registers: [this.registry],
    });

    this.replayRunsTotal = new Counter({
      name: METRIC_NAMES.REPLAY_RUNS_TOTAL,
      help: 'Total count of replay execution runs by terminal outcome',
      labelNames: ['service', 'status'],
      registers: [this.registry],
    });

    this.replayDurationSeconds = new Histogram({
      name: METRIC_NAMES.REPLAY_DURATION_SECONDS,
      help: 'Duration in seconds of replay execution runs',
      labelNames: ['service', 'status'],
      buckets: [0.1, 0.5, 1, 2, 5, 10, 30, 60, 120],
      registers: [this.registry],
    });
  }

  public getRegistry(): Registry {
    return this.registry;
  }

  public getContentType(): string {
    return this.registry.contentType;
  }

  public async getMetrics(): Promise<string> {
    return this.registry.metrics();
  }

  public clear(): void {
    this.registry.clear();
  }

  public resetMetrics(): void {
    this.registry.resetMetrics();
  }

  // --- Strongly typed recording helpers ---

  public recordSourceEventCommitted(eventType: string, mode: string = 'outbox_cdc'): void {
    try {
      this.sourceEventsCommittedTotal.inc({
        service: this.serviceName,
        event_type: eventType,
        mode,
      });
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordPublishAttempt(transport: string, mode: string): void {
    try {
      this.publishAttemptsTotal.inc({
        service: this.serviceName,
        transport,
        mode,
      });
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordPublishConfirmation(
    transport: string,
    mode: string,
    status: 'success' | 'failure',
    durationSeconds?: number,
  ): void {
    try {
      this.publishConfirmationsTotal.inc({
        service: this.serviceName,
        transport,
        mode,
        status,
      });
      if (typeof durationSeconds === 'number' && durationSeconds >= 0) {
        this.publishConfirmationDurationSeconds.observe(
          { service: this.serviceName, transport, mode },
          durationSeconds,
        );
      }
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordConfirmedUniqueEvent(transport: string, mode: string, payloadBytes?: number): void {
    try {
      this.confirmedUniqueEventsTotal.inc({
        service: this.serviceName,
        transport,
        mode,
      });
      if (typeof payloadBytes === 'number' && payloadBytes > 0) {
        this.confirmedPayloadBytesTotal.inc(
          { service: this.serviceName, transport, mode },
          payloadBytes,
        );
      }
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordDelivery(consumerRole: string, transport: string, eventType: string): void {
    try {
      this.deliveriesTotal.inc({
        service: this.serviceName,
        consumer_role: consumerRole,
        transport,
        event_type: eventType,
      });
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordHandlerCommit(
    consumerRole: string,
    eventType: string,
    durationSeconds?: number,
  ): void {
    try {
      this.handlerCommitsTotal.inc({
        service: this.serviceName,
        consumer_role: consumerRole,
        event_type: eventType,
      });
      if (typeof durationSeconds === 'number' && durationSeconds >= 0) {
        this.handlerDurationSeconds.observe(
          { service: this.serviceName, consumer_role: consumerRole, status: 'success' },
          durationSeconds,
        );
      }
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordDuplicateDelivery(consumerRole: string, eventType: string): void {
    try {
      this.duplicateDeliveriesTotal.inc({
        service: this.serviceName,
        consumer_role: consumerRole,
        event_type: eventType,
      });
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordAck(
    consumerRole: string,
    transport: string,
    status: 'success' | 'failure',
    durationSeconds?: number,
  ): void {
    try {
      this.ackOperationsTotal.inc({
        service: this.serviceName,
        consumer_role: consumerRole,
        transport,
        status,
      });
      if (typeof durationSeconds === 'number' && durationSeconds >= 0) {
        this.deliveryToAckDurationSeconds.observe(
          { service: this.serviceName, consumer_role: consumerRole, transport },
          durationSeconds,
        );
      }
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordProcessedUniqueEvent(consumerRole: string, transport: string): void {
    try {
      this.processedUniqueEventsTotal.inc({
        service: this.serviceName,
        consumer_role: consumerRole,
        transport,
      });
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordProcessingFailure(
    consumerRole: string,
    reason: string,
    durationSeconds?: number,
  ): void {
    try {
      this.processingFailuresTotal.inc({
        service: this.serviceName,
        consumer_role: consumerRole,
        reason,
      });
      if (typeof durationSeconds === 'number' && durationSeconds >= 0) {
        this.handlerDurationSeconds.observe(
          { service: this.serviceName, consumer_role: consumerRole, status: 'failure' },
          durationSeconds,
        );
      }
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordSourceToHandlerDuration(consumerRole: string, durationSeconds: number): void {
    try {
      if (durationSeconds >= 0) {
        this.sourceToHandlerDurationSeconds.observe(
          { service: this.serviceName, consumer_role: consumerRole },
          durationSeconds,
        );
      }
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordHttpRequest(
    method: string,
    route: string,
    statusCode: number,
    durationSeconds: number,
  ): void {
    try {
      this.httpRequestDurationSeconds.observe(
        {
          service: this.serviceName,
          method,
          route,
          status_code: String(statusCode),
        },
        durationSeconds,
      );
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public setConsumerState(consumerRole: string, activeState: ConsumerState): void {
    try {
      for (const state of ALL_CONSUMER_STATES) {
        this.consumerState.set(
          {
            service: this.serviceName,
            consumer_role: consumerRole,
            state,
          },
          state === activeState ? 1 : 0,
        );
      }
    } catch {
      // Telemetry must never crash domain flow
    }
  }

  public recordReplayRun(status: 'completed' | 'failed', durationSeconds: number): void {
    try {
      this.replayRunsTotal.inc({
        service: this.serviceName,
        status,
      });
      if (durationSeconds >= 0) {
        this.replayDurationSeconds.observe({ service: this.serviceName, status }, durationSeconds);
      }
    } catch {
      // Telemetry must never crash domain flow
    }
  }
}
