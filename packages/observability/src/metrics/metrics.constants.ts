export const METRICS_OPTIONS = 'METRICS_OPTIONS';

export interface MetricsModuleOptions {
  serviceName: string;
  defaultMetrics?: boolean;
}

export const METRIC_NAMES = {
  SOURCE_EVENTS_COMMITTED_TOTAL: 'lab_source_events_committed_total',
  PUBLISH_ATTEMPTS_TOTAL: 'lab_publish_attempts_total',
  PUBLISH_CONFIRMATIONS_TOTAL: 'lab_publish_confirmations_total',
  CONFIRMED_UNIQUE_EVENTS_TOTAL: 'lab_confirmed_unique_events_total',
  CONFIRMED_PAYLOAD_BYTES_TOTAL: 'lab_confirmed_payload_bytes_total',
  DELIVERIES_TOTAL: 'lab_deliveries_total',
  HANDLER_COMMITS_TOTAL: 'lab_handler_commits_total',
  DUPLICATE_DELIVERIES_TOTAL: 'lab_duplicate_deliveries_total',
  ACK_OPERATIONS_TOTAL: 'lab_ack_operations_total',
  PROCESSED_UNIQUE_EVENTS_TOTAL: 'lab_processed_unique_events_total',
  PROCESSING_FAILURES_TOTAL: 'lab_processing_failures_total',
  PUBLISH_CONFIRMATION_DURATION_SECONDS: 'lab_publish_confirmation_duration_seconds',
  DELIVERY_TO_ACK_DURATION_SECONDS: 'lab_delivery_to_ack_duration_seconds',
  HANDLER_DURATION_SECONDS: 'lab_handler_duration_seconds',
  SOURCE_TO_HANDLER_DURATION_SECONDS: 'lab_source_to_handler_duration_seconds',
  HTTP_REQUEST_DURATION_SECONDS: 'lab_http_request_duration_seconds',
  CONSUMER_STATE: 'lab_consumer_state',
  REPLAY_RUNS_TOTAL: 'lab_replay_runs_total',
  REPLAY_DURATION_SECONDS: 'lab_replay_duration_seconds',
} as const;

export type ConsumerState =
  'idle' | 'polling' | 'processing' | 'paused' | 'rebalancing' | 'stopped';
