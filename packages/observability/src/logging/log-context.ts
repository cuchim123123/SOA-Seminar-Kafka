export interface LogContext {
  service?: string;
  context?: string;
  correlationId?: string;
  eventId?: string;
  customerId?: string;
  topic?: string;
  partition?: number;
  offset?: string;
  key?: string;
  durationMs?: number;
  statusCode?: number;
  error?: unknown;
  [key: string]: unknown;
}
