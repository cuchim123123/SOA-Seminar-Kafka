import { LogContext } from './log-context';
import { LogLevel } from './log-level';

export interface FormattedLog {
  timestamp: string;
  level: LogLevel;
  service: string;
  context: string;
  message: string;
  correlationId?: string;
  eventId?: string;
  metadata?: Record<string, unknown>;
  error?: {
    name: string;
    message: string;
    stack?: string;
    code?: string;
    details?: unknown;
  };
}

export class LogFormatter {
  static formatJson(
    level: LogLevel,
    message: string,
    service: string,
    contextName: string,
    logContext?: LogContext,
    err?: unknown,
  ): string {
    const entry = this.buildEntry(level, message, service, contextName, logContext, err);
    return JSON.stringify(entry);
  }

  static formatPretty(
    level: LogLevel,
    message: string,
    service: string,
    contextName: string,
    logContext?: LogContext,
    err?: unknown,
  ): string {
    const entry = this.buildEntry(level, message, service, contextName, logContext, err);
    const levelUpper = entry.level.toUpperCase().padEnd(5);
    const metaStr =
      entry.metadata && Object.keys(entry.metadata).length > 0
        ? ` ${JSON.stringify(entry.metadata)}`
        : '';
    const errStr = entry.error
      ? ` | Error: ${entry.error.name}: ${entry.error.message}${entry.error.stack ? `\n${entry.error.stack}` : ''}`
      : '';

    return `[${entry.timestamp}] [${levelUpper}] [${entry.service}] [${entry.context}] ${entry.message}${metaStr}${errStr}`;
  }

  private static buildEntry(
    level: LogLevel,
    message: string,
    service: string,
    contextName: string,
    logContext?: LogContext,
    err?: unknown,
  ): FormattedLog {
    const { correlationId, eventId, ...rest } = logContext ?? {};

    const errorObj = this.serializeError(err ?? rest.error);
    delete rest.error;
    delete rest.service;
    delete rest.context;

    return {
      timestamp: new Date().toISOString(),
      level,
      service: logContext?.service ?? service,
      context: logContext?.context ?? contextName,
      message,
      ...(correlationId ? { correlationId } : {}),
      ...(eventId ? { eventId } : {}),
      ...(Object.keys(rest).length > 0 ? { metadata: rest } : {}),
      ...(errorObj ? { error: errorObj } : {}),
    };
  }

  private static serializeError(err: unknown): FormattedLog['error'] | undefined {
    if (!err) return undefined;
    if (err instanceof Error) {
      const details = 'details' in err ? (err as { details?: unknown }).details : undefined;
      const code = 'code' in err ? String((err as { code?: unknown }).code) : undefined;
      return {
        name: err.name,
        message: err.message,
        stack: err.stack,
        ...(code ? { code } : {}),
        ...(details ? { details } : {}),
      };
    }
    return {
      name: 'Error',
      message: String(err),
    };
  }
}
