import { Injectable, LoggerService, Optional } from '@nestjs/common';
import { LogContext } from './log-context';
import { LogFormatter } from './log-formatter';
import { LogLevel } from './log-level';

export interface StructuredLoggerOptions {
  serviceName?: string;
  format?: 'json' | 'pretty';
  minLevel?: LogLevel;
}

const LEVEL_WEIGHTS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

@Injectable()
export class StructuredLogger implements LoggerService {
  private readonly serviceName: string;
  private readonly format: 'json' | 'pretty';
  private readonly minLevel: LogLevel;
  private contextName: string;

  constructor(@Optional() context?: string, @Optional() options?: StructuredLoggerOptions) {
    this.contextName = context ?? 'Application';
    this.serviceName = options?.serviceName ?? 'kafka-lab';
    this.format = options?.format ?? 'pretty';
    this.minLevel = options?.minLevel ?? 'debug';
  }

  setContext(context: string): this {
    this.contextName = context;
    return this;
  }

  log(message: string, contextOrMeta?: string | LogContext): void {
    this.writeLog('info', message, contextOrMeta);
  }

  info(message: string, contextOrMeta?: string | LogContext): void {
    this.writeLog('info', message, contextOrMeta);
  }

  warn(message: string, contextOrMeta?: string | LogContext): void {
    this.writeLog('warn', message, contextOrMeta);
  }

  error(message: string, trace?: unknown, contextOrMeta?: string | LogContext): void {
    if (LEVEL_WEIGHTS['error'] < LEVEL_WEIGHTS[this.minLevel]) return;

    let meta: LogContext | undefined;
    let actualTrace: unknown = trace;

    if (typeof contextOrMeta === 'string') {
      meta = { context: contextOrMeta };
    } else if (contextOrMeta && typeof contextOrMeta === 'object') {
      meta = contextOrMeta;
    }

    if (trace instanceof Error || typeof trace === 'string') {
      actualTrace = trace;
    } else if (trace && typeof trace === 'object' && !meta) {
      meta = trace as LogContext;
      actualTrace = undefined;
    }

    const output =
      this.format === 'json'
        ? LogFormatter.formatJson(
            'error',
            message,
            this.serviceName,
            meta?.context ?? this.contextName,
            meta,
            actualTrace,
          )
        : LogFormatter.formatPretty(
            'error',
            message,
            this.serviceName,
            meta?.context ?? this.contextName,
            meta,
            actualTrace,
          );

    console.error(output);
  }

  debug(message: string, contextOrMeta?: string | LogContext): void {
    this.writeLog('debug', message, contextOrMeta);
  }

  private writeLog(level: LogLevel, message: string, contextOrMeta?: string | LogContext): void {
    if (LEVEL_WEIGHTS[level] < LEVEL_WEIGHTS[this.minLevel]) return;

    let meta: LogContext | undefined;
    if (typeof contextOrMeta === 'string') {
      meta = { context: contextOrMeta };
    } else if (contextOrMeta && typeof contextOrMeta === 'object') {
      meta = contextOrMeta;
    }

    const output =
      this.format === 'json'
        ? LogFormatter.formatJson(
            level,
            message,
            this.serviceName,
            meta?.context ?? this.contextName,
            meta,
          )
        : LogFormatter.formatPretty(
            level,
            message,
            this.serviceName,
            meta?.context ?? this.contextName,
            meta,
          );

    if (level === 'warn') {
      console.warn(output);
    } else {
      console.log(output);
    }
  }
}
