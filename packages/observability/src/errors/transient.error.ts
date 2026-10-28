import { AppError } from './app-error';

/**
 * Thrown when an infrastructure or network failure occurs that is expected to recover
 * upon retry (e.g. broker disconnection, database connection timeout, lock contention).
 * Kafka consumers MUST NOT commit offset when encountering a TransientError.
 */
export class TransientError extends AppError {
  public readonly isOperational = true;
  public readonly code = 'TRANSIENT_ERROR';

  constructor(
    message: string,
    public override readonly details?: Record<string, unknown>,
  ) {
    super(message, details);
  }
}
