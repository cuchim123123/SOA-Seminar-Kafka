import { AppError } from './app-error';

export interface PoisonPillDetails {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly key?: string;
  readonly rawValuePreview?: string;
  readonly originalError?: string;
  [key: string]: unknown;
}

/**
 * Thrown when a Kafka message contains malformed JSON, breaks event envelope schema,
 * or cannot be decoded. This error is non-retriable for the message itself.
 */
export class PoisonPillError extends AppError {
  public readonly isOperational = true;
  public readonly code = 'POISON_PILL';

  constructor(
    message: string,
    public override readonly details?: PoisonPillDetails,
  ) {
    super(message, details);
  }
}
