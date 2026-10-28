import { AppError } from './app-error';

/**
 * Thrown when business invariants or domain rules are violated.
 */
export class DomainError extends AppError {
  public readonly isOperational = true;
  public readonly code = 'DOMAIN_RULE_VIOLATION';

  constructor(
    message: string,
    public override readonly details?: Record<string, unknown>,
  ) {
    super(message, details);
  }
}
