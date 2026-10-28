export abstract class AppError extends Error {
  public readonly timestamp: string;
  public abstract readonly isOperational: boolean;
  public abstract readonly code: string;

  constructor(
    message: string,
    public readonly details?: Record<string, unknown>,
  ) {
    super(message);
    this.name = this.constructor.name;
    this.timestamp = new Date().toISOString();
    Error.captureStackTrace?.(this, this.constructor);
  }
}
