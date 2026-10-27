import { z } from 'zod';
import { IntegrationEventEnvelopeSchemaV1, IntegrationEventEnvelopeV1 } from './envelope.v1';

export interface ValidationIssue {
  readonly path: string;
  readonly message: string;
}

export class ContractValidationError extends Error {
  public readonly issues: readonly ValidationIssue[];

  constructor(message: string, issues: readonly ValidationIssue[] = []) {
    super(message);
    this.name = 'ContractValidationError';
    this.issues = issues;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

function formatZodIssues(error: z.ZodError): readonly ValidationIssue[] {
  return error.issues.map((issue) => ({
    path: issue.path.join('.'),
    message: issue.message,
  }));
}

export function serializeEnvelope(envelope: IntegrationEventEnvelopeV1): string {
  const result = IntegrationEventEnvelopeSchemaV1.safeParse(envelope);
  if (!result.success) {
    const issues = formatZodIssues(result.error);
    throw new ContractValidationError(
      `Cannot serialize invalid IntegrationEventEnvelopeV1: ${issues.map((i) => `${i.path}: ${i.message}`).join('; ')}`,
      issues,
    );
  }
  return JSON.stringify(result.data);
}

export function deserializeEnvelope(raw: string | Buffer | unknown): IntegrationEventEnvelopeV1 {
  let parsedJson: unknown;

  if (typeof raw === 'string') {
    try {
      parsedJson = JSON.parse(raw);
    } catch (err: unknown) {
      throw new ContractValidationError(
        `Failed to parse raw event JSON: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  } else if (Buffer.isBuffer(raw)) {
    try {
      parsedJson = JSON.parse(raw.toString('utf-8'));
    } catch (err: unknown) {
      throw new ContractValidationError(
        `Failed to parse raw event buffer as JSON: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  } else {
    parsedJson = raw;
  }

  const result = IntegrationEventEnvelopeSchemaV1.safeParse(parsedJson);
  if (!result.success) {
    const issues = formatZodIssues(result.error);
    throw new ContractValidationError(
      `Invalid IntegrationEventEnvelopeV1: ${issues.map((i) => `${i.path}: ${i.message}`).join('; ')}`,
      issues,
    );
  }

  return result.data;
}

export function isIntegrationEventEnvelopeV1(value: unknown): value is IntegrationEventEnvelopeV1 {
  return IntegrationEventEnvelopeSchemaV1.safeParse(value).success;
}
