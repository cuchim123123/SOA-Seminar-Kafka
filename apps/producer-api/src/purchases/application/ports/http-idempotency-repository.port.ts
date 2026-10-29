export interface HttpIdempotencyRecord {
  readonly customerId: string;
  readonly idempotencyKey: string;
  readonly requestFingerprint: string;
  readonly purchaseId: string;
}

export const HTTP_IDEMPOTENCY_REPOSITORY_PORT = Symbol('HTTP_IDEMPOTENCY_REPOSITORY_PORT');

export interface HttpIdempotencyRepositoryPort {
  find(customerId: string, idempotencyKey: string): Promise<HttpIdempotencyRecord | null>;
  save(record: HttpIdempotencyRecord): Promise<void>;
}
