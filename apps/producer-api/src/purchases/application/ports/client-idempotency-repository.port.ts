export interface ClientIdempotencyRecord {
  readonly customerId: string;
  readonly idempotencyKey: string;
  readonly requestFingerprint: string;
  readonly purchaseId: string;
}

export const CLIENT_IDEMPOTENCY_REPOSITORY_PORT = Symbol('CLIENT_IDEMPOTENCY_REPOSITORY_PORT');

export interface ClientIdempotencyRepositoryPort {
  find(customerId: string, idempotencyKey: string): Promise<ClientIdempotencyRecord | null>;
  save(record: ClientIdempotencyRecord): Promise<void>;
}
