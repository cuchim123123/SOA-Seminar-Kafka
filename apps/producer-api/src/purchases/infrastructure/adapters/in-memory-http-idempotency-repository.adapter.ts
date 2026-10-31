import { Injectable } from '@nestjs/common';
import {
  ClientIdempotencyRecord,
  ClientIdempotencyRepositoryPort,
} from '../../application/ports/client-idempotency-repository.port';

@Injectable()
export class InMemoryHttpIdempotencyRepositoryAdapter implements ClientIdempotencyRepositoryPort {
  private readonly records = new Map<string, ClientIdempotencyRecord>();

  private makeKey(customerId: string, idempotencyKey: string): string {
    return `${customerId}:${idempotencyKey}`;
  }

  async find(customerId: string, idempotencyKey: string): Promise<ClientIdempotencyRecord | null> {
    const key = this.makeKey(customerId, idempotencyKey);
    return this.records.get(key) ?? null;
  }

  async save(record: ClientIdempotencyRecord): Promise<void> {
    const key = this.makeKey(record.customerId, record.idempotencyKey);
    this.records.set(key, record);
  }

  clear(): void {
    this.records.clear();
  }
}
