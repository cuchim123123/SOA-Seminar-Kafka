import { Injectable } from '@nestjs/common';
import {
  HttpIdempotencyRecord,
  HttpIdempotencyRepositoryPort,
} from '../../application/ports/http-idempotency-repository.port';

@Injectable()
export class InMemoryHttpIdempotencyRepositoryAdapter implements HttpIdempotencyRepositoryPort {
  private readonly records = new Map<string, HttpIdempotencyRecord>();

  private makeKey(customerId: string, idempotencyKey: string): string {
    return `${customerId}:${idempotencyKey}`;
  }

  async find(customerId: string, idempotencyKey: string): Promise<HttpIdempotencyRecord | null> {
    const key = this.makeKey(customerId, idempotencyKey);
    return this.records.get(key) ?? null;
  }

  async save(record: HttpIdempotencyRecord): Promise<void> {
    const key = this.makeKey(record.customerId, record.idempotencyKey);
    this.records.set(key, record);
  }

  clear(): void {
    this.records.clear();
  }
}
