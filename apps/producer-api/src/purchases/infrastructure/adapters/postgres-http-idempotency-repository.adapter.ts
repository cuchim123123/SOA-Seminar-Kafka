import { Inject, Injectable } from '@nestjs/common';
import { Prisma, PrismaClient } from '@prisma/client';
import { PrismaService } from '../../../database/prisma.service';
import {
  HttpIdempotencyRecord,
  HttpIdempotencyRepositoryPort,
} from '../../application/ports/http-idempotency-repository.port';

@Injectable()
export class PostgresHttpIdempotencyRepositoryAdapter implements HttpIdempotencyRepositoryPort {
  constructor(
    @Inject(PrismaService)
    private readonly client: Prisma.TransactionClient | PrismaClient,
  ) {}

  async find(customerId: string, idempotencyKey: string): Promise<HttpIdempotencyRecord | null> {
    const row = await this.client.httpIdempotencyKey.findUnique({
      where: {
        customerId_idempotencyKey: {
          customerId,
          idempotencyKey,
        },
      },
    });

    if (!row) {
      return null;
    }

    return {
      customerId: row.customerId,
      idempotencyKey: row.idempotencyKey,
      requestFingerprint: row.requestFingerprint,
      purchaseId: row.purchaseId,
    };
  }

  async save(record: HttpIdempotencyRecord): Promise<void> {
    await this.client.httpIdempotencyKey.create({
      data: {
        customerId: record.customerId,
        idempotencyKey: record.idempotencyKey,
        requestFingerprint: record.requestFingerprint,
        purchaseId: record.purchaseId,
      },
    });
  }
}
