import { describe, expect, it, vi } from 'vitest';
import { PrismaClient } from '@prisma/client';
import { PostgresSequenceGeneratorAdapter } from '../../../src/purchases/infrastructure/adapters/postgres-sequence-generator.adapter';

describe('PostgresSequenceGeneratorAdapter', () => {
  it('returns sequence 1 when inserting a brand new customer via atomic UPSERT RETURNING', async () => {
    const mockClient = {
      $queryRawUnsafe: vi.fn(async () => [{ assigned_sequence: 1n }]),
    } as unknown as PrismaClient;

    const adapter = new PostgresSequenceGeneratorAdapter(mockClient);
    const seq = await adapter.nextSequence('U_NEW_CUSTOMER');

    expect(seq).toBe(1);
    expect(mockClient.$queryRawUnsafe).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO purchase_customers'),
      'U_NEW_CUSTOMER',
    );
  });

  it('atomically increments and returns sequence under row lock for existing customer', async () => {
    const mockClient = {
      $queryRawUnsafe: vi.fn(async () => [{ assigned_sequence: 5n }]),
    } as unknown as PrismaClient;

    const adapter = new PostgresSequenceGeneratorAdapter(mockClient);
    const seq = await adapter.nextSequence('U_EXISTING_CUSTOMER');

    expect(seq).toBe(5);
    expect(mockClient.$queryRawUnsafe).toHaveBeenCalledWith(
      expect.stringContaining(
        'DO UPDATE SET next_event_sequence = purchase_customers.next_event_sequence + 1',
      ),
      'U_EXISTING_CUSTOMER',
    );
  });

  it('throws an error if sequence query returns empty result', async () => {
    const mockClient = {
      $queryRawUnsafe: vi.fn(async () => []),
    } as unknown as PrismaClient;

    const adapter = new PostgresSequenceGeneratorAdapter(mockClient);
    await expect(adapter.nextSequence('U_ERR')).rejects.toThrow(
      "Failed to allocate customer sequence for customer 'U_ERR'",
    );
  });
});
