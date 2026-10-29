import { describe, expect, it, vi } from 'vitest';
import { PrismaClient } from '@prisma/client';
import { PostgresSequenceGeneratorAdapter } from '../../../src/purchases/infrastructure/adapters/postgres-sequence-generator.adapter';

describe('PostgresSequenceGeneratorAdapter', () => {
  it('returns sequence 1 when inserting a brand new customer', async () => {
    const mockClient = {
      $executeRawUnsafe: vi.fn(async () => 1),
      $queryRawUnsafe: vi.fn(),
    } as unknown as PrismaClient;

    const adapter = new PostgresSequenceGeneratorAdapter(mockClient);
    const seq = await adapter.nextSequence('U_NEW_CUSTOMER');

    expect(seq).toBe(1);
    expect(mockClient.$executeRawUnsafe).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO purchase_customers'),
      'U_NEW_CUSTOMER',
    );
    expect(mockClient.$queryRawUnsafe).not.toHaveBeenCalled();
  });

  it('atomically increments and returns sequence under row lock for existing customer', async () => {
    const mockClient = {
      $executeRawUnsafe: vi.fn(async () => 0), // Row already existed
      $queryRawUnsafe: vi.fn(async () => [{ assigned_sequence: 5n }]),
    } as unknown as PrismaClient;

    const adapter = new PostgresSequenceGeneratorAdapter(mockClient);
    const seq = await adapter.nextSequence('U_EXISTING_CUSTOMER');

    expect(seq).toBe(5);
    expect(mockClient.$executeRawUnsafe).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO purchase_customers'),
      'U_EXISTING_CUSTOMER',
    );
    expect(mockClient.$queryRawUnsafe).toHaveBeenCalledWith(
      expect.stringContaining('UPDATE purchase_customers'),
      'U_EXISTING_CUSTOMER',
    );
  });
});
