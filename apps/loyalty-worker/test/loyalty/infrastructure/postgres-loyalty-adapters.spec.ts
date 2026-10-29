import { describe, expect, it, vi } from 'vitest';
import { PrismaClient } from '@prisma/client-loyalty';
import { PrismaService } from '../../../src/database/prisma.service';
import { LoyaltyAccount } from '../../../src/loyalty/domain/loyalty-account.entity';
import { PostgresConsumerIdempotencyRepositoryAdapter } from '../../../src/loyalty/infrastructure/adapters/postgres-consumer-idempotency-repository.adapter';
import { PostgresLoyaltyAccountReadDaoAdapter } from '../../../src/loyalty/infrastructure/adapters/postgres-loyalty-account-read-dao.adapter';
import { PostgresLoyaltyAccountRepositoryAdapter } from '../../../src/loyalty/infrastructure/adapters/postgres-loyalty-account-repository.adapter';
import { PostgresLoyaltyUnitOfWorkAdapter } from '../../../src/loyalty/infrastructure/adapters/postgres-loyalty-unit-of-work.adapter';

describe('Postgres Loyalty Adapters', () => {
  describe('PostgresLoyaltyAccountRepositoryAdapter', () => {
    it('findByCustomerId reconstitutes domain entity properly', async () => {
      const mockPrisma = {
        loyaltyAccount: {
          findUnique: vi.fn(async () => ({
            customerId: 'CUST_01',
            tier: 'SILVER',
            eligibleSpendCents: 15_000n,
            points: 15n,
            lastCustomerSequence: 2n,
            correctionRequired: false,
          })),
        },
      } as unknown as PrismaClient;

      const adapter = new PostgresLoyaltyAccountRepositoryAdapter(mockPrisma);
      const account = await adapter.findByCustomerId('CUST_01');

      expect(account).not.toBeNull();
      expect(account?.customerId).toBe('CUST_01');
      expect(account?.tier).toBe('SILVER');
      expect(account?.totalEligibleSpendCents).toBe(15_000);
      expect(account?.pointsBalance).toBe(15);
      expect(account?.lastCustomerSequence).toBe(2);
      expect(account?.correctionRequired).toBe(false);
    });

    it('findByCustomerId returns null when record not found', async () => {
      const mockPrisma = {
        loyaltyAccount: {
          findUnique: vi.fn(async () => null),
        },
      } as unknown as PrismaClient;

      const adapter = new PostgresLoyaltyAccountRepositoryAdapter(mockPrisma);
      const account = await adapter.findByCustomerId('UNKNOWN');
      expect(account).toBeNull();
    });

    it('save upserts loyalty account with bigint conversions', async () => {
      const mockPrisma = {
        loyaltyAccount: {
          upsert: vi.fn(async () => ({})),
        },
      } as unknown as PrismaClient;

      const adapter = new PostgresLoyaltyAccountRepositoryAdapter(mockPrisma);
      const account = LoyaltyAccount.create('CUST_02');
      account.applyPurchase(10_000, 1);

      await adapter.save(account);

      expect(mockPrisma.loyaltyAccount.upsert).toHaveBeenCalledWith({
        where: { customerId: 'CUST_02' },
        create: {
          customerId: 'CUST_02',
          eligibleSpendCents: 10_000n,
          points: 10n,
          tier: 'SILVER',
          lastCustomerSequence: 1n,
          correctionRequired: false,
        },
        update: {
          eligibleSpendCents: 10_000n,
          points: 10n,
          tier: 'SILVER',
          lastCustomerSequence: 1n,
          correctionRequired: false,
        },
      });
    });
  });

  describe('PostgresConsumerIdempotencyRepositoryAdapter', () => {
    it('returns true when insert succeeds (new event)', async () => {
      const mockPrisma = {
        $queryRaw: vi.fn(async () => [{ event_id: 'evt_new_1' }]),
      } as unknown as PrismaClient;

      const adapter = new PostgresConsumerIdempotencyRepositoryAdapter(mockPrisma);
      const isNew = await adapter.markProcessedIfNew('loyalty-v1', 'evt_new_1');

      expect(isNew).toBe(true);
      expect(mockPrisma.$queryRaw).toHaveBeenCalled();
    });

    it('returns false when insert results in conflict (duplicate event)', async () => {
      const mockPrisma = {
        $queryRaw: vi.fn(async () => []),
      } as unknown as PrismaClient;

      const adapter = new PostgresConsumerIdempotencyRepositoryAdapter(mockPrisma);
      const isNew = await adapter.markProcessedIfNew('loyalty-v1', 'evt_dup_1');

      expect(isNew).toBe(false);
    });
  });

  describe('PostgresLoyaltyAccountReadDaoAdapter', () => {
    it('returns projection DTO directly without aggregate reconstitution', async () => {
      const mockPrisma = {
        loyaltyAccount: {
          findUnique: vi.fn(async () => ({
            customerId: 'CUST_03',
            tier: 'GOLD',
            eligibleSpendCents: 45_000n,
            points: 90n,
          })),
        },
      } as unknown as PrismaService;

      const adapter = new PostgresLoyaltyAccountReadDaoAdapter(mockPrisma);
      const result = await adapter.findById('CUST_03');

      expect(result).toEqual({
        customerId: 'CUST_03',
        tier: 'GOLD',
        totalEligibleSpendCents: 45_000,
        pointsBalance: 90,
      });
    });
  });

  describe('PostgresLoyaltyUnitOfWorkAdapter', () => {
    it('wraps execution within $transaction', async () => {
      const mockTx = {
        loyaltyAccount: { findUnique: vi.fn() },
        $queryRaw: vi.fn(),
      };
      const mockPrisma = {
        $transaction: vi.fn(async (cb: (tx: unknown) => Promise<unknown>) => {
          return await cb(mockTx);
        }),
      } as unknown as PrismaService;

      const uow = new PostgresLoyaltyUnitOfWorkAdapter(mockPrisma);
      const output = await uow.execute(async (ctx) => {
        expect(ctx.loyaltyRepository).toBeDefined();
        expect(ctx.idempotencyRepository).toBeDefined();
        return 'success';
      });

      expect(output).toBe('success');
      expect(mockPrisma.$transaction).toHaveBeenCalled();
    });
  });
});
