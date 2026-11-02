import { AppConfigService as ReplayPushConfig } from '../../../src/config/app-config.service';
import { validateEnv as validateReplayPushEnv } from '../../../src/config/env.validation';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { describe, expect, it, vi } from 'vitest';
import { CutoverReplayRunCommand } from '../../../src/replay/application/commands/cutover-replay-run/cutover-replay-run.command';
import { CutoverReplayRunHandler } from '../../../src/replay/application/commands/cutover-replay-run/cutover-replay-run.handler';
import { PrismaService } from '../../../src/database/prisma.service';

function transactional(prisma: PrismaService): PrismaService {
  const original = prisma.$transaction?.bind(prisma);
  const enrich = (tx: object) => ({
    ...tx,
    $executeRaw: vi.fn().mockResolvedValue(1),
    replayRun: { ...prisma.replayRun, ...(tx as { replayRun?: object }).replayRun },
    replayStagingRecord: prisma.replayStagingRecord,
  });
  return {
    ...prisma,
    $transaction: (work: (tx: object) => Promise<unknown>) =>
      original ? original((tx) => work(enrich(tx))) : work(enrich(prisma)),
  } as unknown as PrismaService;
}

describe('CutoverReplayRunHandler', () => {
  it('throws NotFoundException if run does not exist', async () => {
    const mockPrisma = {
      replayRun: {
        findUnique: vi.fn().mockResolvedValue(null),
      },
    } as unknown as PrismaService;

    const handler = new CutoverReplayRunHandler(
      transactional(mockPrisma),
      new ReplayPushConfig(validateReplayPushEnv({})),
    );
    await expect(handler.execute(new CutoverReplayRunCommand('non-existent'))).rejects.toThrow(
      NotFoundException,
    );
  });

  it('returns idempotent result if run is already CUTOVER_COMPLETED', async () => {
    const mockPrisma = {
      replayRun: {
        findUnique: vi.fn().mockResolvedValue({
          id: 'run-1',
          status: 'CUTOVER_COMPLETED',
          updatedAt: new Date('2026-10-31T12:00:00Z'),
        }),
      },
    } as unknown as PrismaService;

    const handler = new CutoverReplayRunHandler(
      transactional(mockPrisma),
      new ReplayPushConfig(validateReplayPushEnv({})),
    );
    const result = await handler.execute(new CutoverReplayRunCommand('run-1'));

    expect(result.status).toBe('CUTOVER_COMPLETED');
    expect(result.appliedCustomersCount).toBe(0);
  });

  it('throws BadRequestException if run is not in COMPLETED status', async () => {
    const mockPrisma = {
      replayRun: {
        findUnique: vi.fn().mockResolvedValue({
          id: 'run-1',
          status: 'RUNNING',
        }),
      },
    } as unknown as PrismaService;

    const handler = new CutoverReplayRunHandler(
      transactional(mockPrisma),
      new ReplayPushConfig(validateReplayPushEnv({})),
    );
    await expect(handler.execute(new CutoverReplayRunCommand('run-1'))).rejects.toThrow(
      BadRequestException,
    );
  });

  it('throws BadRequestException if all staged records are skipped due to advanced live state', async () => {
    const mockPrisma = {
      replayRun: {
        findUnique: vi.fn().mockResolvedValue({
          id: 'run-1',
          status: 'COMPLETED',
        }),
      },
      replayStagingRecord: {
        findMany: vi.fn().mockResolvedValue([
          {
            runId: 'run-1',
            customerId: 'CUST_001',
            points: 40n,
            tier: 'GOLD',
            eligibleSpendCents: 30000n,
            lastCustomerSequence: 3n,
          },
        ]),
      },
      $transaction: vi.fn(async (cb: (tx: unknown) => Promise<unknown>) => {
        const mockTx = {
          loyaltyAccount: {
            findUnique: vi.fn().mockResolvedValue({
              customerId: 'CUST_001',
              lastCustomerSequence: 5n, // Live is sequence 5, but staged is only sequence 3!
            }),
            upsert: vi.fn(),
          },
          replayRun: {
            update: vi.fn(),
          },
        };
        return cb(mockTx);
      }),
    } as unknown as PrismaService;

    const handler = new CutoverReplayRunHandler(
      transactional(mockPrisma),
      new ReplayPushConfig(validateReplayPushEnv({})),
    );
    await expect(handler.execute(new CutoverReplayRunCommand('run-1'))).rejects.toThrow(
      BadRequestException,
    );
  });

  it('applies cutover successfully when staged records are newer or equal', async () => {
    const upsertSpy = vi.fn();
    const updateRunSpy = vi.fn();

    const mockPrisma = {
      replayRun: {
        findUnique: vi.fn().mockResolvedValue({
          id: 'run-1',
          status: 'COMPLETED',
        }),
      },
      replayStagingRecord: {
        findMany: vi.fn().mockResolvedValue([
          {
            runId: 'run-1',
            customerId: 'CUST_001',
            points: 40n,
            tier: 'GOLD',
            eligibleSpendCents: 30000n,
            lastCustomerSequence: 4n,
          },
        ]),
      },
      $transaction: vi.fn(async (cb: (tx: unknown) => Promise<unknown>) => {
        const mockTx = {
          loyaltyAccount: {
            findUnique: vi.fn().mockResolvedValue({
              customerId: 'CUST_001',
              lastCustomerSequence: 4n,
            }),
            upsert: upsertSpy,
          },
          replayRun: {
            update: updateRunSpy,
          },
        };
        return cb(mockTx);
      }),
    } as unknown as PrismaService;

    const handler = new CutoverReplayRunHandler(
      transactional(mockPrisma),
      new ReplayPushConfig(validateReplayPushEnv({})),
    );
    const result = await handler.execute(new CutoverReplayRunCommand('run-1'));

    expect(result.status).toBe('CUTOVER_COMPLETED');
    expect(result.appliedCustomersCount).toBe(1);
    expect(upsertSpy).toHaveBeenCalledTimes(1);
    expect(updateRunSpy).toHaveBeenCalledWith({
      where: { id: 'run-1' },
      data: { status: 'CUTOVER_COMPLETED' },
    });
  });
});
