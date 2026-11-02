import 'reflect-metadata';
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Prisma } from '@prisma/client-replay';
import { PrismaService } from '../../src/database/prisma.service';
import { AppConfigService } from '../../src/config/app-config.service';
import { validateEnv } from '../../src/config/env.validation';
import { CutoverReplayRunHandler } from '../../src/replay/application/commands/cutover-replay-run/cutover-replay-run.handler';
import { CutoverReplayRunCommand } from '../../src/replay/application/commands/cutover-replay-run/cutover-replay-run.command';
import { PostgresLoyaltyUnitOfWorkAdapter } from '../../../loyalty-worker/src/loyalty/infrastructure/adapters/postgres-loyalty-unit-of-work.adapter';
import { PrismaService as LoyaltyPrismaService } from '../../../loyalty-worker/src/database/prisma.service';
import { LoyaltyAccount } from '../../../loyalty-worker/src/loyalty/domain/loyalty-account.entity';

function gate() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

describe('coordinated cutover and live Loyalty PostgreSQL locking', () => {
  const prisma = new PrismaService(new AppConfigService(validateEnv()));
  const ownedCustomers: string[] = [];
  const ownedRuns: string[] = [];
  beforeAll(() => prisma.$connect());
  afterAll(async () => {
    // Only random IDs created by this suite; never reset shared application data.
    try {
      await prisma.replayStagingRecord.deleteMany({ where: { runId: { in: ownedRuns } } });
      await prisma.replayRun.deleteMany({ where: { id: { in: ownedRuns } } });
      await prisma.loyaltyAccount.deleteMany({ where: { customerId: { in: ownedCustomers } } });
    } finally {
      await prisma.$disconnect();
    }
  });

  async function fixture(existing = true) {
    const customerId = `real-ui-lock-${randomUUID()}`;
    const runId = randomUUID();
    ownedCustomers.push(customerId);
    ownedRuns.push(runId);
    if (existing)
      await prisma.loyaltyAccount.create({
        data: {
          customerId,
          points: 1n,
          eligibleSpendCents: 1000n,
          tier: 'BRONZE',
          lastCustomerSequence: 1n,
          correctionRequired: true,
        },
      });
    await prisma.replayRun.create({
      data: {
        id: runId,
        groupId: `replay-${runId}`,
        status: 'COMPLETED',
        startOffsets: {},
        highWatermarks: {},
      },
    });
    await prisma.replayStagingRecord.create({
      data: {
        id: randomUUID(),
        runId,
        customerId,
        points: 10n,
        eligibleSpendCents: 1000n,
        tier: 'BRONZE',
        lastCustomerSequence: 1,
      },
    });
    return { customerId, runId };
  }

  // Interpose only synchronization gates, retaining real Prisma transactions/SQL.
  function intercepted(onLock?: (key: unknown) => void, afterAccountRead?: () => Promise<void>) {
    return {
      $transaction: (work: (tx: Prisma.TransactionClient) => Promise<unknown>) =>
        prisma.$transaction(
          async (tx) => {
            const wrapped = new Proxy(tx, {
              get(target, property) {
                if (property === '$executeRaw')
                  return async (sql: TemplateStringsArray, ...values: unknown[]) => {
                    onLock?.(values[0]);
                    return target.$executeRaw(sql, ...values);
                  };
                if (property === 'loyaltyAccount' && afterAccountRead)
                  return new Proxy(target.loyaltyAccount, {
                    get(delegate, name) {
                      if (name === 'findUnique')
                        return async (args: Prisma.LoyaltyAccountFindUniqueArgs) => {
                          const row = await delegate.findUnique(args);
                          await afterAccountRead();
                          return row;
                        };
                      return Reflect.get(delegate, name);
                    },
                  });
                return Reflect.get(target, property);
              },
            });
            return work(wrapped);
          },
          { timeout: 10000 },
        ),
    } as unknown as PrismaService;
  }

  it('waits for a live update, then skips the now-obsolete staged record', async () => {
    const { customerId, runId } = await fixture();
    const liveRead = gate();
    const releaseLive = gate();
    const cutoverAttempt = gate();
    const live = new PostgresLoyaltyUnitOfWorkAdapter(prisma as unknown as LoyaltyPrismaService);
    const liveWork = live.execute(async (ctx) => {
      const account = await ctx.loyaltyRepository.findByCustomerId(customerId);
      liveRead.release();
      await releaseLive.promise;
      account!.applyPurchase(1000, 2);
      await ctx.loyaltyRepository.save(account!);
    });
    await liveRead.promise;
    const handler = new CutoverReplayRunHandler(
      intercepted((key) => {
        if (key === `loyalty-account:${customerId}`) cutoverAttempt.release();
      }),
    );
    const outcome = handler.execute(new CutoverReplayRunCommand(runId)).then(
      (value) => ({ value }),
      (error) => ({ error }),
    );
    try {
      await cutoverAttempt.promise;
    } finally {
      releaseLive.release();
    }
    await liveWork;
    expect(await outcome).toMatchObject({
      error: { message: expect.stringContaining('all staged customer records') },
    });
    expect(await prisma.loyaltyAccount.findUnique({ where: { customerId } })).toMatchObject({
      points: 2n,
      lastCustomerSequence: 2n,
    });
    expect(await prisma.replayRun.findUnique({ where: { id: runId } })).toMatchObject({
      status: 'COMPLETED',
    });
  }, 15000);

  it.each([true, false])(
    'live processing reads the promoted state after cutover (existing account: %s)',
    async (existing) => {
      const { customerId, runId } = await fixture(existing);
      const cutoverRead = gate();
      const releaseCutover = gate();
      const liveAttempt = gate();
      const handler = new CutoverReplayRunHandler(
        intercepted(undefined, async () => {
          cutoverRead.release();
          await releaseCutover.promise;
        }),
      );
      const cutover = handler.execute(new CutoverReplayRunCommand(runId));
      await cutoverRead.promise;
      const liveDb = intercepted((key) => {
        if (key === `loyalty-account:${customerId}`) liveAttempt.release();
      });
      const live = new PostgresLoyaltyUnitOfWorkAdapter(liveDb as unknown as LoyaltyPrismaService);
      const liveWork = live.execute(async (ctx) => {
        const account =
          (await ctx.loyaltyRepository.findByCustomerId(customerId)) ??
          LoyaltyAccount.create(customerId);
        account.applyPurchase(1000, 2);
        await ctx.loyaltyRepository.save(account);
      });
      try {
        await liveAttempt.promise;
      } finally {
        releaseCutover.release();
      }
      expect((await cutover).appliedCustomersCount).toBe(1);
      await liveWork;
      expect(await prisma.loyaltyAccount.findUnique({ where: { customerId } })).toMatchObject({
        points: 11n,
        lastCustomerSequence: 2n,
      });
    },
    15000,
  );

  it('serializes concurrent promotion requests for the same run', async () => {
    const { runId } = await fixture();
    const handler = new CutoverReplayRunHandler(prisma);
    const results = await Promise.all([
      handler.execute(new CutoverReplayRunCommand(runId)),
      handler.execute(new CutoverReplayRunCommand(runId)),
    ]);
    expect(results.map((result) => result.appliedCustomersCount).sort()).toEqual([0, 1]);
  });
});
