import 'reflect-metadata';
import { randomUUID } from 'node:crypto';
import { Client } from 'pg';
import { describe, expect, it } from 'vitest';
import { PrismaService } from '../../src/database/prisma.service';
import { AppConfigService } from '../../src/config/app-config.service';
import { validateEnv } from '../../src/config/env.validation';
import { PostgresReplayRunRepositoryAdapter } from '../../src/replay/infrastructure/adapters/postgres-replay-run-repository.adapter';
import { PostgresReplayStagingRepositoryAdapter } from '../../src/replay/infrastructure/adapters/postgres-replay-staging-repository.adapter';
import { ReplayRun } from '../../src/replay/domain/replay-run.entity';
import { ReplayStagingRecord } from '../../src/replay/domain/replay-staging-record.entity';
import { CutoverReplayRunHandler } from '../../src/replay/application/commands/cutover-replay-run/cutover-replay-run.handler';
import { CutoverReplayRunCommand } from '../../src/replay/application/commands/cutover-replay-run/cutover-replay-run.command';

describe('cross-process replay and Loyalty notifications', () => {
  it('notifies persisted run/staging changes and both projections after cutover commit', async () => {
    const suffix = randomUUID().replaceAll('-', '');
    const config = new AppConfigService(
      validateEnv({
        ...process.env,
        UI_PUSH_REPLAY_CHANNEL: `test_replay_${suffix}`,
        UI_PUSH_LOYALTY_CHANNEL: `test_loyalty_${suffix}`,
      }),
    );
    const prisma = new PrismaService(config);
    const listener = new Client({ connectionString: config.databaseUrl });
    const channels: string[] = [];
    const runId = randomUUID();
    const customerId = `real-ui-replay-push-${suffix}`;
    const wait = async (count: number) => {
      const deadline = Date.now() + 2000;
      while (channels.length < count && Date.now() < deadline)
        await new Promise((resolve) => setTimeout(resolve, 10));
      expect(channels).toHaveLength(count);
    };
    try {
      await prisma.$connect();
      await listener.connect();
      listener.on('notification', (event) => channels.push(event.channel));
      await listener.query(`LISTEN "${config.changeFeedOptions.channel}"`);
      await listener.query(`LISTEN "${config.loyaltyChangesChannel}"`);
      const run = ReplayRun.create({
        id: runId,
        groupId: `replay-${runId}`,
        topic: config.purchaseTopic,
        startOffsets: {},
        highWatermarks: {},
      });
      const runs = new PostgresReplayRunRepositoryAdapter(prisma, config);
      await runs.save(run);
      await wait(1);
      const staging = new PostgresReplayStagingRepositoryAdapter(prisma, config);
      await staging.upsertRecord(
        new ReplayStagingRecord({
          id: randomUUID(),
          runId,
          customerId,
          points: 1n,
          tier: 'BRONZE',
          eligibleSpendCents: 1000n,
          scannedEventCount: 1,
          lastCustomerSequence: 1n,
          createdAt: new Date(),
        }),
      );
      await wait(2);
      run.start();
      run.complete();
      await runs.save(run);
      await wait(3);
      await new CutoverReplayRunHandler(prisma, config).execute(new CutoverReplayRunCommand(runId));
      await wait(5);
      expect(channels.slice(3).sort()).toEqual(
        [config.changeFeedOptions.channel, config.loyaltyChangesChannel].sort(),
      );
      expect(await prisma.loyaltyAccount.findUnique({ where: { customerId } })).toMatchObject({
        points: 1n,
        correctionRequired: false,
      });
    } finally {
      await listener.end();
      await prisma.replayStagingRecord.deleteMany({ where: { runId } });
      await prisma.replayRun.deleteMany({ where: { id: runId } });
      await prisma.loyaltyAccount.deleteMany({ where: { customerId } });
      await prisma.$disconnect();
    }
  }, 15000);
});
