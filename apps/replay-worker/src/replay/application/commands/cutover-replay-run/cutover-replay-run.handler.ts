import { BadRequestException, Inject, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../../../../database/prisma.service';
import { CutoverReplayRunCommand, CutoverReplayRunResult } from './cutover-replay-run.command';

@Injectable()
export class CutoverReplayRunHandler {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
  ) {}

  async execute(command: CutoverReplayRunCommand): Promise<CutoverReplayRunResult> {
    const { runId } = command;

    const run = await this.prisma.replayRun.findUnique({
      where: { id: runId },
    });

    if (!run) {
      throw new NotFoundException(`Replay run '${runId}' not found`);
    }

    if (run.status === 'CUTOVER_COMPLETED') {
      return {
        runId,
        status: 'CUTOVER_COMPLETED',
        appliedCustomersCount: 0,
        cutoverAt: run.updatedAt ? run.updatedAt.toISOString() : new Date().toISOString(),
      };
    }

    if (run.status !== 'COMPLETED') {
      throw new BadRequestException(
        `Cannot execute cutover for replay run '${runId}' with status '${run.status}'. Run must be COMPLETED.`,
      );
    }

    const stagedRecords = await this.prisma.replayStagingRecord.findMany({
      where: { runId },
    });

    if (stagedRecords.length === 0) {
      throw new BadRequestException(`No staging records found for replay run '${runId}'`);
    }

    return await this.prisma.$transaction(async (tx) => {
      let appliedCount = 0;
      let skippedCount = 0;
      for (const staged of stagedRecords) {
        const existing = await tx.loyaltyAccount.findUnique({
          where: { customerId: staged.customerId },
        });

        // Boundary guard: do not overwrite live state if customer has already progressed past replay snapshot
        if (existing && existing.lastCustomerSequence > BigInt(staged.lastCustomerSequence)) {
          skippedCount++;
          continue;
        }

        await tx.loyaltyAccount.upsert({
          where: { customerId: staged.customerId },
          create: {
            customerId: staged.customerId,
            points: staged.points,
            tier: staged.tier,
            eligibleSpendCents: staged.eligibleSpendCents,
            lastCustomerSequence: staged.lastCustomerSequence,
            correctionRequired: false,
          },
          update: {
            points: staged.points,
            tier: staged.tier,
            eligibleSpendCents: staged.eligibleSpendCents,
            lastCustomerSequence: staged.lastCustomerSequence,
            correctionRequired: false,
          },
        });
        appliedCount++;
      }

      if (appliedCount === 0) {
        throw new BadRequestException(
          `Cannot complete cutover for replay run '${runId}': all staged customer records (${skippedCount}) were skipped because live customer sequence has already advanced past the replay snapshot. Staged state is obsolete.`,
        );
      }

      await tx.replayRun.update({
        where: { id: runId },
        data: {
          status: 'CUTOVER_COMPLETED',
        },
      });

      return {
        runId,
        status: 'CUTOVER_COMPLETED',
        appliedCustomersCount: appliedCount,
        cutoverAt: new Date().toISOString(),
      };
    });
  }
}
