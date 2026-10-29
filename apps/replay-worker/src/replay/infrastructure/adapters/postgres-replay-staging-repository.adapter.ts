import { Inject, Injectable } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import { ReplayStagingRepositoryPort } from '../../application/ports/replay-staging-repository.port';
import { ReplayStagingRecord } from '../../domain/replay-staging-record.entity';

@Injectable()
export class PostgresReplayStagingRepositoryAdapter implements ReplayStagingRepositoryPort {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
  ) {}

  async upsertRecord(record: ReplayStagingRecord): Promise<void> {
    await this.prisma.replayStagingRecord.upsert({
      where: {
        runId_customerId: {
          runId: record.runId,
          customerId: record.customerId,
        },
      },
      create: {
        id: record.id,
        runId: record.runId,
        customerId: record.customerId,
        points: record.points,
        tier: record.tier,
        eligibleSpendCents: record.eligibleSpendCents,
        scannedEventCount: record.scannedEventCount,
        lastCustomerSequence: record.lastCustomerSequence,
        createdAt: record.createdAt,
      },
      update: {
        points: record.points,
        tier: record.tier,
        eligibleSpendCents: record.eligibleSpendCents,
        scannedEventCount: record.scannedEventCount,
        lastCustomerSequence: record.lastCustomerSequence,
      },
    });
  }

  async findByRunId(runId: string): Promise<ReplayStagingRecord[]> {
    const raw = await this.prisma.replayStagingRecord.findMany({
      where: { runId },
      orderBy: { customerId: 'asc' },
    });

    return raw.map(
      (r) =>
        new ReplayStagingRecord({
          id: r.id,
          runId: r.runId,
          customerId: r.customerId,
          points: r.points,
          tier: r.tier,
          eligibleSpendCents: r.eligibleSpendCents,
          scannedEventCount: r.scannedEventCount,
          lastCustomerSequence: r.lastCustomerSequence,
          createdAt: r.createdAt,
        }),
    );
  }
}
