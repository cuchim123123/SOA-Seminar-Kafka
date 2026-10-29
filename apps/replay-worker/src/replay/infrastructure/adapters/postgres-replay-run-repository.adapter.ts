import { Inject, Injectable } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import { ReplayRunRepositoryPort } from '../../application/ports/replay-run-repository.port';
import { ReplayRun, ReplayRunStatus } from '../../domain/replay-run.entity';

@Injectable()
export class PostgresReplayRunRepositoryAdapter implements ReplayRunRepositoryPort {
  constructor(
    @Inject(PrismaService)
    private readonly prisma: PrismaService,
  ) {}

  async save(run: ReplayRun): Promise<void> {
    await this.prisma.replayRun.upsert({
      where: { id: run.id },
      create: {
        id: run.id,
        groupId: run.groupId,
        topic: run.topic,
        status: run.status,
        startOffsets: run.startOffsets,
        highWatermarks: run.highWatermarks,
        scannedCount: run.scannedCount,
        errorMessage: run.errorMessage,
        startedAt: run.startedAt,
        completedAt: run.completedAt,
        createdAt: run.createdAt,
        updatedAt: run.updatedAt,
      },
      update: {
        status: run.status,
        scannedCount: run.scannedCount,
        errorMessage: run.errorMessage,
        startedAt: run.startedAt,
        completedAt: run.completedAt,
        updatedAt: run.updatedAt,
      },
    });
  }

  async findById(id: string): Promise<ReplayRun | null> {
    const raw = await this.prisma.replayRun.findUnique({
      where: { id },
    });
    if (!raw) return null;

    return new ReplayRun({
      id: raw.id,
      groupId: raw.groupId,
      topic: raw.topic,
      status: raw.status as ReplayRunStatus,
      startOffsets: (raw.startOffsets as Record<number, string>) || {},
      highWatermarks: (raw.highWatermarks as Record<number, string>) || {},
      scannedCount: raw.scannedCount,
      errorMessage: raw.errorMessage,
      startedAt: raw.startedAt,
      completedAt: raw.completedAt,
      createdAt: raw.createdAt,
      updatedAt: raw.updatedAt,
    });
  }

  async findLatest(): Promise<ReplayRun | null> {
    const raw = await this.prisma.replayRun.findFirst({
      orderBy: { createdAt: 'desc' },
    });
    if (!raw) return null;

    return new ReplayRun({
      id: raw.id,
      groupId: raw.groupId,
      topic: raw.topic,
      status: raw.status as ReplayRunStatus,
      startOffsets: (raw.startOffsets as Record<number, string>) || {},
      highWatermarks: (raw.highWatermarks as Record<number, string>) || {},
      scannedCount: raw.scannedCount,
      errorMessage: raw.errorMessage,
      startedAt: raw.startedAt,
      completedAt: raw.completedAt,
      createdAt: raw.createdAt,
      updatedAt: raw.updatedAt,
    });
  }
}
