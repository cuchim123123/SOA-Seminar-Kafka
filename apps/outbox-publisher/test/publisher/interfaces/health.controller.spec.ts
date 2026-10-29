import { describe, expect, it, vi } from 'vitest';
import { PrismaService } from '../../../src/database/prisma.service';
import { OutboxPublisherService } from '../../../src/publisher/application/services/outbox-publisher.service';
import { HealthController } from '../../../src/publisher/interfaces/http/health.controller';

describe('HealthController', () => {
  it('returns ok on GET /health', () => {
    const mockPrisma = {} as unknown as PrismaService;
    const mockService = {} as unknown as OutboxPublisherService;
    const controller = new HealthController(mockPrisma, mockService);

    const result = controller.getHealth();
    expect(result.status).toBe('ok');
    expect(result.service).toBe('outbox-publisher');
  });

  it('returns ready on GET /health/ready when database is connected', async () => {
    const mockPrisma = {
      $queryRaw: vi.fn(async () => [{ 1: 1 }]),
    } as unknown as PrismaService;

    const mockService = {
      getStats: vi.fn(() => ({
        isRunning: true,
        totalPublished: 5,
        totalErrors: 0,
        lastPublishedEventId: 'evt_1',
        lastPublishedAt: '2026-10-29T12:00:00.000Z',
        currentBackoffMs: 250,
      })),
    } as unknown as OutboxPublisherService;

    const controller = new HealthController(mockPrisma, mockService);
    const result = await controller.getReadiness();

    expect(result.status).toBe('ready');
    expect(result.database).toBe('connected');
    expect(result.stats.totalPublished).toBe(5);
  });

  it('triggers drainBatch on POST /ops/drain', async () => {
    const mockPrisma = {} as unknown as PrismaService;
    const mockService = {
      drainBatch: vi.fn(async () => 3),
      getStats: vi.fn(() => ({
        isRunning: true,
        totalPublished: 3,
        totalErrors: 0,
        lastPublishedEventId: 'evt_3',
        lastPublishedAt: '2026-10-29T12:00:00.000Z',
        currentBackoffMs: 250,
      })),
    } as unknown as OutboxPublisherService;

    const controller = new HealthController(mockPrisma, mockService);
    const result = await controller.drainPending();

    expect(mockService.drainBatch).toHaveBeenCalled();
    expect(result.processed).toBe(3);
    expect(result.stats.totalPublished).toBe(3);
  });
});
