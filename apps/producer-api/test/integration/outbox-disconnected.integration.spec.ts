import { ConflictException } from '@nestjs/common';
import { describe, expect, it } from 'vitest';
import { PrismaService } from '../../src/database/prisma.service';
import { AppConfigService } from '../../src/config/app-config.service';
import { CreatePurchaseCommand } from '../../src/purchases/application/commands/create-purchase.command';
import { CreatePurchaseHandler } from '../../src/purchases/application/commands/create-purchase.handler';
import {
  EventPublisherPort,
  PublishResult,
} from '../../src/purchases/application/ports/event-publisher.port';
import { PostgresPurchaseReadDaoAdapter } from '../../src/purchases/infrastructure/adapters/postgres-purchase-read-dao.adapter';
import { PostgresUnitOfWorkAdapter } from '../../src/purchases/infrastructure/adapters/postgres-unit-of-work.adapter';
import { PostgresOutboxRepositoryAdapter } from '../../src/purchases/infrastructure/adapters/postgres-outbox-repository.adapter';
import { OutboxRelayService } from '../../src/purchases/infrastructure/services/outbox-relay.service';
import { IntegrationEventEnvelopeV1 } from '@lab/contracts';
import { validateEnv } from '../../src/config/env.validation';

describe('P4-02 CQRS Purchase Flow & Outbox Offline Durability', () => {
  it('proves DB source commit survives Kafka disconnection with visible PENDING outbox', async () => {
    const config = new AppConfigService(
      validateEnv({
        DATABASE_URL:
          process.env.DATABASE_URL ||
          `postgresql://${process.env.POSTGRES_USER || 'postgres'}:${process.env.POSTGRES_PASSWORD || 'postgres'}@localhost:${process.env.POSTGRES_PORT || '5432'}/${process.env.POSTGRES_DB || 'kafka_streaming_lab'}?schema=public`,
      }),
    );

    const prisma = new PrismaService(config);
    await prisma.onModuleInit();

    // 1. Mock Kafka EventPublisher that fails to simulate Kafka disconnection
    let kafkaAvailable = false;
    const publishedEvents: IntegrationEventEnvelopeV1[] = [];

    const mockPublisher: EventPublisherPort = {
      publish: async (envelope: IntegrationEventEnvelopeV1): Promise<PublishResult> => {
        if (!kafkaAvailable) {
          throw new Error('Kafka broker connection refused (simulated offline)');
        }
        publishedEvents.push(envelope);
        return {
          eventId: envelope.eventId,
          correlationId: envelope.correlationId,
          topic: 'purchase.events',
          partition: 0,
          offset: '100',
        };
      },
    };

    const outboxRepo = new PostgresOutboxRepositoryAdapter(prisma);
    const outboxRelay = new OutboxRelayService(outboxRepo, mockPublisher);
    const unitOfWork = new PostgresUnitOfWorkAdapter(prisma, outboxRelay);
    const createPurchaseHandler = new CreatePurchaseHandler(unitOfWork);
    const readDao = new PostgresPurchaseReadDaoAdapter(prisma);

    const customerId = `U_OFFLINE_${Date.now()}`;
    const idempotencyKey = `idem_${Date.now()}`;

    // 2. Dispatch CreatePurchaseCommand while Kafka is DISCONNECTED
    const commandResult = await createPurchaseHandler.execute(
      new CreatePurchaseCommand({
        customerId,
        amountCents: 4500,
        idempotencyKey,
      }),
    );

    expect(commandResult.purchaseId).toBeDefined();
    expect(commandResult.customerId).toBe(customerId);
    expect(commandResult.amountCents).toBe(4500);
    expect(commandResult.customerSequence).toBe(1);

    // Give asynchronous post-commit relay attempt time to execute and fail gracefully
    await new Promise((resolve) => setTimeout(resolve, 500));

    // 3. Directly inspect PostgreSQL: Purchase is committed!
    const dbPurchase = await prisma.purchase.findUnique({
      where: { purchaseId: commandResult.purchaseId },
    });
    expect(dbPurchase).not.toBeNull();
    expect(dbPurchase?.status).toBe('COMPLETED');
    expect(Number(dbPurchase?.amountCents)).toBe(4500);

    // 4. Directly inspect PostgreSQL: Outbox event is committed as PENDING!
    const dbOutbox = await prisma.outboxEvent.findUnique({
      where: { eventId: commandResult.eventId },
    });
    expect(dbOutbox).not.toBeNull();
    expect(dbOutbox?.status).toBe('PENDING');
    expect(dbOutbox?.publishedAt).toBeNull();

    // 5. Query via CQRS Read DAO: verify outboxStatus is PENDING
    const readModel = await readDao.findById(commandResult.purchaseId);
    expect(readModel).not.toBeNull();
    expect(readModel?.purchaseId).toBe(commandResult.purchaseId);
    expect(readModel?.status).toBe('COMPLETED');
    expect(readModel?.outboxStatus).toBe('PENDING');
    expect(readModel?.publishedAt).toBeNull();

    // 6. Test Idempotency with exact duplicate while still offline: returns same purchaseId
    const duplicateResult = await createPurchaseHandler.execute(
      new CreatePurchaseCommand({
        customerId,
        amountCents: 4500,
        idempotencyKey,
      }),
    );
    expect(duplicateResult.purchaseId).toBe(commandResult.purchaseId);

    // 7. Test Idempotency conflict with different amount: throws ConflictException (HTTP 409)
    await expect(
      createPurchaseHandler.execute(
        new CreatePurchaseCommand({
          customerId,
          amountCents: 9999,
          idempotencyKey,
        }),
      ),
    ).rejects.toThrow(ConflictException);

    // 8. Reconnect Kafka and drain the outbox
    await new Promise((resolve) => setTimeout(resolve, 300));
    kafkaAvailable = true;
    const relayedCount = await outboxRelay.processPending();
    expect(relayedCount).toBeGreaterThanOrEqual(1);

    // 9. Query via CQRS Read DAO after relay: verify outboxStatus is now PUBLISHED
    const publishedReadModel = await readDao.findById(commandResult.purchaseId);
    expect(publishedReadModel?.outboxStatus).toBe('PUBLISHED');
    expect(publishedReadModel?.publishedAt).not.toBeNull();

    await prisma.$disconnect();
  });
});
