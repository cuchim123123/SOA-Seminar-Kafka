import { BadRequestException } from '@nestjs/common';
import { describe, expect, it, vi } from 'vitest';
import { ZodValidationPipe } from '../../../src/common/pipes/zod-validation.pipe';
import { CreatePurchaseCommand } from '../../../src/purchases/application/commands/create-purchase.command';
import { CreatePurchaseHandler } from '../../../src/purchases/application/commands/create-purchase.handler';
import { InvalidateSyntheticPurchaseHandler } from '../../../src/purchases/application/commands/invalidate-synthetic-purchase.handler';
import { GetPurchaseHandler } from '../../../src/purchases/application/queries/get-purchase.handler';
import { createPurchaseSchema } from '../../../src/purchases/interfaces/http/dto/create-purchase.dto';
import { PurchasesController } from '../../../src/purchases/interfaces/http/purchases.controller';

describe('PurchasesController & ZodValidationPipe', () => {
  describe('PurchasesController', () => {
    it('dispatches command to handler and returns HTTP 202 response shape', async () => {
      const mockHandler = {
        execute: vi.fn(async (cmd: CreatePurchaseCommand) => ({
          purchaseId: 'pur_xyz',
          customerId: cmd.data.customerId,
          amountCents: cmd.data.amountCents,
          customerSequence: 1,
          eventId: '01a18192-9c94-7d7b-952b-02265a63bac4',
          correlationId: cmd.data.correlationId ?? 'cor_123',
          idempotencyKey: cmd.data.idempotencyKey,
        })),
      } as unknown as CreatePurchaseHandler;

      const mockInvalidateHandler = {
        execute: vi.fn(),
      } as unknown as InvalidateSyntheticPurchaseHandler;

      const mockGetHandler = {
        execute: vi.fn(),
      } as unknown as GetPurchaseHandler;

      const controller = new PurchasesController(
        mockHandler,
        mockInvalidateHandler,
        mockGetHandler,
      );
      const response = await controller.createPurchase(
        {
          customerId: 'CUST_1',
          amountCents: 2000,
          correlationId: '01a18192-9c94-7d7b-952b-02265a63bac4',
        },
        'idem-header-001',
      );

      expect(mockHandler.execute).toHaveBeenCalledWith(
        new CreatePurchaseCommand({
          customerId: 'CUST_1',
          amountCents: 2000,
          correlationId: '01a18192-9c94-7d7b-952b-02265a63bac4',
          idempotencyKey: 'idem-header-001',
        }),
      );

      expect(response).toEqual({
        purchaseId: 'pur_xyz',
        requestStatus: 'ACCEPTED',
        eventId: '01a18192-9c94-7d7b-952b-02265a63bac4',
        correlationId: '01a18192-9c94-7d7b-952b-02265a63bac4',
      });
    });

    it('dispatches invalidate command to handler and returns HTTP 200 response shape', async () => {
      const mockCreateHandler = {} as unknown as CreatePurchaseHandler;
      const mockInvalidateHandler = {
        execute: vi.fn(async () => ({
          purchaseId: 'pur_xyz',
          customerId: 'CUST_1',
          status: 'INVALIDATED' as const,
          customerSequence: 4,
          eventId: 'evt_inv_123',
          reason: 'DEMO_INVALIDATION' as const,
          isAlreadyInvalidated: false,
        })),
      } as unknown as InvalidateSyntheticPurchaseHandler;
      const mockGetHandler = {} as unknown as GetPurchaseHandler;

      const controller = new PurchasesController(
        mockCreateHandler,
        mockInvalidateHandler,
        mockGetHandler,
      );

      const response = await controller.invalidatePurchase('pur_xyz', {
        reason: 'DEMO_INVALIDATION',
      });

      expect(mockInvalidateHandler.execute).toHaveBeenCalled();
      expect(response).toEqual({
        purchaseId: 'pur_xyz',
        customerId: 'CUST_1',
        status: 'INVALIDATED',
        customerSequence: 4,
        eventId: 'evt_inv_123',
        reason: 'DEMO_INVALIDATION',
        isAlreadyInvalidated: false,
      });
    });

    it('returns purchase read model when found', async () => {
      const mockCreateHandler = {} as unknown as CreatePurchaseHandler;
      const mockInvalidateHandler = {} as unknown as InvalidateSyntheticPurchaseHandler;
      const mockGetHandler = {
        execute: vi.fn(async () => ({
          purchaseId: 'pur_xyz',
          customerId: 'CUST_1',
          amountCents: 2000,
          status: 'COMPLETED' as const,
          customerSequence: 1,
          createdAt: '2026-10-29T12:00:00.000Z',
          outboxStatus: 'PUBLISHED' as const,
          publishedAt: '2026-10-29T12:00:01.000Z',
          eventId: 'evt_123',
        })),
      } as unknown as GetPurchaseHandler;

      const controller = new PurchasesController(
        mockCreateHandler,
        mockInvalidateHandler,
        mockGetHandler,
      );
      const result = await controller.getPurchase('pur_xyz');

      expect(mockGetHandler.execute).toHaveBeenCalled();
      expect(result.purchaseId).toBe('pur_xyz');
      expect(result.outboxStatus).toBe('PUBLISHED');
    });

    it('throws NotFoundException when purchase does not exist', async () => {
      const mockCreateHandler = {} as unknown as CreatePurchaseHandler;
      const mockInvalidateHandler = {} as unknown as InvalidateSyntheticPurchaseHandler;
      const mockGetHandler = {
        execute: vi.fn(async () => null),
      } as unknown as GetPurchaseHandler;

      const controller = new PurchasesController(
        mockCreateHandler,
        mockInvalidateHandler,
        mockGetHandler,
      );
      await expect(controller.getPurchase('pur_unknown')).rejects.toThrow();
    });
  });

  describe('ZodValidationPipe with createPurchaseSchema', () => {
    const pipe = new ZodValidationPipe(createPurchaseSchema);

    it('accepts valid purchase DTO', () => {
      const valid = {
        customerId: 'CUST_1',
        amountCents: 1500,
      };
      expect(pipe.transform(valid)).toEqual(valid);
    });

    it('rejects empty or whitespace-only customerId', () => {
      expect(() => pipe.transform({ customerId: '', amountCents: 1500 })).toThrow(
        BadRequestException,
      );
      expect(() => pipe.transform({ customerId: '   ', amountCents: 1500 })).toThrow(
        BadRequestException,
      );
    });

    it('rejects zero or negative amountCents', () => {
      expect(() => pipe.transform({ customerId: 'CUST_1', amountCents: 0 })).toThrow(
        BadRequestException,
      );
      expect(() => pipe.transform({ customerId: 'CUST_1', amountCents: -100 })).toThrow(
        BadRequestException,
      );
    });

    it('rejects invalid correlationId that is not a UUID', () => {
      expect(() =>
        pipe.transform({
          customerId: 'CUST_1',
          amountCents: 100,
          correlationId: 'not-a-uuid',
        }),
      ).toThrow(BadRequestException);
    });
  });
});
