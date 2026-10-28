import { BadRequestException } from '@nestjs/common';
import { describe, expect, it, vi } from 'vitest';
import { ZodValidationPipe } from '../../../src/common/pipes/zod-validation.pipe';
import { CreatePurchaseCommand } from '../../../src/purchases/application/commands/create-purchase.command';
import { CreatePurchaseHandler } from '../../../src/purchases/application/commands/create-purchase.handler';
import { createPurchaseSchema } from '../../../src/purchases/interfaces/http/dto/create-purchase.dto';
import { PurchasesController } from '../../../src/purchases/interfaces/http/purchases.controller';

describe('PurchasesController & ZodValidationPipe', () => {
  describe('PurchasesController', () => {
    it('dispatches command to handler and returns HTTP 202 response shape', async () => {
      const mockHandler = {
        execute: vi.fn(async (cmd: CreatePurchaseCommand) => ({
          purchaseId: 'pur_xyz',
          customerId: cmd.customerId,
          amountCents: cmd.amountCents,
          customerSequence: 1,
          eventId: '01a18192-9c94-7d7b-952b-02265a63bac4',
          correlationId: cmd.correlationId ?? 'cor_123',
          idempotencyKey: cmd.idempotencyKey,
        })),
      } as unknown as CreatePurchaseHandler;

      const controller = new PurchasesController(mockHandler);
      const response = await controller.createPurchase(
        {
          customerId: 'CUST_1',
          amountCents: 2000,
          correlationId: '01a18192-9c94-7d7b-952b-02265a63bac4',
        },
        'idem-header-001',
      );

      expect(mockHandler.execute).toHaveBeenCalledWith(
        new CreatePurchaseCommand(
          'CUST_1',
          2000,
          '01a18192-9c94-7d7b-952b-02265a63bac4',
          'idem-header-001',
        ),
      );

      expect(response).toEqual({
        purchaseId: 'pur_xyz',
        requestStatus: 'ACCEPTED',
        eventId: '01a18192-9c94-7d7b-952b-02265a63bac4',
        correlationId: '01a18192-9c94-7d7b-952b-02265a63bac4',
      });
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
