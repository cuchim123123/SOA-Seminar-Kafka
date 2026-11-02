import { Controller, Get, Inject, Query } from '@nestjs/common';
import { z } from 'zod';
import { ZodValidationPipe } from '../../../common/pipes/zod-validation.pipe';
import { ListPurchasesHandler } from '../../application/queries/list-purchases.handler';

export const purchaseHistoryQuerySchema = z
  .object({
    purchaseId: z.string().trim().min(1).max(256).optional(),
    customerId: z.string().trim().min(1).max(128).optional(),
    cursor: z.string().min(1).max(256).optional(),
    limit: z.coerce.number().int().min(1).max(100).default(25),
  })
  .strict();

@Controller('api/v1/purchases')
export class PurchaseHistoryController {
  constructor(@Inject(ListPurchasesHandler) private readonly handler: ListPurchasesHandler) {}

  @Get()
  list(
    @Query(new ZodValidationPipe(purchaseHistoryQuerySchema))
    query: z.infer<typeof purchaseHistoryQuerySchema>,
  ) {
    return this.handler.execute(query);
  }
}
