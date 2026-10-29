import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpStatus,
  Inject,
  NotFoundException,
  Param,
  Post,
  UsePipes,
} from '@nestjs/common';
import { ZodValidationPipe } from '../../../common/pipes/zod-validation.pipe';
import { CreatePurchaseCommand } from '../../application/commands/create-purchase.command';
import { CreatePurchaseHandler } from '../../application/commands/create-purchase.handler';
import { InvalidateSyntheticPurchaseCommand } from '../../application/commands/invalidate-synthetic-purchase.command';
import { InvalidateSyntheticPurchaseHandler } from '../../application/commands/invalidate-synthetic-purchase.handler';
import { GetPurchaseHandler } from '../../application/queries/get-purchase.handler';
import { GetPurchaseQuery } from '../../application/queries/get-purchase.query';
import { CreatePurchaseDto, createPurchaseSchema } from './dto/create-purchase.dto';
import { InvalidatePurchaseDto, invalidatePurchaseSchema } from './dto/invalidate-purchase.dto';
import { PurchaseAcceptedResponseDto } from './dto/purchase-accepted-response.dto';
import { PurchaseInvalidatedResponseDto } from './dto/purchase-invalidated-response.dto';
import { PurchaseStatusResponseDto } from './dto/purchase-status-response.dto';

@Controller(['api/v1/demo/purchases', 'api/v1/purchases'])
export class PurchasesController {
  constructor(
    @Inject(CreatePurchaseHandler)
    private readonly createPurchaseHandler: CreatePurchaseHandler,
    @Inject(InvalidateSyntheticPurchaseHandler)
    private readonly invalidatePurchaseHandler: InvalidateSyntheticPurchaseHandler,
    @Inject(GetPurchaseHandler)
    private readonly getPurchaseHandler: GetPurchaseHandler,
  ) {}

  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  @UsePipes(new ZodValidationPipe(createPurchaseSchema))
  async createPurchase(
    @Body() dto: CreatePurchaseDto,
    @Headers('idempotency-key') idempotencyKeyHeader?: string,
  ): Promise<PurchaseAcceptedResponseDto> {
    const idempotencyKey = idempotencyKeyHeader ?? dto.idempotencyKey;
    const command = new CreatePurchaseCommand({
      customerId: dto.customerId,
      amountCents: dto.amountCents,
      correlationId: dto.correlationId,
      idempotencyKey,
    });
    const result = await this.createPurchaseHandler.execute(command);

    return {
      purchaseId: result.purchaseId,
      requestStatus: 'ACCEPTED',
      eventId: result.eventId,
      correlationId: result.correlationId,
    };
  }

  @Post(':purchaseId/invalidate')
  @HttpCode(HttpStatus.OK)
  async invalidatePurchase(
    @Param('purchaseId') purchaseId: string,
    @Body(new ZodValidationPipe(invalidatePurchaseSchema))
    dto: InvalidatePurchaseDto = { reason: 'DEMO_INVALIDATION' },
  ): Promise<PurchaseInvalidatedResponseDto> {
    const command = new InvalidateSyntheticPurchaseCommand({
      purchaseId,
      reason: dto.reason,
      correlationId: dto.correlationId,
    });
    const result = await this.invalidatePurchaseHandler.execute(command);

    return {
      purchaseId: result.purchaseId,
      customerId: result.customerId,
      status: result.status,
      customerSequence: result.customerSequence,
      eventId: result.eventId,
      reason: result.reason,
      isAlreadyInvalidated: result.isAlreadyInvalidated,
    };
  }

  @Get(':purchaseId')
  async getPurchase(@Param('purchaseId') purchaseId: string): Promise<PurchaseStatusResponseDto> {
    const result = await this.getPurchaseHandler.execute(new GetPurchaseQuery({ purchaseId }));
    if (!result) {
      throw new NotFoundException(`Purchase '${purchaseId}' not found`);
    }
    return result;
  }
}
