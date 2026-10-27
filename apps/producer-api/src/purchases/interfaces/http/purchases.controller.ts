import { Body, Controller, HttpCode, HttpStatus, Inject, Post, UsePipes } from '@nestjs/common';
import { ZodValidationPipe } from '../../../common/pipes/zod-validation.pipe';
import { CreatePurchaseCommand } from '../../application/commands/create-purchase.command';
import { CreatePurchaseHandler } from '../../application/commands/create-purchase.handler';
import { CreatePurchaseDto, createPurchaseSchema } from './dto/create-purchase.dto';
import { PurchaseAcceptedResponseDto } from './dto/purchase-accepted-response.dto';

@Controller('api/v1/demo/purchases')
export class PurchasesController {
  constructor(
    @Inject(CreatePurchaseHandler)
    private readonly createPurchaseHandler: CreatePurchaseHandler,
  ) {}

  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  @UsePipes(new ZodValidationPipe(createPurchaseSchema))
  async createPurchase(@Body() dto: CreatePurchaseDto): Promise<PurchaseAcceptedResponseDto> {
    const command = new CreatePurchaseCommand(dto.customerId, dto.amountCents, dto.correlationId);
    const result = await this.createPurchaseHandler.execute(command);

    return {
      purchaseId: result.purchaseId,
      requestStatus: 'ACCEPTED',
      eventId: result.eventId,
      correlationId: result.correlationId,
      partition: result.kafkaMetadata.partition,
      offset: result.kafkaMetadata.offset,
    };
  }
}
