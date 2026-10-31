import { Controller, Get, Inject, NotFoundException, Optional, Param } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import { GetLoyaltyAccountHandler } from '../../application/queries/get-loyalty-account/get-loyalty-account.handler';
import { GetLoyaltyAccountQuery } from '../../application/queries/get-loyalty-account/get-loyalty-account.query';
import { LoyaltyAccountResponseDto } from './dto/loyalty-account-response.dto';

@Controller(['api/v1/demo/loyalty', 'api/v1/loyalty'])
export class LoyaltyController {
  constructor(
    @Inject(GetLoyaltyAccountHandler)
    private readonly getLoyaltyAccountHandler: GetLoyaltyAccountHandler,
    @Optional()
    @Inject(PrismaService)
    private readonly prisma?: PrismaService,
  ) {}

  @Get('processed-events')
  async getProcessedEvents(): Promise<{
    count: number;
    events: Array<{
      consumerName: string;
      eventId: string;
      processedAt: string;
    }>;
  }> {
    if (!this.prisma) {
      return { count: 0, events: [] };
    }

    const [count, events] = await Promise.all([
      this.prisma.processedEvent.count(),
      this.prisma.processedEvent.findMany({
        take: 30,
        orderBy: { processedAt: 'desc' },
      }),
    ]);

    return {
      count,
      events: events.map((e) => ({
        consumerName: e.consumerName,
        eventId: e.eventId,
        processedAt: e.processedAt.toISOString(),
      })),
    };
  }

  @Get(':customerId')
  async getLoyaltyAccount(
    @Param('customerId') customerId: string,
  ): Promise<LoyaltyAccountResponseDto> {
    const result = await this.getLoyaltyAccountHandler.execute(
      new GetLoyaltyAccountQuery({ customerId }),
    );

    if (!result) {
      throw new NotFoundException(`Loyalty account for customer '${customerId}' not found`);
    }

    return result;
  }
}
