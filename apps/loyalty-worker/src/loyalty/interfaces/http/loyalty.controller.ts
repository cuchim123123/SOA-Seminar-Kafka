import { Controller, Get, Inject, NotFoundException, Param } from '@nestjs/common';
import { GetLoyaltyAccountHandler } from '../../application/queries/get-loyalty-account.handler';
import { GetLoyaltyAccountQuery } from '../../application/queries/get-loyalty-account.query';
import { LoyaltyAccountResponseDto } from './dto/loyalty-account-response.dto';

@Controller(['api/v1/demo/loyalty', 'api/v1/loyalty'])
export class LoyaltyController {
  constructor(
    @Inject(GetLoyaltyAccountHandler)
    private readonly getLoyaltyAccountHandler: GetLoyaltyAccountHandler,
  ) {}

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
