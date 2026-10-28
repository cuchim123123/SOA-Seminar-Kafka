import { Inject, Injectable } from '@nestjs/common';
import {
  SALES_SUMMARY_READ_DAO_PORT,
  SalesSummaryReadDaoPort,
  SalesSummaryReadModel,
} from '../ports/sales-summary-read-dao.port';
import { GetSalesSummaryQuery } from './get-sales-summary.query';

@Injectable()
export class GetSalesSummaryHandler {
  constructor(
    @Inject(SALES_SUMMARY_READ_DAO_PORT)
    private readonly readDao: SalesSummaryReadDaoPort,
  ) {}

  async execute(_query: GetSalesSummaryQuery): Promise<SalesSummaryReadModel> {
    return this.readDao.getSummary();
  }
}
