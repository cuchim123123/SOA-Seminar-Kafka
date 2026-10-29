import { Inject, Injectable } from '@nestjs/common';
import {
  PURCHASE_READ_DAO_PORT,
  PurchaseReadDaoPort,
  PurchaseReadModel,
} from '../ports/purchase-read-dao.port';
import { GetPurchaseQuery } from './get-purchase.query';

@Injectable()
export class GetPurchaseHandler {
  constructor(
    @Inject(PURCHASE_READ_DAO_PORT)
    private readonly readDao: PurchaseReadDaoPort,
  ) {}

  async execute(query: GetPurchaseQuery): Promise<PurchaseReadModel | null> {
    return this.readDao.findById(query.data.purchaseId);
  }
}
