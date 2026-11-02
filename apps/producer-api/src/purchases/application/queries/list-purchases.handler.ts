import { Inject, Injectable } from '@nestjs/common';
import {
  PURCHASE_HISTORY_PORT,
  PurchaseHistoryPort,
  PurchaseHistoryQuery,
} from '../ports/purchase-history.port';

@Injectable()
export class ListPurchasesHandler {
  constructor(@Inject(PURCHASE_HISTORY_PORT) private readonly history: PurchaseHistoryPort) {}

  execute(query: PurchaseHistoryQuery) {
    return this.history.list(query);
  }
}
