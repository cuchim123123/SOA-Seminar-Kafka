export interface EventReceipt {
  eventId: string;
  processedAt: string;
}
export const EVENT_RECEIPT_READ_PORT = Symbol('EVENT_RECEIPT_READ_PORT');
export interface EventReceiptReadPort {
  find(eventIds: string[]): Promise<EventReceipt[]>;
}
