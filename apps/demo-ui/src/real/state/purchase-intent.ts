import { z } from 'zod';

const intentSchema = z.object({
  amount: z.string().max(64),
  key: z.string().trim().min(1).max(256),
});
export type PurchaseIntent = z.infer<typeof intentSchema>;
export const intentStorageKey = (customerId: string) => `real-purchase-intent:v1:${customerId}`;

export function readPurchaseIntent(
  storage: Pick<Storage, 'getItem'>,
  customerId: string,
): PurchaseIntent | null {
  try {
    const parsed = intentSchema.safeParse(
      JSON.parse(storage.getItem(intentStorageKey(customerId)) ?? 'null'),
    );
    return parsed.success ? parsed.data : null;
  } catch {
    return null;
  }
}
