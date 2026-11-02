import { z } from 'zod';

// Shared wire-input validation only; projection models stay context-owned.
const eventIdsSchema = z
  .string()
  .min(1)
  .max(12_899)
  .transform((value) => value.split(',').map((id) => id.trim()))
  .pipe(z.array(z.string().min(1).max(128)).min(1).max(100))
  .transform((ids) => [...new Set(ids)]);

export const parseEventReceiptIds = (value: unknown) => eventIdsSchema.safeParse(value);
