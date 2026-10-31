import { z } from 'zod';

export const startReplaySchema = z.object({
  runId: z.string().optional(),
  topic: z.string().optional(),
  requiredStartOffsets: z.record(z.string()).optional(),
  simulateRetentionFailure: z.boolean().optional(),
});

export type StartReplayDto = z.infer<typeof startReplaySchema>;
