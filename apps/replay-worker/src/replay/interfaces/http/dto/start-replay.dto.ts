import { z } from 'zod';

export const startReplaySchema = z.object({
  runId: z.string().optional(),
  topic: z.string().optional(),
});

export type StartReplayDto = z.infer<typeof startReplaySchema>;
