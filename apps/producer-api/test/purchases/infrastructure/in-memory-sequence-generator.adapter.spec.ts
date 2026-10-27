import { describe, expect, it } from 'vitest';
import { InMemorySequenceGeneratorAdapter } from '../../../src/purchases/infrastructure/adapters/in-memory-sequence-generator.adapter';

describe('InMemorySequenceGeneratorAdapter', () => {
  it('generates strictly monotonic sequences per customer independently', async () => {
    const generator = new InMemorySequenceGeneratorAdapter();

    expect(await generator.nextSequence('U001')).toBe(1);
    expect(await generator.nextSequence('U001')).toBe(2);
    expect(await generator.nextSequence('U002')).toBe(1);
    expect(await generator.nextSequence('U001')).toBe(3);
    expect(await generator.nextSequence('U002')).toBe(2);
  });
});
