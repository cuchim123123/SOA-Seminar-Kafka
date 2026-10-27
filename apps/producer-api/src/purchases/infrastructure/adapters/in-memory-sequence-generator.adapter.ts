import { Injectable } from '@nestjs/common';
import { SequenceGeneratorPort } from '../../application/ports/sequence-generator.port';

/**
 * In-memory customer sequence generator for Phase 1 & 2.
 *
 * NOTE - PHASE 1 ARCHITECTURAL LIMITATION:
 * Single-instance non-durable in-memory sequence tracking for simulated fixtures.
 * In Phase 4, this is replaced by durable PostgreSQL sequence allocation with customer row locks (ADR-004).
 */
@Injectable()
export class InMemorySequenceGeneratorAdapter implements SequenceGeneratorPort {
  private readonly sequences = new Map<string, number>();

  async nextSequence(customerId: string): Promise<number> {
    const current = this.sequences.get(customerId) ?? 0;
    const next = current + 1;
    this.sequences.set(customerId, next);
    return Promise.resolve(next);
  }
}
