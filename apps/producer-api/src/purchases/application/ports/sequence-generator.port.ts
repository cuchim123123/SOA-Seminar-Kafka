export const SEQUENCE_GENERATOR_PORT = Symbol('SEQUENCE_GENERATOR_PORT');

export interface SequenceGeneratorPort {
  nextSequence(customerId: string): Promise<number>;
}
