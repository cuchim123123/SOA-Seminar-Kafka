/**
 * Kafka Default Partitioner Simulation
 *
 * Kafka's built-in DefaultPartitioner (org.apache.kafka.clients.producer.internals.DefaultPartitioner)
 * uses a 32-bit Murmur2 hash implementation with seed 0x9747b28c.
 *
 * Algorithm:
 *   hash = murmur2(utf8Encode(key), seed = 0x9747b28c)
 *   positive = hash & 0x7fffffff
 *   targetPartition = positive % partitionCount
 */

export function murmur2(data: Uint8Array): number {
  const m = 0x5bd1e995;
  const r = 24;
  const seed = 0x9747b28c;
  let h = seed ^ data.length;
  let length = data.length;
  let i = 0;

  while (length >= 4) {
    let k =
      (data[i] & 0xff) |
      ((data[i + 1] & 0xff) << 8) |
      ((data[i + 2] & 0xff) << 16) |
      ((data[i + 3] & 0xff) << 24);

    k = Math.imul(k, m);
    k ^= k >>> r;
    k = Math.imul(k, m);

    h = Math.imul(h, m);
    h ^= k;

    i += 4;
    length -= 4;
  }

  if (length === 3) {
    h ^= (data[i + 2] & 0xff) << 16;
    h ^= (data[i + 1] & 0xff) << 8;
    h ^= data[i] & 0xff;
    h = Math.imul(h, m);
  } else if (length === 2) {
    h ^= (data[i + 1] & 0xff) << 8;
    h ^= data[i] & 0xff;
    h = Math.imul(h, m);
  } else if (length === 1) {
    h ^= data[i] & 0xff;
    h = Math.imul(h, m);
  }

  h ^= h >>> 13;
  h = Math.imul(h, m);
  h ^= h >>> 15;

  return h;
}

export interface PartitionComputationResult {
  key: string;
  rawHash: number;
  hexHash: string;
  positiveHash: number;
  partitionCount: number;
  targetPartition: number;
  formula: string;
}

export function computeKafkaPartition(
  key: string,
  partitionCount: number,
): PartitionComputationResult {
  if (partitionCount <= 0) {
    return {
      key,
      rawHash: 0,
      hexHash: '0x00000000',
      positiveHash: 0,
      partitionCount,
      targetPartition: 0,
      formula: '0 % 0 = 0',
    };
  }

  const encoded = new TextEncoder().encode(key);
  const rawHash = murmur2(encoded);
  const positiveHash = rawHash & 0x7fffffff;
  const targetPartition = positiveHash % partitionCount;
  const hexHash = `0x${(rawHash >>> 0).toString(16).padStart(8, '0').toUpperCase()}`;
  const formula = `(${positiveHash} % ${partitionCount}) = Partition #${targetPartition}`;

  return {
    key,
    rawHash,
    hexHash,
    positiveHash,
    partitionCount,
    targetPartition,
    formula,
  };
}
