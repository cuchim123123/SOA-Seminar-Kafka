/**
 * Kafka's default Murmur2 implementation as defined in:
 * org.apache.kafka.common.utils.Utils.murmur2(byte[] data)
 * toPositive(Utils.murmur2(keyBytes)) % numPartitions
 */
export function kafkaMurmur2(data: Uint8Array): number {
  const length = data.length;
  const seed = 0x9747b28c;
  const m = 0x5bd1e995;
  const r = 24;

  let h = seed ^ length;
  const length4 = length >> 2;

  for (let i = 0; i < length4; i++) {
    const i4 = i << 2;
    let k =
      (data[i4 + 0] & 0xff) |
      ((data[i4 + 1] & 0xff) << 8) |
      ((data[i4 + 2] & 0xff) << 16) |
      ((data[i4 + 3] & 0xff) << 24);
    k = Math.imul(k, m);
    k ^= k >>> r;
    k = Math.imul(k, m);
    h = Math.imul(h, m) ^ k;
  }

  const extra = length & 3;
  if (extra >= 3) h ^= (data[(length4 << 2) + 2] & 0xff) << 16;
  if (extra >= 2) h ^= (data[(length4 << 2) + 1] & 0xff) << 8;
  if (extra >= 1) {
    h ^= data[(length4 << 2) + 0] & 0xff;
    h = Math.imul(h, m);
  }

  h ^= h >>> 13;
  h = Math.imul(h, m);
  h ^= h >>> 15;

  return h & 0x7fffffff;
}

/**
 * Calculates exact Kafka partition for a string key using Murmur2
 */
export function calculateKafkaPartition(key: string, partitionCount: number): number {
  if (!key || partitionCount <= 1) return 0;
  const encoder = new TextEncoder();
  const bytes = encoder.encode(key);
  const hash = kafkaMurmur2(bytes);
  return hash % partitionCount;
}
