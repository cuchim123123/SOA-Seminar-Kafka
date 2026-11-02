import type { SimMessage } from './message';
import { calculateKafkaPartition } from '../../utils/kafkaPartitioner';

export interface SimPartitionRecord {
  offset: number;
  message: SimMessage;
  appendedAtVirtualMs: number;
}

export interface SimPartition {
  partitionId: number;
  leaderBrokerId: number;
  replicas: number[];
  isr: number[];
  records: SimPartitionRecord[];
  logStartOffset: number;
  logEndOffset: number; // Next offset to append
  highWatermark: number; // Highest replicated offset visible to consumers
}

export interface SimTopic {
  name: string;
  partitions: Map<number, SimPartition>;
  replicationFactor: number;
  minInsyncReplicas: number;
}

export interface SimBroker {
  id: number;
  host: string;
  isController: boolean;
  status: 'ONLINE' | 'CRASHED';
}

export class SimKafkaClusterModel {
  public clusterId: string;
  public brokers: Map<number, SimBroker> = new Map();
  public topics: Map<string, SimTopic> = new Map();

  constructor(clusterId: string = 'kraft-sim', brokerCount: number = 3) {
    this.clusterId = clusterId;
    this.initializeBrokers(brokerCount);
    this.createTopic('purchases', 4, 3, 2);
  }

  public initializeBrokers(count: number): void {
    this.brokers.clear();
    const clamped = Math.max(1, Math.min(5, count));
    for (let i = 1; i <= clamped; i++) {
      this.brokers.set(i, {
        id: i,
        host: `broker-${i}:9092`,
        isController: i === 1,
        status: 'ONLINE',
      });
    }
  }

  public createTopic(
    topicName: string,
    partitionCount: number = 4,
    replicationFactor: number = 3,
    minInsyncReplicas: number = 2,
  ): SimTopic {
    const partitions = new Map<number, SimPartition>();
    const brokerIds = Array.from(this.brokers.keys());
    const safeRF = Math.max(1, Math.min(brokerIds.length, replicationFactor));

    for (let p = 0; p < partitionCount; p++) {
      // Deterministic round-robin replica distribution across available brokers
      const replicas: number[] = [];
      for (let r = 0; r < safeRF; r++) {
        const brokerIndex = (p + r) % brokerIds.length;
        replicas.push(brokerIds[brokerIndex]);
      }

      const leader = replicas[0];
      partitions.set(p, {
        partitionId: p,
        leaderBrokerId: leader,
        replicas,
        isr: [...replicas],
        records: [],
        logStartOffset: 0,
        logEndOffset: 0,
        highWatermark: 0,
      });
    }

    const topic: SimTopic = {
      name: topicName,
      partitions,
      replicationFactor: safeRF,
      minInsyncReplicas,
    };

    this.topics.set(topicName, topic);
    return topic;
  }

  /**
   * Route message key to partition using exact Murmur2 hash
   */
  public routeKeyToPartition(topicName: string, key: string): number {
    const topic = this.topics.get(topicName) || this.topics.values().next().value;
    const pCount = topic ? topic.partitions.size : 4;
    return calculateKafkaPartition(key, pCount);
  }

  public reassignReplicas(topicName: string, replicationFactor: number): void {
    const topic = this.topics.get(topicName);
    if (!topic) throw new Error(`Topic ${topicName} not found`);
    const ids = Array.from(this.brokers.keys());
    if (
      !Number.isInteger(replicationFactor) ||
      replicationFactor < 1 ||
      replicationFactor > ids.length
    )
      throw new Error('Replication factor must fit the broker membership');
    for (const partition of topic.partitions.values()) {
      const replicas = Array.from(
        { length: replicationFactor },
        (_, index) => ids[(partition.partitionId + index) % ids.length],
      );
      partition.replicas = replicas;
      partition.isr = replicas.filter((id) => this.brokers.get(id)?.status === 'ONLINE');
      if (!partition.isr.includes(partition.leaderBrokerId))
        partition.leaderBrokerId = partition.isr[0] ?? -1;
    }
    topic.replicationFactor = replicationFactor;
  }

  public crashBroker(brokerId: number): {
    affectedPartitions: Array<{ partitionId: number; newLeader?: number }>;
    newController?: number;
  } {
    const broker = this.brokers.get(brokerId);
    if (!broker || broker.status === 'CRASHED') {
      return { affectedPartitions: [] };
    }

    broker.status = 'CRASHED';
    let newController: number | undefined;

    // 1. KRaft Controller Failover if crashed broker was active controller
    if (broker.isController) {
      broker.isController = false;
      const nextOnline = Array.from(this.brokers.values()).find((b) => b.status === 'ONLINE');
      if (nextOnline) {
        nextOnline.isController = true;
        newController = nextOnline.id;
      }
    }

    // 2. Leader failover for partitions where crashed broker was the leader
    const affectedPartitions: Array<{ partitionId: number; newLeader?: number }> = [];

    for (const topic of this.topics.values()) {
      for (const partition of topic.partitions.values()) {
        // Remove from ISR
        partition.isr = partition.isr.filter((id) => id !== brokerId);

        if (partition.leaderBrokerId === brokerId) {
          // Select first online broker from ISR
          const newLeader = partition.isr.find((id) => this.brokers.get(id)?.status === 'ONLINE');
          if (newLeader !== undefined) {
            partition.leaderBrokerId = newLeader;
            affectedPartitions.push({ partitionId: partition.partitionId, newLeader });
          } else {
            // No online in-sync replica available
            partition.leaderBrokerId = -1;
            affectedPartitions.push({ partitionId: partition.partitionId, newLeader: undefined });
          }
        }
      }
    }

    return { affectedPartitions, newController };
  }

  public reviveBroker(brokerId: number): boolean {
    const broker = this.brokers.get(brokerId);
    if (!broker || broker.status === 'ONLINE') return false;

    broker.status = 'ONLINE';

    // Check if controller is needed
    const hasController = Array.from(this.brokers.values()).some(
      (b) => b.isController && b.status === 'ONLINE',
    );
    if (!hasController) {
      broker.isController = true;
    }

    // Re-add to ISR of replicas
    for (const topic of this.topics.values()) {
      for (const partition of topic.partitions.values()) {
        if (partition.replicas.includes(brokerId) && !partition.isr.includes(brokerId)) {
          partition.isr.push(brokerId);
          // If partition had NO_LEADER, revived broker can become leader
          if (partition.leaderBrokerId === -1) {
            partition.leaderBrokerId = brokerId;
          }
        }
      }
    }

    return true;
  }

  public canWriteToPartition(
    topicName: string,
    partitionId: number,
  ): { allowed: boolean; reason?: string } {
    const topic = this.topics.get(topicName);
    if (!topic) return { allowed: false, reason: `Topic ${topicName} not found` };

    const partition = topic.partitions.get(partitionId);
    if (!partition) return { allowed: false, reason: `Partition ${partitionId} not found` };

    if (
      partition.leaderBrokerId === -1 ||
      this.brokers.get(partition.leaderBrokerId)?.status !== 'ONLINE'
    ) {
      return { allowed: false, reason: `LEADER_NOT_AVAILABLE on P${partitionId}` };
    }

    const onlineIsrCount = partition.isr.filter(
      (bId) => this.brokers.get(bId)?.status === 'ONLINE',
    ).length;
    if (onlineIsrCount < topic.minInsyncReplicas) {
      return {
        allowed: false,
        reason: `NOT_ENOUGH_REPLICAS (ISR ${onlineIsrCount} < MinISR ${topic.minInsyncReplicas})`,
      };
    }

    return { allowed: true };
  }

  /**
   * Append record to partition log at logEndOffset
   */
  public appendRecord(
    topicName: string,
    partitionId: number,
    message: SimMessage,
    virtualTimeMs: number,
  ): { offset: number; partition: SimPartition } {
    const topic = this.topics.get(topicName);
    if (!topic) throw new Error(`Topic ${topicName} not found`);

    const partition = topic.partitions.get(partitionId);
    if (!partition) throw new Error(`Partition ${partitionId} not found in ${topicName}`);

    const writeCheck = this.canWriteToPartition(topicName, partitionId);
    if (!writeCheck.allowed) {
      throw new Error(`Write rejected for ${topicName}:${partitionId} - ${writeCheck.reason}`);
    }

    const offset = partition.logEndOffset;
    partition.records.push({
      offset,
      message: { ...message, offset, topic: topicName, partition: partitionId },
      appendedAtVirtualMs: virtualTimeMs,
    });

    partition.logEndOffset += 1;
    // For single-node / initial MVP, highWatermark advances with logEndOffset
    partition.highWatermark = partition.logEndOffset;

    return { offset, partition };
  }

  public getPartition(topicName: string, partitionId: number): SimPartition | undefined {
    return this.topics.get(topicName)?.partitions.get(partitionId);
  }

  public getAllPartitions(topicName: string = 'purchases'): SimPartition[] {
    const topic = this.topics.get(topicName);
    if (!topic) return [];
    return Array.from(topic.partitions.values()).sort((a, b) => a.partitionId - b.partitionId);
  }

  public setPartitionCount(topicName: string, newCount: number): void {
    const topic = this.topics.get(topicName);
    if (!topic) return;
    const clamped = Math.max(1, Math.min(12, newCount));
    const currentCount = topic.partitions.size;

    if (clamped > currentCount) {
      const brokerIds = Array.from(this.brokers.keys());
      for (let p = currentCount; p < clamped; p++) {
        const replicas: number[] = [];
        for (let r = 0; r < topic.replicationFactor; r++) {
          const brokerIndex = (p + r) % brokerIds.length;
          replicas.push(brokerIds[brokerIndex]);
        }
        topic.partitions.set(p, {
          partitionId: p,
          leaderBrokerId: replicas[0],
          replicas,
          isr: [...replicas],
          records: [],
          logStartOffset: 0,
          logEndOffset: 0,
          highWatermark: 0,
        });
      }
    } else if (clamped < currentCount) {
      for (let p = clamped; p < currentCount; p++) {
        topic.partitions.delete(p);
      }
    }
  }
}
