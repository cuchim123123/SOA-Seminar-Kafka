export type PartitionAssignorStrategy = 'round-robin' | 'range';

export interface SimConsumerInstance {
  id: string;
  groupId: string;
  assignedPartitions: number[];
  status: 'POLLING' | 'PROCESSING' | 'PAUSED' | 'IDLE' | 'REBALANCING' | 'CRASHED';
  processingDelayMs: number;
  currentProcessingPartition?: number;
  currentProcessingOffset?: number;
  deliveryId?: string;
}

export class SimConsumerGroupModel {
  public groupId: string;
  public members: Map<string, SimConsumerInstance> = new Map();
  public assignorStrategy: PartitionAssignorStrategy = 'round-robin';

  /**
   * committedNextOffset scoped by `topic:partition`
   * Next offset the consumer group will read.
   */
  public committedNextOffsets: Map<string, number> = new Map();

  /**
   * Tracks partitions currently being processed to enforce strict single-partition ordering.
   */
  public inFlightPartitions: Set<number> = new Set();

  constructor(groupId: string = 'loyalty-v1', strategy: PartitionAssignorStrategy = 'round-robin') {
    this.groupId = groupId;
    this.assignorStrategy = strategy;
  }

  public registerConsumer(
    consumerId: string,
    processingDelayMs: number = 100,
  ): SimConsumerInstance {
    const instance: SimConsumerInstance = {
      id: consumerId,
      groupId: this.groupId,
      assignedPartitions: [],
      status: 'IDLE',
      processingDelayMs,
    };
    this.members.set(consumerId, instance);
    return instance;
  }

  public unregisterConsumer(consumerId: string): void {
    this.members.delete(consumerId);
  }

  public setAssignorStrategy(strategy: PartitionAssignorStrategy): void {
    this.assignorStrategy = strategy;
  }

  /**
   * Rebalances partitions among active consumer members using the selected strategy.
   * If consumer count > partition count, excess consumers remain IDLE.
   */
  public rebalance(totalPartitions: number): Map<string, number[]> {
    const onlineMemberIds = Array.from(this.members.values())
      .filter((m) => m.status !== 'CRASHED')
      .map((m) => m.id);

    const assignments = new Map<string, number[]>();

    for (const id of this.members.keys()) {
      assignments.set(id, []);
    }

    if (onlineMemberIds.length === 0 || totalPartitions === 0) {
      for (const member of this.members.values()) {
        member.assignedPartitions = [];
        if (member.status !== 'CRASHED' && member.status !== 'PAUSED') {
          member.status = 'IDLE';
        }
      }
      return assignments;
    }

    if (this.assignorStrategy === 'round-robin') {
      // Round-robin distribution
      for (let p = 0; p < totalPartitions; p++) {
        const targetMemberId = onlineMemberIds[p % onlineMemberIds.length];
        assignments.get(targetMemberId)?.push(p);
      }
    } else {
      // Range distribution: divide into contiguous segments
      const numMembers = onlineMemberIds.length;
      const minPerMember = Math.floor(totalPartitions / numMembers);
      const surplus = totalPartitions % numMembers;

      let currentPartition = 0;
      for (let i = 0; i < numMembers; i++) {
        const memberId = onlineMemberIds[i];
        const countForThisMember = minPerMember + (i < surplus ? 1 : 0);
        for (let j = 0; j < countForThisMember; j++) {
          assignments.get(memberId)?.push(currentPartition);
          currentPartition += 1;
        }
      }
    }

    // Apply to member instances
    for (const [id, partitions] of assignments.entries()) {
      const member = this.members.get(id);
      if (member) {
        member.assignedPartitions = [...partitions];
        if (member.status !== 'CRASHED' && member.status !== 'PAUSED') {
          member.status = partitions.length > 0 ? 'POLLING' : 'IDLE';
        }
        if (partitions.length === 0) {
          member.currentProcessingPartition = undefined;
          member.currentProcessingOffset = undefined;
        }
      }
    }

    return assignments;
  }

  public crashMember(memberId: string, totalPartitions: number): Map<string, number[]> {
    const member = this.members.get(memberId);
    if (member) {
      if (member.currentProcessingPartition !== undefined) {
        this.unlockPartition(member.currentProcessingPartition);
      }
      for (const p of member.assignedPartitions) {
        this.unlockPartition(p);
      }
      member.status = 'CRASHED';
      member.deliveryId = undefined;
      member.assignedPartitions = [];
      member.currentProcessingPartition = undefined;
      member.currentProcessingOffset = undefined;
    }
    return this.rebalance(totalPartitions);
  }

  public reviveMember(memberId: string, totalPartitions: number): Map<string, number[]> {
    const member = this.members.get(memberId);
    if (member) {
      member.status = 'POLLING';
    }
    return this.rebalance(totalPartitions);
  }

  public commitOffset(topic: string, partition: number, offset: number): void {
    const key = `${topic}:${partition}`;
    // Committing consumed offset N sets next read offset to N + 1
    this.committedNextOffsets.set(key, offset + 1);
    this.inFlightPartitions.delete(partition);
  }

  public hasCommittedOffset(topic: string, partition: number): boolean {
    return this.committedNextOffsets.has(`${topic}:${partition}`);
  }

  /** Sets the next offset to read (committed next offset) without releasing the partition lock. */
  public seekNextOffset(topic: string, partition: number, nextOffset: number): void {
    this.committedNextOffsets.set(`${topic}:${partition}`, Math.max(0, nextOffset));
  }

  public getCommittedNextOffset(topic: string, partition: number): number {
    const key = `${topic}:${partition}`;
    return this.committedNextOffsets.get(key) ?? 0;
  }

  public calculateLag(topic: string, partition: number, logEndOffset: number): number {
    const committed = this.getCommittedNextOffset(topic, partition);
    return Math.max(0, logEndOffset - committed);
  }

  public isPartitionInFlight(partition: number): boolean {
    return this.inFlightPartitions.has(partition);
  }

  public lockPartition(partition: number): void {
    this.inFlightPartitions.add(partition);
  }

  public unlockPartition(partition: number): void {
    this.inFlightPartitions.delete(partition);
  }

  public unlockAllPartitions(): void {
    this.inFlightPartitions.clear();
  }
}
