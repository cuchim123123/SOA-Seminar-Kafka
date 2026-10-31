import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { StructuredLogger } from '@lab/observability';
import { Admin, AssignerProtocol, Consumer, Kafka, Producer } from 'kafkajs';
import { AppConfigService } from '../../../config/app-config.service';
import {
  BrokerMessageRecord,
  ConsumerGroupDescriptionResult,
  ConsumerGroupLagResult,
  ConsumerGroupMemberDescription,
  StreamInspectorPort,
  PartitionLag,
  PartitionPosition,
  TopicInspectionResult,
} from '../../application/ports/stream-inspector.port';

@Injectable()
export class KafkaJsInspectorAdapter implements StreamInspectorPort, OnModuleInit, OnModuleDestroy {
  private readonly logger: StructuredLogger;
  private readonly kafka: Kafka;
  private readonly admin: Admin;
  private readonly producer: Producer;

  constructor(@Inject(AppConfigService) private readonly config: AppConfigService) {
    this.logger = new StructuredLogger(KafkaJsInspectorAdapter.name, {
      serviceName: 'producer-api',
      format: this.config.nodeEnv === 'production' ? 'json' : 'pretty',
    });
    this.kafka = new Kafka({
      clientId: `${this.config.kafkaClientId}-inspector`,
      brokers: this.config.kafkaBrokers,
    });
    this.admin = this.kafka.admin();
    this.producer = this.kafka.producer();
  }

  async onModuleInit(): Promise<void> {
    this.logger.info('Connecting KafkaJS admin and producer client for ops inspection...');
    await Promise.all([this.admin.connect(), this.producer.connect()]);
    this.logger.info('KafkaJS admin and producer client connected');
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.info('Disconnecting KafkaJS admin and producer client...');
    await Promise.all([this.admin.disconnect(), this.producer.disconnect()]);
    this.logger.info('KafkaJS admin and producer client disconnected');
  }

  async inspectTopic(topic: string): Promise<TopicInspectionResult> {
    const metadata = await this.admin.fetchTopicMetadata({ topics: [topic] });
    const topicInfo = metadata.topics.find((t) => t.name === topic);

    if (!topicInfo) {
      throw new Error(`Topic '${topic}' not found on Kafka broker cluster`);
    }

    const offsets = await this.admin.fetchTopicOffsets(topic);

    const partitions: PartitionPosition[] = topicInfo.partitions.map((p) => {
      const offsetInfo = offsets.find((o) => o.partition === p.partitionId);
      return {
        topic,
        partition: p.partitionId,
        leader: p.leader,
        replicas: p.replicas,
        isr: p.isr,
        logStartOffset: offsetInfo?.low ?? '0',
        logEndOffset: offsetInfo?.high ?? '0',
      };
    });

    return {
      topic,
      partitionCount: partitions.length,
      partitions,
    };
  }

  async scaleTopicPartitions(
    topic: string,
    newPartitionCount: number,
  ): Promise<TopicInspectionResult> {
    const current = await this.inspectTopic(topic);
    if (newPartitionCount <= current.partitionCount) {
      throw new Error(
        `Topic '${topic}' currently has ${current.partitionCount} partitions. Kafka only supports scaling partitions upward (requested: ${newPartitionCount}).`,
      );
    }

    this.logger.info('Scaling topic partitions on broker cluster...', {
      topic,
      currentCount: current.partitionCount,
      newCount: newPartitionCount,
    });

    await this.admin.createPartitions({
      topicPartitions: [{ topic, count: newPartitionCount }],
    });

    this.logger.info(
      'Successfully requested partition creation on broker, waiting for KRaft leader assignment...',
      { topic, newCount: newPartitionCount },
    );

    // Allow KRaft leader election and partition log initialization to settle
    await new Promise((resolve) => setTimeout(resolve, 500));

    let attempts = 0;
    while (attempts < 3) {
      try {
        return await this.inspectTopic(topic);
      } catch (err) {
        attempts++;
        if (attempts >= 3) throw err;
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    return this.inspectTopic(topic);
  }

  async peekPartitionMessages(
    topic: string,
    partition: number,
    limit: number = 10,
  ): Promise<BrokerMessageRecord[]> {
    const offsets = await this.admin.fetchTopicOffsets(topic);
    const partOffset = offsets.find((o) => o.partition === partition);

    if (!partOffset) {
      throw new Error(`Partition #${partition} does not exist for topic '${topic}'`);
    }

    const low = BigInt(partOffset.low);
    const high = BigInt(partOffset.high);

    if (high <= low) {
      return [];
    }

    const maxRecords = BigInt(Math.max(1, Math.min(50, limit)));
    const startBigInt = high - maxRecords > low ? high - maxRecords : low;
    const startOffset = startBigInt.toString();

    const consumer: Consumer = this.kafka.consumer({
      groupId: `peeker-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      allowAutoTopicCreation: false,
      maxWaitTimeInMs: 100,
    });

    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    consumer.on(consumer.events.GROUP_JOIN, () => {
      consumer.seek({ topic, partition, offset: startOffset });
    });

    const messages: BrokerMessageRecord[] = [];

    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => resolve(), 2500);

      consumer
        .run({
          autoCommit: false,
          eachMessage: async ({ partition: msgPart, message }) => {
            if (msgPart === partition) {
              let payload: unknown = null;
              try {
                payload = message.value ? JSON.parse(message.value.toString('utf-8')) : null;
              } catch {
                payload = message.value ? message.value.toString('utf-8') : null;
              }

              messages.push({
                topic,
                partition: msgPart,
                offset: message.offset,
                key: message.key ? message.key.toString('utf-8') : null,
                timestamp: message.timestamp,
                payload,
              });

              if (messages.length >= limit || BigInt(message.offset) + 1n >= high) {
                clearTimeout(timer);
                resolve();
              }
            }
          },
        })
        .catch(() => {
          clearTimeout(timer);
          resolve();
        });
    });

    await consumer.disconnect().catch(() => {});
    return messages.reverse();
  }

  private calculatePartitionLag(
    topic: string,
    topicOffset: { partition: number; low: string; high: string },
    committedRaw?: string,
  ): { lagRecord: PartitionLag; lagBigInt: bigint; isUncommitted: boolean } {
    const low = BigInt(topicOffset.low);
    const high = BigInt(topicOffset.high);
    const hasNoCommittedOffset = committedRaw === undefined || committedRaw === '-1';

    if (hasNoCommittedOffset) {
      const isUncommitted = high > low;
      return {
        lagRecord: {
          topic,
          partition: topicOffset.partition,
          logStartOffset: low.toString(),
          logEndOffset: high.toString(),
          committedOffset: null,
          lag: isUncommitted ? 'UNCOMMITTED' : '0',
          isUncommitted,
        },
        lagBigInt: 0n,
        isUncommitted,
      };
    }

    const committedBigInt = BigInt(committedRaw);
    const lagBigInt = high > committedBigInt ? high - committedBigInt : 0n;

    return {
      lagRecord: {
        topic,
        partition: topicOffset.partition,
        logStartOffset: low.toString(),
        logEndOffset: high.toString(),
        committedOffset: committedRaw,
        lag: lagBigInt.toString(),
        isUncommitted: false,
      },
      lagBigInt,
      isUncommitted: false,
    };
  }

  async inspectConsumerGroupLag(
    groupId: string,
    topic: string = this.config.purchaseTopic,
  ): Promise<ConsumerGroupLagResult> {
    const topicOffsets = await this.admin.fetchTopicOffsets(topic);

    let groupPartitions: Array<{ partition: number; offset: string; metadata: string | null }> = [];

    try {
      const fetched = await this.admin.fetchOffsets({ groupId, topics: [topic] });
      const topicEntry = fetched.find((f) => f.topic === topic);
      if (topicEntry) {
        groupPartitions = topicEntry.partitions;
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.warn(`Could not fetch committed offsets for consumer group '${groupId}': ${msg}`);
    }

    const groupOffsetMap = new Map<number, string>(
      groupPartitions.map((gp) => [gp.partition, gp.offset]),
    );

    let hasUncommitted = false;
    let totalLagBigInt = 0n;

    const partitions: PartitionLag[] = topicOffsets.map((to) => {
      const { lagRecord, lagBigInt, isUncommitted } = this.calculatePartitionLag(
        topic,
        to,
        groupOffsetMap.get(to.partition),
      );

      if (isUncommitted) hasUncommitted = true;
      totalLagBigInt += lagBigInt;

      return lagRecord;
    });

    return {
      groupId,
      topic,
      totalLag: hasUncommitted ? 'UNCOMMITTED' : totalLagBigInt.toString(),
      partitions,
    };
  }

  async describeConsumerGroup(
    groupId: string,
    topic: string = this.config.purchaseTopic,
  ): Promise<ConsumerGroupDescriptionResult> {
    const res = await this.admin.describeGroups([groupId]);
    const group = res.groups.find((g) => g.groupId === groupId);
    if (!group) {
      throw new Error(`Consumer group '${groupId}' not found on Kafka broker`);
    }

    const members: ConsumerGroupMemberDescription[] = group.members.map((m) => {
      let assignedPartitions: number[] = [];
      try {
        if (m.memberAssignment && m.memberAssignment.length > 0) {
          const decoded = AssignerProtocol.MemberAssignment.decode(m.memberAssignment);
          if (decoded && decoded.assignment) {
            assignedPartitions = (decoded.assignment[topic] ?? []).slice().sort((a, b) => a - b);
          }
        }
      } catch (err: unknown) {
        this.logger.warn(`Failed to decode member assignment for ${m.memberId}: ${String(err)}`);
      }

      return {
        memberId: m.memberId,
        clientId: m.clientId,
        clientHost: m.clientHost,
        assignedPartitions,
      };
    });

    return {
      groupId: group.groupId,
      state: group.state,
      protocol: group.protocol,
      protocolType: group.protocolType,
      members,
    };
  }

  async publishRawMessage(
    topic: string,
    key: string,
    payload: string,
  ): Promise<{ topic: string; partition: number; offset: string }> {
    const records = await this.producer.send({
      topic,
      messages: [{ key, value: payload }],
    });
    const first = records[0];
    return {
      topic,
      partition: first.partition,
      offset: first.baseOffset ?? '0',
    };
  }
}
