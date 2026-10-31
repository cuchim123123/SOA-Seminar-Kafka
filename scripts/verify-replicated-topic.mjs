import { Kafka, logLevel, CompressionTypes } from 'kafkajs';

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9094,localhost:9096')
  .split(',')
  .map((b) => b.trim());

const TOPIC = process.env.TOPIC || 'purchase.events';

async function verifyReplicatedTopic() {
  console.log(`[P7-02] Verifying replicated topic '${TOPIC}' on brokers: ${BROKERS.join(', ')}...`);

  const kafka = new Kafka({
    clientId: 'replicated-topic-verifier',
    brokers: BROKERS,
    logLevel: logLevel.ERROR,
  });

  const admin = kafka.admin();
  const producer = kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
  });

  try {
    await admin.connect();
    await producer.connect();

    // Ensure topic exists with 6 partitions, RF 3, min.insync.replicas 2
    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes(TOPIC)) {
      console.log(
        `[P7-02] Topic '${TOPIC}' not found. Creating with 6 partitions, RF 3, minISR 2...`,
      );
      await admin.createTopics({
        topics: [
          {
            topic: TOPIC,
            numPartitions: 6,
            replicationFactor: 3,
            configEntries: [
              { name: 'min.insync.replicas', value: '2' },
              { name: 'cleanup.policy', value: 'delete' },
            ],
          },
        ],
      });
      console.log(`[P7-02] Topic '${TOPIC}' created.`);
    }

    // Inspect metadata
    const metadata = await admin.fetchTopicMetadata({ topics: [TOPIC] });
    const topicMeta = metadata.topics.find((t) => t.name === TOPIC);

    if (!topicMeta) {
      throw new Error(`Topic '${TOPIC}' metadata not returned by broker.`);
    }

    console.log(`[P7-02] Topic '${TOPIC}' has ${topicMeta.partitions.length} partitions.`);

    const partitionReports = [];
    for (const p of topicMeta.partitions) {
      console.log(
        `       - P${p.partitionId}: Leader=${p.leader}, Replicas=[${p.replicas.join(', ')}], ISR=[${p.isr.join(', ')}]`,
      );

      if (p.replicas.length < 3) {
        throw new Error(
          `Partition ${p.partitionId} has replication factor ${p.replicas.length}, expected 3.`,
        );
      }
      if (p.isr.length < 2) {
        throw new Error(
          `Partition ${p.partitionId} has ISR size ${p.isr.length}, expected at least minISR=2.`,
        );
      }

      partitionReports.push({
        partitionId: p.partitionId,
        leader: p.leader,
        replicas: p.replicas,
        isr: p.isr,
        isMinIsrSatisfied: p.isr.length >= 2,
      });
    }

    // Producer test with acks = all (-1)
    console.log(
      '[P7-02] Publishing 6 test records with acks=all (strict ISR quorum confirmation)...',
    );
    const sendResults = [];
    for (let i = 0; i < 6; i++) {
      const res = await producer.send({
        topic: TOPIC,
        acks: -1, // acks = all
        compression: CompressionTypes.None,
        messages: [
          {
            key: `CUSTOMER_P7_${i}`,
            value: JSON.stringify({
              eventType: 'PURCHASE_COMPLETED',
              payload: {
                testRun: 'P7-02',
                partitionTarget: i,
                amountCents: 1000 + i * 100,
                occurredAt: new Date().toISOString(),
              },
            }),
          },
        ],
      });
      sendResults.push(res[0]);
    }

    console.log(`[P7-02] Successfully published 6 records with acks=all.`);

    const result = {
      status: 'PASSED',
      phase: 'Phase 7 — P7-02',
      topic: TOPIC,
      partitionCount: topicMeta.partitions.length,
      replicationFactor: 3,
      minInsyncReplicas: 2,
      partitions: partitionReports,
      verifiedWrites: sendResults.map((r) => ({
        topic: r.topicName,
        partition: r.partition,
        offset: r.baseOffset,
      })),
      timestamp: new Date().toISOString(),
    };

    console.log('\n--- P7-02 REPLICATED TOPIC & PRODUCER ACKS VERIFICATION PASSED ---');
    console.log(JSON.stringify(result, null, 2));

    await producer.disconnect();
    await admin.disconnect();
    process.exit(0);
  } catch (err) {
    console.error(`[P7-02] Verification failed: ${err.message}`);
    await producer.disconnect().catch(() => {});
    await admin.disconnect().catch(() => {});
    process.exit(1);
  }
}

verifyReplicatedTopic();
