import { Kafka, logLevel } from 'kafkajs';

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092,localhost:9094,localhost:9096')
  .split(',')
  .map((b) => b.trim());

async function verifyKraftCluster() {
  console.log(`[P7-01] Connecting to KRaft 3-node cluster: ${BROKERS.join(', ')}...`);

  const kafka = new Kafka({
    clientId: 'kraft-cluster-verifier',
    brokers: BROKERS,
    logLevel: logLevel.ERROR,
  });

  const admin = kafka.admin();

  try {
    await admin.connect();
    console.log('[P7-01] Admin connected. Describing KRaft cluster topology...');

    const clusterInfo = await admin.describeCluster();
    console.log(`[P7-01] Cluster ID: ${clusterInfo.clusterId}`);
    console.log(`[P7-01] Controller Node ID: ${clusterInfo.controller}`);
    console.log(`[P7-01] Discovered Brokers (${clusterInfo.brokers.length}):`);
    for (const b of clusterInfo.brokers) {
      console.log(`       - Broker Node ${b.nodeId} (${b.host}:${b.port})`);
    }

    if (clusterInfo.brokers.length < 3) {
      throw new Error(
        `Expected at least 3 brokers in KRaft cluster, but found ${clusterInfo.brokers.length}`,
      );
    }

    const brokerNodeIds = clusterInfo.brokers.map((b) => b.nodeId).sort((a, b) => a - b);
    const expectedIds = [1, 2, 3];
    const matchesExpected = expectedIds.every((id) => brokerNodeIds.includes(id));

    if (!matchesExpected) {
      throw new Error(
        `Expected broker node IDs [1, 2, 3], but found: [${brokerNodeIds.join(', ')}]`,
      );
    }

    const result = {
      status: 'PASSED',
      phase: 'Phase 7 — P7-01',
      clusterId: clusterInfo.clusterId,
      controllerId: clusterInfo.controller,
      brokerCount: clusterInfo.brokers.length,
      brokers: clusterInfo.brokers,
      timestamp: new Date().toISOString(),
    };

    console.log('\n--- P7-01 KRAFT 3-NODE CLUSTER VERIFICATION PASSED ---');
    console.log(JSON.stringify(result, null, 2));
    await admin.disconnect();
    process.exit(0);
  } catch (err) {
    console.error(`[P7-01] Verification failed: ${err.message}`);
    await admin.disconnect().catch(() => {});
    process.exit(1);
  }
}

verifyKraftCluster();
