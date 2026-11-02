export const SIMULATION_LIMITS = {
  messagesPerRun: 10_000,
  messagesPerBurst: 100,
  components: 100,
  wires: 400,
  replicasPerGroup: 12,
  retainedEvents: 100,
  particlesPerWire: 6,
  partitionRowsPerNode: 6,
  topics: 24,
  partitionsPerTopic: 12,
} as const;
