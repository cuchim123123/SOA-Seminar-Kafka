import React, { useState, useEffect, useMemo, useCallback } from 'react';
import type { TopicPartitionMetadata, BrokerMessageRecord } from '../api/client';
import { api } from '../api/client';
import { computeKafkaPartition } from '../utils/kafka-partitioner';

interface PartitionGridProps {
  metadata: TopicPartitionMetadata | null;
  loading: boolean;
  highlightedPartition?: number | null;
  onMetadataChanged?: () => void;
}

export const PartitionGrid: React.FC<PartitionGridProps> = ({
  metadata,
  loading,
  highlightedPartition: externalHighlight,
  onMetadataChanged,
}) => {
  const partitions = metadata?.partitions || [];
  const partitionCount = metadata?.partitionCount || partitions.length || 6;
  const topicName = metadata?.topic || 'purchase.events';

  // 1. Interactive Murmur2 Simulator State
  const [simulatorKey, setSimulatorKey] = useState<string>('U001');

  // 2. Partition Selection & Live Broker Inspection State
  const [selectedPartition, setSelectedPartition] = useState<number>(0);
  const [messages, setMessages] = useState<BrokerMessageRecord[]>([]);
  const [peeking, setPeeking] = useState<boolean>(false);
  const [peekLimit, setPeekLimit] = useState<number>(10);
  const [autoPeek, setAutoPeek] = useState<boolean>(false);
  const [peekError, setPeekError] = useState<string | null>(null);
  const [expandedOffset, setExpandedOffset] = useState<string | null>(null);
  const [messageSearch, setMessageSearch] = useState<string>('');

  // 3. Partition Scaling State
  const [showScaleModal, setShowScaleModal] = useState<boolean>(false);
  const [targetPartitionCount, setTargetPartitionCount] = useState<number>(partitionCount + 2);
  const [scaling, setScaling] = useState<boolean>(false);
  const [scaleError, setScaleError] = useState<string | null>(null);
  const [scaleSuccess, setScaleSuccess] = useState<string | null>(null);

  // Sync target partition count when metadata updates
  useEffect(() => {
    if (partitionCount) {
      setTargetPartitionCount(partitionCount + 2);
    }
  }, [partitionCount]);

  // Compute Murmur2 partition mapping
  const hashResult = useMemo(() => {
    return computeKafkaPartition(simulatorKey.trim() || 'U001', partitionCount);
  }, [simulatorKey, partitionCount]);

  // Highlighted partition: internal hash simulation takes precedence, then external prop
  const activeHighlight = simulatorKey.trim() ? hashResult.targetPartition : externalHighlight;

  // Total messages across topic
  const totalMessagesAcrossTopic = useMemo(() => {
    return partitions.reduce((sum, p) => sum + BigInt(p.messageCount || '0'), 0n);
  }, [partitions]);

  // Fetch live messages from broker for selected partition
  const fetchBrokerMessages = useCallback(
    async (partId: number, limit: number) => {
      setPeeking(true);
      setPeekError(null);
      try {
        const records = await api.peekPartitionMessages(topicName, partId, limit);
        setMessages(records);
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        setPeekError(msg);
      } finally {
        setPeeking(false);
      }
    },
    [topicName],
  );

  // Fetch messages when selected partition or limit changes
  useEffect(() => {
    fetchBrokerMessages(selectedPartition, peekLimit);
  }, [selectedPartition, peekLimit, fetchBrokerMessages]);

  // Auto-peek interval
  useEffect(() => {
    if (!autoPeek) return;
    const timer = setInterval(() => {
      fetchBrokerMessages(selectedPartition, peekLimit);
    }, 3000);
    return () => clearInterval(timer);
  }, [autoPeek, selectedPartition, peekLimit, fetchBrokerMessages]);

  // Handle Partition Scale
  const handleScalePartitions = async (e: React.FormEvent) => {
    e.preventDefault();
    if (targetPartitionCount <= partitionCount) {
      setScaleError(`New partition count (${targetPartitionCount}) must be greater than current count (${partitionCount}). Kafka does not allow decreasing partitions.`);
      return;
    }

    setScaling(true);
    setScaleError(null);
    setScaleSuccess(null);
    try {
      await api.scaleTopicPartitions(topicName, targetPartitionCount);
      setScaleSuccess(`Successfully scaled ${topicName} to ${targetPartitionCount} partitions!`);
      if (onMetadataChanged) {
        onMetadataChanged();
      }
      setTimeout(() => {
        setShowScaleModal(false);
        setScaleSuccess(null);
      }, 1500);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setScaleError(msg);
    } finally {
      setScaling(false);
    }
  };

  // Filter messages
  const filteredMessages = useMemo(() => {
    if (!messageSearch.trim()) return messages;
    const q = messageSearch.toLowerCase();
    return messages.filter((m) => {
      const keyMatch = m.key?.toLowerCase().includes(q);
      const offsetMatch = m.offset.includes(q);
      const payloadStr = JSON.stringify(m.payload || '').toLowerCase();
      return keyMatch || offsetMatch || payloadStr.includes(q);
    });
  }, [messages, messageSearch]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* 1. Header & Quick Actions */}
      <section className="cyber-card" style={{ padding: '20px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <h2 style={{ fontSize: '1.15rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ color: 'var(--color-cyan)' }}>❖</span>
                Kafka Partition Management &amp; Live Broker Inspector
              </h2>
              <span className="badge-mono badge-cyan">{topicName}</span>
            </div>
            <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem', marginTop: '4px' }}>
              Direct KRaft broker telemetry. Inspect live Kafka log positions, peek raw broker messages, and scale topic partitions dynamically.
            </p>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
            <span className="badge-mono badge-violet">
              {partitionCount} Partitions
            </span>
            <span className="badge-mono badge-emerald">
              {totalMessagesAcrossTopic.toString()} Total Records
            </span>
            <button
              type="button"
              onClick={() => setShowScaleModal(true)}
              style={{
                background: 'linear-gradient(135deg, #0284c7 0%, #2563eb 100%)',
                color: '#ffffff',
                border: 'none',
                borderRadius: 'var(--radius-sm)',
                padding: '6px 14px',
                fontSize: '0.8rem',
                fontWeight: 600,
                cursor: 'pointer',
                display: 'inline-flex',
                alignItems: 'center',
                gap: '6px',
                boxShadow: 'var(--shadow-sm)',
              }}
            >
              <span>⚙</span> Scale Partitions
            </button>
          </div>
        </div>

        {/* 2. Interactive Murmur2 Key Partitioner Simulator */}
        <div
          style={{
            marginTop: '16px',
            padding: '14px 18px',
            borderRadius: 'var(--radius-md)',
            background: 'var(--bg-subtle)',
            border: '1px solid var(--border-subtle)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '8px', marginBottom: '10px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ fontSize: '0.9rem', color: 'var(--color-violet)', fontWeight: 700 }}>⚡</span>
              <span style={{ fontSize: '0.85rem', fontWeight: 700, color: 'var(--text-main)' }}>
                Murmur2 Key Hashing Simulator (Kafka Default Partitioner)
              </span>
            </div>
            <div style={{ display: 'flex', gap: '6px' }}>
              {['U001', 'U002', 'VIP_001', 'ALICE', 'BOB'].map((preset) => (
                <button
                  key={preset}
                  type="button"
                  onClick={() => {
                    setSimulatorKey(preset);
                    const res = computeKafkaPartition(preset, partitionCount);
                    setSelectedPartition(res.targetPartition);
                  }}
                  style={{
                    fontSize: '0.75rem',
                    fontFamily: 'var(--font-mono)',
                    padding: '2px 8px',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid var(--border-medium)',
                    background: simulatorKey === preset ? '#e0f2fe' : '#ffffff',
                    color: simulatorKey === preset ? '#0284c7' : 'var(--text-body)',
                    cursor: 'pointer',
                    fontWeight: 600,
                  }}
                >
                  {preset}
                </button>
              ))}
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: '12px', alignItems: 'center' }}>
            <div>
              <label htmlFor="partition-key-input" style={{ display: 'block', fontSize: '0.75rem', color: 'var(--text-muted)', marginBottom: '4px', fontWeight: 600 }}>
                Test Key / Customer ID:
              </label>
              <input
                id="partition-key-input"
                type="text"
                value={simulatorKey}
                onChange={(e) => {
                  const val = e.target.value;
                  setSimulatorKey(val);
                  if (val.trim()) {
                    const res = computeKafkaPartition(val.trim(), partitionCount);
                    setSelectedPartition(res.targetPartition);
                  }
                }}
                placeholder="Type customer ID (e.g. U001)..."
                style={{
                  width: '100%',
                  padding: '7px 12px',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid var(--border-medium)',
                  fontFamily: 'var(--font-mono)',
                  fontSize: '0.85rem',
                  background: '#ffffff',
                }}
              />
            </div>

            <div style={{ background: '#ffffff', padding: '10px 14px', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)', fontSize: '0.785rem', fontFamily: 'var(--font-mono)' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '3px' }}>
                <span style={{ color: 'var(--text-muted)' }}>Murmur2 Hash:</span>
                <span style={{ color: 'var(--text-main)', fontWeight: 600 }}>{hashResult.hexHash}</span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '3px' }}>
                <span style={{ color: 'var(--text-muted)' }}>Positive (31-bit):</span>
                <span style={{ color: 'var(--text-main)', fontWeight: 600 }}>{hashResult.positiveHash.toLocaleString()}</span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', borderTop: '1px solid var(--border-subtle)', paddingTop: '4px' }}>
                <span style={{ color: 'var(--text-muted)' }}>Target Partition:</span>
                <span style={{ color: '#0284c7', fontWeight: 700, fontSize: '0.85rem' }}>
                  Partition #{hashResult.targetPartition}
                </span>
              </div>
            </div>

            <div style={{ fontSize: '0.775rem', color: 'var(--text-muted)', lineHeight: '1.4' }}>
              <strong style={{ color: 'var(--text-body)' }}>Deterministic Guarantees:</strong> Any message produced with key <code style={{ fontFamily: 'var(--font-mono)', color: '#0284c7', fontWeight: 600 }}>{simulatorKey || '...'}</code> is guaranteed to land in <strong style={{ color: '#0284c7' }}>Partition #{hashResult.targetPartition}</strong>.
            </div>
          </div>
        </div>
      </section>

      {/* 3. Partition Cards Matrix */}
      <section className="cyber-card" style={{ padding: '20px 24px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '14px', flexWrap: 'wrap', gap: '8px' }}>
          <div>
            <h3 style={{ fontSize: '0.95rem', fontWeight: 700, color: 'var(--text-main)' }}>
              Partition Matrix ({partitionCount} Partitions)
            </h3>
            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
              Click any partition card below to inspect its live log offsets and stream messages.
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
              <span style={{ width: '10px', height: '10px', borderRadius: '2px', background: '#0284c7', display: 'inline-block' }} />
              Active Target
            </span>
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
              <span style={{ width: '10px', height: '10px', borderRadius: '2px', background: '#e0f2fe', border: '1px solid #0284c7', display: 'inline-block' }} />
              Selected for Peek
            </span>
          </div>
        </div>

        {loading && !metadata ? (
          <div style={{ padding: '28px', textAlign: 'center', color: 'var(--text-dim)', fontFamily: 'var(--font-mono)', fontSize: '0.85rem' }}>
            Reading broker partition log end offsets...
          </div>
        ) : (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(190px, 1fr))', gap: '12px' }}>
            {partitions.map((p) => {
              const isTarget = activeHighlight === p.partition;
              const isSelected = selectedPartition === p.partition;
              const msgCount = BigInt(p.messageCount || '0');
              const highWatermark = BigInt(p.highWatermark || '0');

              return (
                <div
                  key={p.partition}
                  onClick={() => setSelectedPartition(p.partition)}
                  style={{
                    padding: '12px 14px',
                    borderRadius: 'var(--radius-md)',
                    background: isSelected ? '#f0f9ff' : '#ffffff',
                    border: isTarget
                      ? '2px solid #0284c7'
                      : isSelected
                      ? '1px solid #0284c7'
                      : '1px solid var(--border-subtle)',
                    boxShadow: isTarget ? '0 0 0 3px rgba(2, 132, 199, 0.15)' : 'var(--shadow-sm)',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease',
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
                    <span
                      className={`badge-mono ${isSelected ? 'badge-cyan' : 'badge-violet'}`}
                      style={{ fontSize: '0.75rem' }}
                    >
                      Partition #{p.partition}
                    </span>
                    {isTarget && (
                      <span className="badge-mono badge-cyan" style={{ fontSize: '0.65rem' }}>
                        Target Key
                      </span>
                    )}
                  </div>

                  <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span style={{ color: 'var(--text-muted)' }}>Start Offset:</span>
                      <span style={{ color: 'var(--text-body)', fontWeight: 600 }}>{p.logStartOffset}</span>
                    </div>

                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span style={{ color: 'var(--text-muted)' }}>High-Watermark:</span>
                      <span style={{ color: 'var(--color-emerald)', fontWeight: 600 }}>{p.highWatermark}</span>
                    </div>

                    <div style={{ display: 'flex', justifyContent: 'space-between', borderTop: '1px solid var(--border-subtle)', paddingTop: '4px', marginTop: '2px' }}>
                      <span style={{ color: 'var(--text-muted)' }}>Messages:</span>
                      <span style={{ color: 'var(--color-violet)', fontWeight: 700 }}>
                        {msgCount.toString()}
                      </span>
                    </div>
                  </div>

                  {/* Offset Progress Indicator */}
                  <div
                    style={{
                      height: '4px',
                      width: '100%',
                      background: '#f1f5f9',
                      borderRadius: 'var(--radius-full)',
                      marginTop: '8px',
                      overflow: 'hidden',
                    }}
                  >
                    <div
                      style={{
                        height: '100%',
                        width: `${Math.min(100, Math.max(6, Number(highWatermark % 100n)))}%`,
                        background: isTarget ? '#0284c7' : isSelected ? '#3b82f6' : '#94a3b8',
                        borderRadius: 'var(--radius-full)',
                        transition: 'width 0.3s ease',
                      }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </section>

      {/* 4. Live Broker Message Viewer (Peeker) */}
      <section className="cyber-card" style={{ padding: '20px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '12px', marginBottom: '16px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <h3 style={{ fontSize: '1.05rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '6px' }}>
                <span>🔍</span> Live Broker Records — Partition #{selectedPartition}
              </h3>
              <span className="badge-mono badge-cyan">
                {messages.length} Records Loaded
              </span>
            </div>
            <p style={{ color: 'var(--text-muted)', fontSize: '0.8rem', marginTop: '2px' }}>
              Peeking the newest records directly from Kafka partition log without altering consumer group committed offsets.
            </p>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
            {/* Search Filter */}
            <input
              type="text"
              value={messageSearch}
              onChange={(e) => setMessageSearch(e.target.value)}
              placeholder="Filter by key, ID, event..."
              style={{
                padding: '5px 10px',
                borderRadius: 'var(--radius-sm)',
                border: '1px solid var(--border-medium)',
                fontFamily: 'var(--font-mono)',
                fontSize: '0.785rem',
                minWidth: '180px',
              }}
            />

            {/* Limit Selector */}
            <select
              aria-label="Message Limit Selector"
              value={peekLimit}
              onChange={(e) => setPeekLimit(Number(e.target.value))}
              style={{
                padding: '5px 8px',
                borderRadius: 'var(--radius-sm)',
                border: '1px solid var(--border-medium)',
                fontFamily: 'var(--font-mono)',
                fontSize: '0.785rem',
                background: '#ffffff',
              }}
            >
              <option value={5}>Limit: 5</option>
              <option value={10}>Limit: 10</option>
              <option value={25}>Limit: 25</option>
              <option value={50}>Limit: 50</option>
            </select>

            {/* Auto-peek toggle */}
            <label style={{ display: 'inline-flex', alignItems: 'center', gap: '4px', fontSize: '0.775rem', color: 'var(--text-body)', cursor: 'pointer', userSelect: 'none' }}>
              <input
                type="checkbox"
                checked={autoPeek}
                onChange={(e) => setAutoPeek(e.target.checked)}
              />
              Live Watch (3s)
            </label>

            {/* Refresh Button */}
            <button
              type="button"
              disabled={peeking}
              onClick={() => fetchBrokerMessages(selectedPartition, peekLimit)}
              style={{
                padding: '5px 12px',
                borderRadius: 'var(--radius-sm)',
                border: '1px solid var(--border-medium)',
                background: '#ffffff',
                color: 'var(--text-main)',
                fontSize: '0.785rem',
                fontWeight: 600,
                cursor: peeking ? 'not-allowed' : 'pointer',
                display: 'inline-flex',
                alignItems: 'center',
                gap: '4px',
              }}
            >
              {peeking ? 'Peeking...' : '↻ Peek Now'}
            </button>
          </div>
        </div>

        {peekError && (
          <div style={{ padding: '10px 14px', borderRadius: 'var(--radius-sm)', background: 'var(--color-crimson-bg)', border: '1px solid var(--color-crimson-border)', color: 'var(--color-crimson)', fontSize: '0.8rem', marginBottom: '14px' }}>
            Failed to peek partition messages: {peekError}
          </div>
        )}

        {filteredMessages.length === 0 ? (
          <div style={{ padding: '36px', textAlign: 'center', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', fontSize: '0.825rem', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-md)' }}>
            {peeking ? 'Fetching broker partition records...' : `No messages found in Partition #${selectedPartition} log.`}
          </div>
        ) : (
          <div style={{ overflowX: 'auto', border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-md)' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem', textAlign: 'left', fontFamily: 'var(--font-sans)' }}>
              <thead>
                <tr style={{ background: 'var(--bg-subtle)', borderBottom: '1px solid var(--border-subtle)', color: 'var(--text-muted)', fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                  <th style={{ padding: '10px 14px' }}>Offset</th>
                  <th style={{ padding: '10px 14px' }}>Key (CustomerId)</th>
                  <th style={{ padding: '10px 14px' }}>Event Type</th>
                  <th style={{ padding: '10px 14px' }}>Timestamp</th>
                  <th style={{ padding: '10px 14px' }}>Payload Summary</th>
                  <th style={{ padding: '10px 14px', textAlign: 'right' }}>Action</th>
                </tr>
              </thead>
              <tbody>
                {filteredMessages.map((msg) => {
                  const payloadObj = typeof msg.payload === 'object' && msg.payload !== null ? (msg.payload as Record<string, unknown>) : null;
                  const eventType = payloadObj?.eventType ? String(payloadObj.eventType) : 'RawMessage';
                  const customerSeq = payloadObj?.customerSequence !== undefined ? String(payloadObj.customerSequence) : '-';
                  const isExpanded = expandedOffset === msg.offset;

                  let timeDisplay = msg.timestamp;
                  try {
                    const d = new Date(Number(msg.timestamp));
                    timeDisplay = !isNaN(d.getTime()) ? d.toLocaleTimeString() : msg.timestamp;
                  } catch {
                    // ignore
                  }

                  return (
                    <React.Fragment key={msg.offset}>
                      <tr
                        style={{
                          borderBottom: '1px solid var(--border-subtle)',
                          background: isExpanded ? '#f8fafc' : '#ffffff',
                          transition: 'background 0.15s ease',
                        }}
                      >
                        <td style={{ padding: '10px 14px', fontFamily: 'var(--font-mono)', fontWeight: 600, color: 'var(--color-primary)' }}>
                          <span className="badge-mono badge-cyan">#{msg.offset}</span>
                        </td>
                        <td style={{ padding: '10px 14px', fontFamily: 'var(--font-mono)', fontWeight: 600, color: 'var(--text-main)' }}>
                          {msg.key ? (
                            <span style={{ color: '#0284c7' }}>{msg.key}</span>
                          ) : (
                            <span style={{ color: 'var(--text-dim)' }}>[Null Key]</span>
                          )}
                        </td>
                        <td style={{ padding: '10px 14px' }}>
                          <span className={`badge-mono ${eventType.includes('Invalid') ? 'badge-amber' : 'badge-emerald'}`}>
                            {eventType}
                          </span>
                        </td>
                        <td style={{ padding: '10px 14px', color: 'var(--text-muted)', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
                          {timeDisplay}
                        </td>
                        <td style={{ padding: '10px 14px', color: 'var(--text-body)', fontSize: '0.775rem' }}>
                          {payloadObj ? (
                            <span>
                              Seq <strong style={{ fontFamily: 'var(--font-mono)' }}>#{customerSeq}</strong>
                              {payloadObj.payload && typeof payloadObj.payload === 'object' ? (
                                <span style={{ color: 'var(--text-muted)', marginLeft: '6px' }}>
                                  ({(payloadObj.payload as Record<string, unknown>).amountCents ? `$${(Number((payloadObj.payload as Record<string, unknown>).amountCents) / 100).toFixed(2)}` : 'Payload'})
                                </span>
                              ) : null}
                            </span>
                          ) : (
                            <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-dim)' }}>
                              {String(msg.payload).slice(0, 40)}...
                            </span>
                          )}
                        </td>
                        <td style={{ padding: '10px 14px', textAlign: 'right' }}>
                          <button
                            type="button"
                            onClick={() => setExpandedOffset(isExpanded ? null : msg.offset)}
                            style={{
                              padding: '3px 8px',
                              borderRadius: 'var(--radius-sm)',
                              border: '1px solid var(--border-medium)',
                              background: '#ffffff',
                              color: 'var(--color-primary)',
                              fontSize: '0.725rem',
                              fontWeight: 600,
                              cursor: 'pointer',
                            }}
                          >
                            {isExpanded ? 'Hide JSON' : 'Inspect JSON'}
                          </button>
                        </td>
                      </tr>

                      {isExpanded && (
                        <tr style={{ background: '#f8fafc', borderBottom: '1px solid var(--border-subtle)' }}>
                          <td colSpan={6} style={{ padding: '12px 16px' }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                              <span style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                                FULL KAFKA ENVELOPE (RECORD #{msg.offset})
                              </span>
                              <button
                                type="button"
                                onClick={() => navigator.clipboard.writeText(JSON.stringify(msg.payload, null, 2))}
                                style={{
                                  fontSize: '0.7rem',
                                  padding: '2px 8px',
                                  borderRadius: 'var(--radius-sm)',
                                  border: '1px solid var(--border-medium)',
                                  background: '#ffffff',
                                  cursor: 'pointer',
                                }}
                              >
                                Copy JSON
                              </button>
                            </div>
                            <pre
                              style={{
                                margin: 0,
                                padding: '10px 12px',
                                background: '#ffffff',
                                border: '1px solid var(--border-subtle)',
                                borderRadius: 'var(--radius-sm)',
                                fontSize: '0.75rem',
                                fontFamily: 'var(--font-mono)',
                                color: 'var(--text-main)',
                                overflowX: 'auto',
                                maxHeight: '220px',
                              }}
                            >
                              {JSON.stringify(msg.payload, null, 2)}
                            </pre>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* 5. Scale Partitions Modal */}
      {showScaleModal && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(15, 23, 42, 0.4)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000,
            padding: '20px',
          }}
        >
          <div
            className="cyber-card"
            style={{
              width: '100%',
              maxWidth: '480px',
              padding: '24px',
              background: '#ffffff',
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '14px' }}>
              <h3 style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span>⚙</span> Scale Topic Partitions
              </h3>
              <button
                type="button"
                onClick={() => setShowScaleModal(false)}
                style={{
                  border: 'none',
                  background: 'transparent',
                  color: 'var(--text-muted)',
                  fontSize: '1.2rem',
                  cursor: 'pointer',
                  padding: '2px 6px',
                }}
              >
                ✕
              </button>
            </div>

            <form onSubmit={handleScalePartitions} style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
              <div>
                <label htmlFor="target-partition-count-input" style={{ display: 'block', fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-body)', marginBottom: '4px' }}>
                  Topic: <code style={{ color: '#0284c7' }}>{topicName}</code>
                </label>
                <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '10px' }}>
                  Current partition count: <strong>{partitionCount}</strong>
                </div>

                <label htmlFor="target-partition-count-input" style={{ display: 'block', fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-body)', marginBottom: '4px' }}>
                  Target Partition Count:
                </label>
                <input
                  id="target-partition-count-input"
                  type="number"
                  min={partitionCount + 1}
                  max={64}
                  value={targetPartitionCount}
                  onChange={(e) => setTargetPartitionCount(Number(e.target.value))}
                  style={{
                    width: '100%',
                    padding: '8px 12px',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid var(--border-medium)',
                    fontFamily: 'var(--font-mono)',
                    fontSize: '0.9rem',
                    background: '#ffffff',
                  }}
                />
              </div>

              {/* Kafka Rule Callout */}
              <div
                style={{
                  padding: '10px 12px',
                  borderRadius: 'var(--radius-sm)',
                  background: 'var(--color-amber-bg)',
                  border: '1px solid var(--color-amber-border)',
                  fontSize: '0.785rem',
                  color: '#92400e',
                  lineHeight: '1.4',
                }}
              >
                <strong>Kafka Partition Scaling Rule:</strong>
                <ul style={{ paddingLeft: '16px', marginTop: '4px' }}>
                  <li>Partitions can only be increased, never reduced.</li>
                  <li>Existing messages remain on their current partitions with their original offsets.</li>
                  <li>New records will be hashed across {targetPartitionCount} partitions.</li>
                </ul>
              </div>

              {scaleError && (
                <div style={{ padding: '8px 12px', borderRadius: 'var(--radius-sm)', background: 'var(--color-crimson-bg)', border: '1px solid var(--color-crimson-border)', color: 'var(--color-crimson)', fontSize: '0.8rem' }}>
                  {scaleError}
                </div>
              )}

              {scaleSuccess && (
                <div style={{ padding: '8px 12px', borderRadius: 'var(--radius-sm)', background: 'var(--color-emerald-bg)', border: '1px solid var(--color-emerald-border)', color: 'var(--color-emerald)', fontSize: '0.8rem' }}>
                  {scaleSuccess}
                </div>
              )}

              <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px', marginTop: '8px' }}>
                <button
                  type="button"
                  onClick={() => setShowScaleModal(false)}
                  style={{
                    padding: '7px 14px',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid var(--border-medium)',
                    background: '#ffffff',
                    color: 'var(--text-body)',
                    fontSize: '0.825rem',
                    fontWeight: 600,
                    cursor: 'pointer',
                  }}
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={scaling}
                  style={{
                    padding: '7px 16px',
                    borderRadius: 'var(--radius-sm)',
                    border: 'none',
                    background: 'linear-gradient(135deg, #0284c7 0%, #2563eb 100%)',
                    color: '#ffffff',
                    fontSize: '0.825rem',
                    fontWeight: 600,
                    cursor: scaling ? 'not-allowed' : 'pointer',
                    boxShadow: 'var(--shadow-sm)',
                  }}
                >
                  {scaling ? 'Scaling Broker...' : `Apply Scale to ${targetPartitionCount}`}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};
