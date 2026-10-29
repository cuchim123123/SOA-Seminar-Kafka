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

  // 3. Sorting & UX State
  const [sortOrder, setSortOrder] = useState<'latest' | 'oldest'>('latest');
  const [sortField, setSortField] = useState<'offset' | 'key' | 'sequence'>('offset');
  const [copiedNotification, setCopiedNotification] = useState<string | null>(null);

  // 4. Invalidation & Message Tick-Selection State
  const [selectedPurchaseIds, setSelectedPurchaseIds] = useState<string[]>([]);
  const [invalidationReason, setInvalidationReason] = useState<string>('DEMO_INVALIDATION');
  const [isInvalidating, setIsInvalidating] = useState<boolean>(false);
  const [invalidationStatus, setInvalidationStatus] = useState<{
    type: 'success' | 'error';
    message: string;
    items?: Array<{ purchaseId: string; customerId: string; sequence: number }>;
  } | null>(null);

  // 5. Partition Scaling State
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

  // Copy helper
  const copyToClipboard = (text: string, label: string) => {
    navigator.clipboard.writeText(text);
    setCopiedNotification(`Copied ${label}`);
    setTimeout(() => setCopiedNotification(null), 1500);
  };

  // Handle Partition Scale
  const handleScalePartitions = async (e: React.FormEvent) => {
    e.preventDefault();
    if (targetPartitionCount <= partitionCount) {
      setScaleError(
        `New partition count (${targetPartitionCount}) must be greater than current count (${partitionCount}). Kafka does not allow decreasing partitions.`,
      );
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

  // Filter and Sort messages
  const processedMessages = useMemo(() => {
    let result = [...messages];

    // Filter
    if (messageSearch.trim()) {
      const q = messageSearch.toLowerCase();
      result = result.filter((m) => {
        const keyMatch = m.key?.toLowerCase().includes(q);
        const offsetMatch = m.offset.includes(q);
        const payloadStr = JSON.stringify(m.payload || '').toLowerCase();
        return keyMatch || offsetMatch || payloadStr.includes(q);
      });
    }

    // Sort
    result.sort((a, b) => {
      if (sortField === 'offset') {
        const diff = BigInt(a.offset) - BigInt(b.offset);
        return sortOrder === 'latest'
          ? diff > 0n
            ? -1
            : diff < 0n
              ? 1
              : 0
          : diff > 0n
            ? 1
            : diff < 0n
              ? -1
              : 0;
      }

      if (sortField === 'key') {
        const keyA = a.key || '';
        const keyB = b.key || '';
        const comp = keyA.localeCompare(keyB);
        return sortOrder === 'latest' ? -comp : comp;
      }

      if (sortField === 'sequence') {
        const seqA = (a.payload as Record<string, unknown>)?.customerSequence ?? 0;
        const seqB = (b.payload as Record<string, unknown>)?.customerSequence ?? 0;
        const diff = Number(seqA) - Number(seqB);
        return sortOrder === 'latest' ? -diff : diff;
      }

      return 0;
    });

    return result;
  }, [messages, messageSearch, sortOrder, sortField]);

  // Message statistics
  const partitionStats = useMemo(() => {
    let completed = 0;
    let invalidated = 0;
    for (const m of messages) {
      const eventType = (m.payload as Record<string, unknown>)?.eventType;
      if (eventType === 'PurchaseInvalidated') {
        invalidated++;
      } else {
        completed++;
      }
    }
    return { completed, invalidated, total: messages.length };
  }, [messages]);

  // Extract purchase details from envelope
  const getPurchaseDetails = (msg: BrokerMessageRecord) => {
    if (!msg.payload || typeof msg.payload !== 'object') {
      return {
        purchaseId: null,
        eventType: 'RawMessage',
        isInvalidated: false,
        customerSeq: null,
        amountCents: null,
        reason: null,
      };
    }
    const p = msg.payload as Record<string, unknown>;
    const eventType = typeof p.eventType === 'string' ? p.eventType : 'RawMessage';
    const isInvalidated = eventType === 'PurchaseInvalidated';
    const innerPayload =
      p.payload && typeof p.payload === 'object'
        ? (p.payload as Record<string, unknown>)
        : undefined;

    const purchaseId =
      innerPayload && typeof innerPayload.purchaseId === 'string'
        ? innerPayload.purchaseId
        : typeof p.purchaseId === 'string'
          ? (p.purchaseId as string)
          : null;

    const customerSeq =
      p.customerSequence !== undefined
        ? String(p.customerSequence)
        : innerPayload?.customerSequence !== undefined
          ? String(innerPayload.customerSequence)
          : null;

    const amountCents =
      innerPayload && innerPayload.amountCents !== undefined
        ? Number(innerPayload.amountCents)
        : null;

    const reason =
      innerPayload && typeof innerPayload.reason === 'string'
        ? innerPayload.reason
        : null;

    return { purchaseId, eventType, isInvalidated, customerSeq, amountCents, reason };
  };

  // Computes selectable purchases from current processed messages
  const selectablePurchases = useMemo(() => {
    const list: string[] = [];
    for (const msg of processedMessages) {
      const { purchaseId, isInvalidated } = getPurchaseDetails(msg);
      if (purchaseId && !isInvalidated && !list.includes(purchaseId)) {
        list.push(purchaseId);
      }
    }
    return list;
  }, [processedMessages]);

  const allSelectableChecked =
    selectablePurchases.length > 0 &&
    selectablePurchases.every((id) => selectedPurchaseIds.includes(id));

  const toggleSelectAll = () => {
    if (allSelectableChecked) {
      setSelectedPurchaseIds([]);
    } else {
      setSelectedPurchaseIds([...selectablePurchases]);
    }
  };

  const toggleSelectPurchase = (purchaseId: string) => {
    setSelectedPurchaseIds((prev) =>
      prev.includes(purchaseId) ? prev.filter((id) => id !== purchaseId) : [...prev, purchaseId],
    );
  };

  // Clear selection on partition change
  useEffect(() => {
    setSelectedPurchaseIds([]);
    setInvalidationStatus(null);
  }, [selectedPartition]);

  // Batch invalidation handler
  const handleBatchInvalidate = async () => {
    if (selectedPurchaseIds.length === 0) return;
    setIsInvalidating(true);
    setInvalidationStatus(null);
    const results: Array<{ purchaseId: string; customerId: string; sequence: number }> = [];
    const errors: string[] = [];

    try {
      for (const pid of selectedPurchaseIds) {
        try {
          const res = await api.invalidatePurchase(pid, invalidationReason);
          results.push({
            purchaseId: pid,
            customerId: res.customerId,
            sequence: res.customerSequence,
          });
        } catch (err: unknown) {
          const msg = err instanceof Error ? err.message : String(err);
          errors.push(`${pid.slice(0, 10)}...: ${msg}`);
        }
      }

      if (results.length > 0) {
        setInvalidationStatus({
          type: 'success',
          message: `✓ Successfully emitted compensation for ${results.length} purchase${results.length > 1 ? 's' : ''}!`,
          items: results,
        });
        setSelectedPurchaseIds([]);
        await fetchBrokerMessages(selectedPartition, peekLimit);
        if (onMetadataChanged) onMetadataChanged();
      } else {
        setInvalidationStatus({
          type: 'error',
          message: `Failed to invalidate: ${errors.join(', ')}`,
        });
      }
    } finally {
      setIsInvalidating(false);
    }
  };

  // Single 1-click invalidation handler
  const handleSingleInvalidate = async (purchaseId: string) => {
    setIsInvalidating(true);
    setInvalidationStatus(null);
    try {
      const res = await api.invalidatePurchase(purchaseId, invalidationReason);
      setInvalidationStatus({
        type: 'success',
        message: `✓ Invalidation compensation emitted for customer ${res.customerId} (allocated customer sequence #${res.customerSequence}).`,
        items: [{ purchaseId, customerId: res.customerId, sequence: res.customerSequence }],
      });
      setSelectedPurchaseIds((prev) => prev.filter((id) => id !== purchaseId));
      await fetchBrokerMessages(selectedPartition, peekLimit);
      if (onMetadataChanged) onMetadataChanged();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setInvalidationStatus({
        type: 'error',
        message: `Failed to invalidate purchase: ${msg}`,
      });
    } finally {
      setIsInvalidating(false);
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* 1. Header & Quick Actions */}
      <section className="cyber-card" style={{ padding: '20px 24px' }}>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: '12px',
          }}
        >
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <h2
                style={{
                  fontSize: '1.15rem',
                  fontWeight: 700,
                  color: 'var(--text-main)',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                }}
              >
                <span style={{ color: 'var(--color-cyan)' }}>❖</span>
                Kafka Partition Management &amp; Live Broker Inspector
              </h2>
              <span className="badge-mono badge-cyan">{topicName}</span>
            </div>
            <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem', marginTop: '4px' }}>
              Direct KRaft broker telemetry. Inspect live Kafka log positions, peek raw broker
              messages, and scale topic partitions dynamically.
            </p>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
            <span className="badge-mono badge-violet">{partitionCount} Partitions</span>
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
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              flexWrap: 'wrap',
              gap: '8px',
              marginBottom: '10px',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ fontSize: '0.9rem', color: 'var(--color-violet)', fontWeight: 700 }}>
                ⚡
              </span>
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

          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
              gap: '12px',
              alignItems: 'center',
            }}
          >
            <div>
              <label
                htmlFor="partition-key-input"
                style={{
                  display: 'block',
                  fontSize: '0.75rem',
                  color: 'var(--text-muted)',
                  marginBottom: '4px',
                  fontWeight: 600,
                }}
              >
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

            <div
              style={{
                background: '#ffffff',
                padding: '10px 14px',
                borderRadius: 'var(--radius-sm)',
                border: '1px solid var(--border-subtle)',
                fontSize: '0.785rem',
                fontFamily: 'var(--font-mono)',
              }}
            >
              <div
                style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '3px' }}
              >
                <span style={{ color: 'var(--text-muted)' }}>Murmur2 Hash:</span>
                <span style={{ color: 'var(--text-main)', fontWeight: 600 }}>
                  {hashResult.hexHash}
                </span>
              </div>
              <div
                style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '3px' }}
              >
                <span style={{ color: 'var(--text-muted)' }}>Positive (31-bit):</span>
                <span style={{ color: 'var(--text-main)', fontWeight: 600 }}>
                  {hashResult.positiveHash.toLocaleString()}
                </span>
              </div>
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  borderTop: '1px solid var(--border-subtle)',
                  paddingTop: '4px',
                }}
              >
                <span style={{ color: 'var(--text-muted)' }}>Target Partition:</span>
                <span style={{ color: '#0284c7', fontWeight: 700, fontSize: '0.85rem' }}>
                  Partition #{hashResult.targetPartition}
                </span>
              </div>
            </div>

            <div style={{ fontSize: '0.775rem', color: 'var(--text-muted)', lineHeight: '1.4' }}>
              <strong style={{ color: 'var(--text-body)' }}>Deterministic Guarantees:</strong> Any
              message produced with key{' '}
              <code
                style={{ fontFamily: 'var(--font-mono)', color: '#0284c7', fontWeight: 600 }}
              >
                {simulatorKey || '...'}
              </code>{' '}
              is guaranteed to land in{' '}
              <strong style={{ color: '#0284c7' }}>
                Partition #{hashResult.targetPartition}
              </strong>
              .
            </div>
          </div>
        </div>
      </section>

      {/* 3. Partition Cards Matrix */}
      <section className="cyber-card" style={{ padding: '20px 24px' }}>
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: '14px',
            flexWrap: 'wrap',
            gap: '8px',
          }}
        >
          <div>
            <h3 style={{ fontSize: '0.95rem', fontWeight: 700, color: 'var(--text-main)' }}>
              Partition Matrix ({partitionCount} Partitions)
            </h3>
            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
              Click any partition card below to inspect its live log offsets and stream messages.
            </span>
          </div>

          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              fontSize: '0.75rem',
              fontFamily: 'var(--font-mono)',
            }}
          >
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
              <span
                style={{
                  width: '10px',
                  height: '10px',
                  borderRadius: '2px',
                  background: '#0284c7',
                  display: 'inline-block',
                }}
              />
              Active Target
            </span>
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
              <span
                style={{
                  width: '10px',
                  height: '10px',
                  borderRadius: '2px',
                  background: '#e0f2fe',
                  border: '1px solid #0284c7',
                  display: 'inline-block',
                }}
              />
              Selected for Peek
            </span>
          </div>
        </div>

        {loading && !metadata ? (
          <div
            style={{
              padding: '28px',
              textAlign: 'center',
              color: 'var(--text-dim)',
              fontFamily: 'var(--font-mono)',
              fontSize: '0.85rem',
            }}
          >
            Reading broker partition log end offsets...
          </div>
        ) : (
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(190px, 1fr))',
              gap: '12px',
            }}
          >
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
                    boxShadow: isTarget
                      ? '0 0 0 3px rgba(2, 132, 199, 0.15)'
                      : 'var(--shadow-sm)',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease',
                  }}
                >
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      marginBottom: '8px',
                    }}
                  >
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

                  <div
                    style={{
                      display: 'flex',
                      flexDirection: 'column',
                      gap: '4px',
                      fontSize: '0.75rem',
                      fontFamily: 'var(--font-mono)',
                    }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span style={{ color: 'var(--text-muted)' }}>Start Offset:</span>
                      <span style={{ color: 'var(--text-body)', fontWeight: 600 }}>
                        {p.logStartOffset}
                      </span>
                    </div>

                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span style={{ color: 'var(--text-muted)' }}>High-Watermark:</span>
                      <span style={{ color: 'var(--color-emerald)', fontWeight: 600 }}>
                        {p.highWatermark}
                      </span>
                    </div>

                    <div
                      style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        borderTop: '1px solid var(--border-subtle)',
                        paddingTop: '4px',
                        marginTop: '2px',
                      }}
                    >
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

      {/* 4. Live Broker Message Viewer with Advanced Sorting & UX */}
      <section className="cyber-card" style={{ padding: '20px 24px' }}>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: '12px',
            marginBottom: '16px',
          }}
        >
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <h3
                style={{
                  fontSize: '1.05rem',
                  fontWeight: 700,
                  color: 'var(--text-main)',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                }}
              >
                <span>🔍</span> Live Broker Records — Partition #{selectedPartition}
              </h3>
              <span className="badge-mono badge-cyan">{messages.length} Records Peeked</span>
            </div>
            <p style={{ color: 'var(--text-muted)', fontSize: '0.8rem', marginTop: '2px' }}>
              Peeking raw records directly from Kafka partition log without mutating consumer group
              committed offsets.
            </p>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
            {/* Search Filter */}
            <input
              type="text"
              value={messageSearch}
              onChange={(e) => setMessageSearch(e.target.value)}
              placeholder="Search in records..."
              style={{
                padding: '6px 12px',
                borderRadius: 'var(--radius-sm)',
                border: '1px solid var(--border-medium)',
                fontFamily: 'var(--font-mono)',
                fontSize: '0.785rem',
                minWidth: '180px',
                background: '#ffffff',
              }}
            />

            {/* Limit Selector */}
            <select
              aria-label="Message Limit Selector"
              value={peekLimit}
              onChange={(e) => setPeekLimit(Number(e.target.value))}
              style={{
                padding: '6px 10px',
                borderRadius: 'var(--radius-sm)',
                border: '1px solid var(--border-medium)',
                fontFamily: 'var(--font-mono)',
                fontSize: '0.785rem',
                background: '#ffffff',
              }}
            >
              <option value={5}>5 records</option>
              <option value={10}>10 records</option>
              <option value={25}>25 records</option>
              <option value={50}>50 records</option>
            </select>

            {/* Auto-peek toggle */}
            <label
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '4px',
                fontSize: '0.775rem',
                color: 'var(--text-body)',
                cursor: 'pointer',
                userSelect: 'none',
                background: autoPeek ? '#ecfdf5' : '#ffffff',
                border: autoPeek ? '1px solid #a7f3d0' : '1px solid var(--border-medium)',
                padding: '4px 8px',
                borderRadius: 'var(--radius-sm)',
              }}
            >
              <input
                type="checkbox"
                checked={autoPeek}
                onChange={(e) => setAutoPeek(e.target.checked)}
              />
              Auto-Refresh (3s)
            </label>

            {/* Refresh Button */}
            <button
              type="button"
              disabled={peeking}
              onClick={() => fetchBrokerMessages(selectedPartition, peekLimit)}
              style={{
                padding: '6px 14px',
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
                boxShadow: 'var(--shadow-sm)',
              }}
            >
              {peeking ? 'Peeking...' : '↻ Peek Now'}
            </button>
          </div>
        </div>

        {/* 5. Powerful Message Sorting & UX Controls Toolbar */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: '10px',
            padding: '10px 14px',
            borderRadius: 'var(--radius-md)',
            background: 'var(--bg-subtle)',
            border: '1px solid var(--border-subtle)',
            marginBottom: '14px',
          }}
        >
          {/* Sorting Controls */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
            <span
              style={{
                fontSize: '0.75rem',
                fontWeight: 700,
                color: 'var(--text-muted)',
                textTransform: 'uppercase',
                letterSpacing: '0.04em',
              }}
            >
              Sort Records:
            </span>

            {/* Latest vs Oldest Toggle Button */}
            <div
              style={{
                display: 'inline-flex',
                borderRadius: 'var(--radius-sm)',
                border: '1px solid var(--border-medium)',
                overflow: 'hidden',
                background: '#ffffff',
              }}
            >
              <button
                type="button"
                onClick={() => setSortOrder('latest')}
                style={{
                  padding: '4px 10px',
                  border: 'none',
                  background: sortOrder === 'latest' ? '#0284c7' : '#ffffff',
                  color: sortOrder === 'latest' ? '#ffffff' : 'var(--text-body)',
                  fontSize: '0.75rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '4px',
                }}
              >
                <span>⬇</span> Latest First (Newest)
              </button>
              <button
                type="button"
                onClick={() => setSortOrder('oldest')}
                style={{
                  padding: '4px 10px',
                  border: 'none',
                  borderLeft: '1px solid var(--border-medium)',
                  background: sortOrder === 'oldest' ? '#0284c7' : '#ffffff',
                  color: sortOrder === 'oldest' ? '#ffffff' : 'var(--text-body)',
                  fontSize: '0.75rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '4px',
                }}
              >
                <span>⬆</span> Oldest First (Ascending)
              </button>
            </div>

            {/* Sort Field Selector */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
              <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>By:</span>
              <select
                aria-label="Sort Field"
                value={sortField}
                onChange={(e) =>
                  setSortField(e.target.value as 'offset' | 'key' | 'sequence')
                }
                style={{
                  padding: '4px 8px',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid var(--border-medium)',
                  fontSize: '0.75rem',
                  background: '#ffffff',
                  fontFamily: 'var(--font-mono)',
                }}
              >
                <option value="offset">Broker Offset</option>
                <option value="key">Key (Customer ID)</option>
                <option value="sequence">Customer Sequence #</option>
              </select>
            </div>
          </div>

          {/* Quick Metrics & Copied Toast */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
            {copiedNotification && (
              <span
                style={{
                  fontSize: '0.75rem',
                  color: 'var(--color-emerald)',
                  fontWeight: 600,
                  background: '#ecfdf5',
                  padding: '2px 8px',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid #a7f3d0',
                }}
              >
                ✓ {copiedNotification}
              </span>
            )}
            <span className="badge-mono badge-emerald">{partitionStats.completed} Completed</span>
            {partitionStats.invalidated > 0 && (
              <span className="badge-mono badge-amber">
                {partitionStats.invalidated} Invalidated
              </span>
            )}
            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
              Showing {processedMessages.length} of {messages.length}
            </span>
          </div>
        </div>

        {/* Invalidation Execution Feedback Banner */}
        {invalidationStatus && (
          <div
            style={{
              padding: '12px 16px',
              borderRadius: 'var(--radius-md)',
              background: invalidationStatus.type === 'success' ? '#ecfdf5' : '#fff1f2',
              border: invalidationStatus.type === 'success' ? '1px solid #a7f3d0' : '1px solid #fecdd3',
              color: invalidationStatus.type === 'success' ? '#047857' : '#be123c',
              marginBottom: '14px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              flexWrap: 'wrap',
              gap: '8px',
            }}
          >
            <div>
              <div style={{ fontWeight: 700, fontSize: '0.85rem' }}>{invalidationStatus.message}</div>
              {invalidationStatus.items && invalidationStatus.items.length > 0 && (
                <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', marginTop: '4px', display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                  {invalidationStatus.items.map((it) => (
                    <span
                      key={it.purchaseId}
                      style={{
                        background: '#ffffff',
                        padding: '2px 8px',
                        borderRadius: 'var(--radius-sm)',
                        border: '1px solid #a7f3d0',
                        color: '#065f46',
                      }}
                    >
                      Customer <strong>{it.customerId}</strong> ➔ Seq #{it.sequence} (ID: {it.purchaseId.slice(0, 12)}...)
                    </span>
                  ))}
                </div>
              )}
            </div>
            <button
              type="button"
              onClick={() => setInvalidationStatus(null)}
              style={{
                background: 'transparent',
                border: 'none',
                color: 'inherit',
                cursor: 'pointer',
                fontWeight: 700,
                fontSize: '0.85rem',
              }}
            >
              ✕
            </button>
          </div>
        )}

        {/* Batch Invalidation Toolbar when messages are ticked */}
        {selectedPurchaseIds.length > 0 && (
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              padding: '12px 16px',
              borderRadius: 'var(--radius-md)',
              background: '#fff1f2',
              border: '1px solid #fecdd3',
              marginBottom: '14px',
              flexWrap: 'wrap',
              gap: '12px',
              boxShadow: 'var(--shadow-sm)',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
              <span
                style={{
                  background: '#e11d48',
                  color: '#ffffff',
                  fontSize: '0.75rem',
                  fontWeight: 700,
                  padding: '3px 9px',
                  borderRadius: 'var(--radius-full)',
                }}
              >
                {selectedPurchaseIds.length} Selected
              </span>
              <span style={{ fontSize: '0.8rem', fontWeight: 600, color: '#9f1239' }}>
                Batch Invalidation Action:
              </span>
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                <label style={{ fontSize: '0.75rem', color: '#881337', fontWeight: 600 }}>Reason:</label>
                <select
                  aria-label="Batch Invalidation Reason"
                  value={invalidationReason}
                  onChange={(e) => setInvalidationReason(e.target.value)}
                  style={{
                    padding: '4px 8px',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid #fca5a5',
                    fontSize: '0.75rem',
                    background: '#ffffff',
                    fontFamily: 'var(--font-mono)',
                    color: '#881337',
                  }}
                >
                  <option value="DEMO_INVALIDATION">Demo Invalidation (Seminar)</option>
                  <option value="CUSTOMER_RETURN">Customer Return / Refund</option>
                  <option value="FRAUD_CONFIRMED">Fraud Confirmed</option>
                </select>
              </div>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <button
                type="button"
                onClick={() => setSelectedPurchaseIds([])}
                style={{
                  padding: '5px 12px',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid #fca5a5',
                  background: '#ffffff',
                  color: '#9f1239',
                  fontSize: '0.75rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                }}
              >
                Deselect All
              </button>
              <button
                type="button"
                disabled={isInvalidating}
                onClick={handleBatchInvalidate}
                style={{
                  padding: '6px 16px',
                  borderRadius: 'var(--radius-sm)',
                  border: 'none',
                  background: 'linear-gradient(135deg, #e11d48 0%, #be123c 100%)',
                  color: '#ffffff',
                  fontSize: '0.8rem',
                  fontWeight: 700,
                  cursor: isInvalidating ? 'not-allowed' : 'pointer',
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '6px',
                  boxShadow: '0 2px 6px rgba(225, 29, 72, 0.35)',
                }}
              >
                {isInvalidating ? 'Invalidating...' : `⚡ Invalidate Selected (${selectedPurchaseIds.length})`}
              </button>
            </div>
          </div>
        )}

        {peekError && (
          <div
            style={{
              padding: '10px 14px',
              borderRadius: 'var(--radius-sm)',
              background: 'var(--color-crimson-bg)',
              border: '1px solid var(--color-crimson-border)',
              color: 'var(--color-crimson)',
              fontSize: '0.8rem',
              marginBottom: '14px',
            }}
          >
            Failed to peek partition messages: {peekError}
          </div>
        )}

        {processedMessages.length === 0 ? (
          <div
            style={{
              padding: '36px',
              textAlign: 'center',
              color: 'var(--text-muted)',
              fontFamily: 'var(--font-mono)',
              fontSize: '0.825rem',
              background: 'var(--bg-subtle)',
              borderRadius: 'var(--radius-md)',
            }}
          >
            {peeking
              ? 'Fetching broker partition records...'
              : `No messages found in Partition #${selectedPartition} log.`}
          </div>
        ) : (
          <div
            style={{
              overflowX: 'auto',
              border: '1px solid var(--border-subtle)',
              borderRadius: 'var(--radius-md)',
            }}
          >
            <table
              style={{
                width: '100%',
                borderCollapse: 'collapse',
                fontSize: '0.8rem',
                textAlign: 'left',
                fontFamily: 'var(--font-sans)',
              }}
            >
              <thead>
                <tr
                  style={{
                    background: 'var(--bg-subtle)',
                    borderBottom: '1px solid var(--border-subtle)',
                    color: 'var(--text-muted)',
                    fontSize: '0.75rem',
                    textTransform: 'uppercase',
                    letterSpacing: '0.04em',
                  }}
                >
                  <th style={{ width: '42px', padding: '10px 10px', textAlign: 'center' }}>
                    <input
                      type="checkbox"
                      aria-label="Select all completed purchases on partition"
                      title={
                        selectablePurchases.length > 0
                          ? 'Select / Deselect all completed purchases'
                          : 'No completed purchases available to select'
                      }
                      checked={allSelectableChecked}
                      onChange={toggleSelectAll}
                      disabled={selectablePurchases.length === 0}
                      style={{
                        cursor: selectablePurchases.length > 0 ? 'pointer' : 'not-allowed',
                        width: '15px',
                        height: '15px',
                        accentColor: '#e11d48',
                      }}
                    />
                  </th>
                  <th style={{ padding: '10px 12px' }}>Offset</th>
                  <th style={{ padding: '10px 12px' }}>Key (CustomerId)</th>
                  <th style={{ padding: '10px 12px' }}>Event Type</th>
                  <th style={{ padding: '10px 12px' }}>Timestamp</th>
                  <th style={{ padding: '10px 12px' }}>Payload Summary</th>
                  <th style={{ padding: '10px 14px', textAlign: 'right' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {processedMessages.map((msg) => {
                  const {
                    purchaseId,
                    isInvalidated,
                    customerSeq,
                    amountCents,
                    reason,
                  } = getPurchaseDetails(msg);
                  const isSelected = purchaseId ? selectedPurchaseIds.includes(purchaseId) : false;
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
                          background: isSelected
                            ? '#fff1f2'
                            : isExpanded
                              ? '#f8fafc'
                              : isInvalidated
                                ? '#fffbeb'
                                : '#ffffff',
                          borderLeft: isSelected
                            ? '3px solid #e11d48'
                            : isInvalidated
                              ? '3px solid #f59e0b'
                              : '3px solid transparent',
                          transition: 'background 0.15s ease',
                        }}
                      >
                        {/* Checkbox column */}
                        <td style={{ textAlign: 'center', padding: '10px 10px' }}>
                          {purchaseId && !isInvalidated ? (
                            <input
                              type="checkbox"
                              aria-label={`Select purchase ${purchaseId}`}
                              checked={isSelected}
                              onChange={() => toggleSelectPurchase(purchaseId)}
                              style={{
                                cursor: 'pointer',
                                width: '15px',
                                height: '15px',
                                accentColor: '#e11d48',
                              }}
                            />
                          ) : (
                            <span style={{ color: 'var(--text-dim)', fontSize: '0.75rem' }}>-</span>
                          )}
                        </td>

                        <td
                          style={{
                            padding: '10px 12px',
                            fontFamily: 'var(--font-mono)',
                            fontWeight: 600,
                          }}
                        >
                          <span
                            className="badge-mono badge-cyan"
                            onClick={() => copyToClipboard(msg.offset, 'Offset')}
                            title="Click to copy offset"
                            style={{ cursor: 'pointer' }}
                          >
                            #{msg.offset}
                          </span>
                        </td>
                        <td
                          style={{
                            padding: '10px 12px',
                            fontFamily: 'var(--font-mono)',
                            fontWeight: 600,
                          }}
                        >
                          {msg.key ? (
                            <span
                              onClick={() => copyToClipboard(msg.key!, 'Customer ID')}
                              title="Click to copy Customer ID"
                              style={{
                                color: '#0284c7',
                                cursor: 'pointer',
                                textDecoration: 'underline dotted',
                              }}
                            >
                              {msg.key}
                            </span>
                          ) : (
                            <span style={{ color: 'var(--text-dim)' }}>[Null Key]</span>
                          )}
                        </td>
                        <td style={{ padding: '10px 12px' }}>
                          <span
                            className={`badge-mono ${isInvalidated ? 'badge-amber' : 'badge-emerald'}`}
                          >
                            {isInvalidated ? '↩ PurchaseInvalidated' : '✓ PurchaseCompleted'}
                          </span>
                        </td>
                        <td
                          style={{
                            padding: '10px 12px',
                            color: 'var(--text-muted)',
                            fontSize: '0.75rem',
                            fontFamily: 'var(--font-mono)',
                          }}
                        >
                          {timeDisplay}
                        </td>
                        <td
                          style={{
                            padding: '10px 12px',
                            color: 'var(--text-body)',
                            fontSize: '0.775rem',
                          }}
                        >
                          <span>
                            {customerSeq && (
                              <span style={{ marginRight: '6px' }}>
                                Seq{' '}
                                <strong style={{ fontFamily: 'var(--font-mono)' }}>
                                  #{customerSeq}
                                </strong>
                              </span>
                            )}
                            {amountCents !== null && (
                              <span style={{ color: 'var(--color-emerald)', fontWeight: 600, marginRight: '6px' }}>
                                ${(amountCents / 100).toFixed(2)}
                              </span>
                            )}
                            {reason && (
                              <span style={{ color: 'var(--color-amber)', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
                                [{reason}]
                              </span>
                            )}
                            {purchaseId && (
                              <span
                                onClick={() => copyToClipboard(purchaseId, 'Purchase ID')}
                                title="Click to copy Purchase ID"
                                style={{
                                  fontSize: '0.7rem',
                                  color: 'var(--text-muted)',
                                  fontFamily: 'var(--font-mono)',
                                  marginLeft: '4px',
                                  cursor: 'pointer',
                                }}
                              >
                                ({purchaseId.slice(0, 14)}...)
                              </span>
                            )}
                          </span>
                        </td>
                        <td style={{ padding: '10px 14px', textAlign: 'right', whiteSpace: 'nowrap' }}>
                          {purchaseId && !isInvalidated ? (
                            <button
                              type="button"
                              disabled={isInvalidating}
                              onClick={() => handleSingleInvalidate(purchaseId)}
                              style={{
                                padding: '4px 10px',
                                borderRadius: 'var(--radius-sm)',
                                border: '1px solid #fecdd3',
                                background: '#fff1f2',
                                color: '#e11d48',
                                fontSize: '0.725rem',
                                fontWeight: 600,
                                cursor: isInvalidating ? 'not-allowed' : 'pointer',
                                marginRight: '6px',
                                display: 'inline-flex',
                                alignItems: 'center',
                                gap: '3px',
                                transition: 'all 0.15s ease',
                              }}
                              title={`1-click Invalidate purchase ${purchaseId}`}
                            >
                              <span>↩</span> Invalidate
                            </button>
                          ) : isInvalidated ? (
                            <span
                              className="badge-mono badge-amber"
                              style={{ marginRight: '6px', fontSize: '0.675rem' }}
                            >
                              Invalidated
                            </span>
                          ) : null}

                          <button
                            type="button"
                            onClick={() => setExpandedOffset(isExpanded ? null : msg.offset)}
                            style={{
                              padding: '4px 10px',
                              borderRadius: 'var(--radius-sm)',
                              border: '1px solid var(--border-medium)',
                              background: '#ffffff',
                              color: 'var(--color-primary)',
                              fontSize: '0.725rem',
                              fontWeight: 600,
                              cursor: 'pointer',
                            }}
                          >
                            {isExpanded ? 'Hide' : 'Inspect'}
                          </button>
                        </td>
                      </tr>

                      {isExpanded && (
                        <tr
                          style={{
                            background: '#f8fafc',
                            borderBottom: '1px solid var(--border-subtle)',
                          }}
                        >
                          <td colSpan={7} style={{ padding: '14px 18px' }}>
                            <div
                              style={{
                                display: 'flex',
                                justifyContent: 'space-between',
                                alignItems: 'center',
                                marginBottom: '8px',
                              }}
                            >
                              <span
                                style={{
                                  fontSize: '0.75rem',
                                  fontWeight: 700,
                                  color: 'var(--text-muted)',
                                  fontFamily: 'var(--font-mono)',
                                }}
                              >
                                RECORD #{msg.offset} (PARTITION #{msg.partition}) — FULL KAFKA
                                ENVELOPE
                              </span>
                              <button
                                type="button"
                                onClick={() =>
                                  copyToClipboard(
                                    JSON.stringify(msg.payload, null, 2),
                                    'Full Envelope JSON',
                                  )
                                }
                                style={{
                                  fontSize: '0.725rem',
                                  padding: '3px 10px',
                                  borderRadius: 'var(--radius-sm)',
                                  border: '1px solid var(--border-medium)',
                                  background: '#ffffff',
                                  cursor: 'pointer',
                                  fontWeight: 600,
                                }}
                              >
                                📋 Copy JSON
                              </button>
                            </div>
                            <pre
                              style={{
                                margin: 0,
                                padding: '12px 14px',
                                background: '#ffffff',
                                border: '1px solid var(--border-subtle)',
                                borderRadius: 'var(--radius-sm)',
                                fontSize: '0.75rem',
                                fontFamily: 'var(--font-mono)',
                                color: 'var(--text-main)',
                                overflowX: 'auto',
                                maxHeight: '240px',
                                lineHeight: '1.45',
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

      {/* 6. Scale Partitions Modal */}
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
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginBottom: '14px',
              }}
            >
              <h3
                style={{
                  fontSize: '1.1rem',
                  fontWeight: 700,
                  color: 'var(--text-main)',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                }}
              >
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

            <form
              onSubmit={handleScalePartitions}
              style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}
            >
              <div>
                <label
                  htmlFor="target-partition-count-input"
                  style={{
                    display: 'block',
                    fontSize: '0.8rem',
                    fontWeight: 600,
                    color: 'var(--text-body)',
                    marginBottom: '4px',
                  }}
                >
                  Topic: <code style={{ color: '#0284c7' }}>{topicName}</code>
                </label>
                <div
                  style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '10px' }}
                >
                  Current partition count: <strong>{partitionCount}</strong>
                </div>

                <label
                  htmlFor="target-partition-count-input"
                  style={{
                    display: 'block',
                    fontSize: '0.8rem',
                    fontWeight: 600,
                    color: 'var(--text-body)',
                    marginBottom: '4px',
                  }}
                >
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
                  <li>
                    Existing messages remain on their current partitions with their original
                    offsets.
                  </li>
                  <li>New records will be hashed across {targetPartitionCount} partitions.</li>
                </ul>
              </div>

              {scaleError && (
                <div
                  style={{
                    padding: '8px 12px',
                    borderRadius: 'var(--radius-sm)',
                    background: 'var(--color-crimson-bg)',
                    border: '1px solid var(--color-crimson-border)',
                    color: 'var(--color-crimson)',
                    fontSize: '0.8rem',
                  }}
                >
                  {scaleError}
                </div>
              )}

              {scaleSuccess && (
                <div
                  style={{
                    padding: '8px 12px',
                    borderRadius: 'var(--radius-sm)',
                    background: 'var(--color-emerald-bg)',
                    border: '1px solid var(--color-emerald-border)',
                    color: 'var(--color-emerald)',
                    fontSize: '0.8rem',
                  }}
                >
                  {scaleSuccess}
                </div>
              )}

              <div
                style={{
                  display: 'flex',
                  justifyContent: 'flex-end',
                  gap: '8px',
                  marginTop: '8px',
                }}
              >
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
