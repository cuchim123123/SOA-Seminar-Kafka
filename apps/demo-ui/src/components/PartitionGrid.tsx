import React, { useState, useEffect, useMemo, useCallback } from 'react';
import type { TopicPartitionMetadata, BrokerMessageRecord } from '../api/client';
import { api } from '../api/client';
import { computeKafkaPartition } from '../utils/kafka-partitioner';
import { Murmur2SimulatorBar } from './partitions/Murmur2SimulatorBar';
import { ScalePartitionsModal } from './partitions/ScalePartitionsModal';
import { PartitionCardGrid } from './partitions/PartitionCardGrid';
import { PartitionMessageTable } from './partitions/PartitionMessageTable';
import type { PurchaseDetails } from './partitions/PartitionMessageTable';

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

  // 1. Murmur2 Simulator State
  const [simulatorKey, setSimulatorKey] = useState<string>('U001');

  // 2. Partition Selection & Peeking State
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

  // 4. Invalidation Selection State
  const [selectedOffsets, setSelectedOffsets] = useState<string[]>([]);
  const [invalidationReason, setInvalidationReason] = useState<string>('DEMO_INVALIDATION');
  const [isInvalidating, setIsInvalidating] = useState<boolean>(false);
  const [invalidationStatus, setInvalidationStatus] = useState<{
    type: 'success' | 'error';
    message: string;
    items?: Array<{ purchaseId: string; customerId: string; sequence: number }>;
  } | null>(null);

  // 5. Scaling State
  const [showScaleModal, setShowScaleModal] = useState<boolean>(false);
  const [targetPartitionCount, setTargetPartitionCount] = useState<number>(partitionCount + 2);
  const [scaling, setScaling] = useState<boolean>(false);
  const [scaleError, setScaleError] = useState<string | null>(null);
  const [scaleSuccess, setScaleSuccess] = useState<string | null>(null);

  useEffect(() => {
    if (partitionCount) {
      setTargetPartitionCount(partitionCount + 2);
    }
  }, [partitionCount]);

  const hashResult = useMemo(() => {
    return computeKafkaPartition(simulatorKey.trim() || 'U001', partitionCount);
  }, [simulatorKey, partitionCount]);

  const activeHighlight = simulatorKey.trim() ? hashResult.targetPartition : externalHighlight;

  const totalMessagesAcrossTopic = useMemo(() => {
    return partitions.reduce((sum, p) => sum + BigInt(p.messageCount || '0'), 0n);
  }, [partitions]);

  const fetchBrokerMessages = useCallback(
    async (partId: number, limit: number) => {
      setPeeking(true);
      setPeekError(null);
      try {
        const res = await api.peekPartitionMessages(topicName, partId, limit);
        setMessages(res);
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        setPeekError(msg);
      } finally {
        setPeeking(false);
      }
    },
    [topicName],
  );

  useEffect(() => {
    fetchBrokerMessages(selectedPartition, peekLimit);
  }, [selectedPartition, peekLimit, fetchBrokerMessages]);

  useEffect(() => {
    if (!autoPeek) return;
    const interval = setInterval(() => {
      fetchBrokerMessages(selectedPartition, peekLimit);
    }, 3000);
    return () => clearInterval(interval);
  }, [autoPeek, selectedPartition, peekLimit, fetchBrokerMessages]);

  const copyToClipboard = (text: string, label: string) => {
    navigator.clipboard.writeText(text);
    setCopiedNotification(`Copied ${label} to clipboard!`);
    setTimeout(() => setCopiedNotification(null), 2000);
  };

  const getPurchaseDetails = (msg: BrokerMessageRecord): PurchaseDetails => {
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

  const processedMessages = useMemo(() => {
    let result = [...messages];
    if (messageSearch.trim()) {
      const q = messageSearch.toLowerCase();
      result = result.filter((m) => {
        const offsetMatch = m.offset.includes(q);
        const keyMatch = m.key ? m.key.toLowerCase().includes(q) : false;
        const payloadStr = JSON.stringify(m.payload).toLowerCase();
        return offsetMatch || keyMatch || payloadStr.includes(q);
      });
    }

    result.sort((a, b) => {
      if (sortField === 'offset') {
        const diff = BigInt(a.offset) - BigInt(b.offset);
        return sortOrder === 'latest' ? (diff > 0n ? -1 : 1) : diff > 0n ? 1 : -1;
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

  const selectableOffsets = useMemo(() => {
    const list: string[] = [];
    for (const msg of processedMessages) {
      const { purchaseId, isInvalidated } = getPurchaseDetails(msg);
      if (purchaseId && !isInvalidated) {
        list.push(msg.offset);
      }
    }
    return list;
  }, [processedMessages]);

  const allSelectableChecked =
    selectableOffsets.length > 0 &&
    selectableOffsets.every((offset) => selectedOffsets.includes(offset));

  const toggleSelectAll = () => {
    if (allSelectableChecked) {
      setSelectedOffsets([]);
    } else {
      setSelectedOffsets([...selectableOffsets]);
    }
  };

  const toggleSelectOffset = (offset: string) => {
    setSelectedOffsets((prev) =>
      prev.includes(offset) ? prev.filter((id) => id !== offset) : [...prev, offset],
    );
  };

  useEffect(() => {
    setSelectedOffsets([]);
    setInvalidationStatus(null);
  }, [selectedPartition]);

  const handleBatchInvalidate = async () => {
    if (selectedOffsets.length === 0) return;
    setIsInvalidating(true);
    setInvalidationStatus(null);
    const results: Array<{ purchaseId: string; customerId: string; sequence: number }> = [];
    const errors: string[] = [];

    const selectedMessages = processedMessages.filter((m) => selectedOffsets.includes(m.offset));
    const targetPurchaseIds = Array.from(
      new Set(
        selectedMessages
          .map((m) => getPurchaseDetails(m).purchaseId)
          .filter((id): id is string => Boolean(id)),
      ),
    );

    try {
      for (const pid of targetPurchaseIds) {
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
        setSelectedOffsets([]);
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
      setSelectedOffsets((prev) =>
        prev.filter((offset) => {
          const m = messages.find((msg) => msg.offset === offset);
          return m ? getPurchaseDetails(m).purchaseId !== purchaseId : true;
        }),
      );
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

  const handleScalePartitions = async () => {
    setScaling(true);
    setScaleError(null);
    setScaleSuccess(null);
    try {
      await api.scaleTopicPartitions(topicName, targetCount);
      setScaleSuccess(`✓ Successfully scaled topic "${topicName}" to ${targetCount} partitions!`);
      setTimeout(() => {
        setShowScaleModal(false);
        setScaleSuccess(null);
        if (onMetadataChanged) onMetadataChanged();
      }, 1500);
    } catch (err) {
      setScaleError(err instanceof Error ? err.message : String(err));
    } finally {
      setScaling(false);
    }
  };

  const targetCount = targetPartitionCount;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* 1. Header & Murmur2 Simulator */}
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
            <span className="badge-mono badge-violet">{partitionCount} Partitions</span>
            <span className="badge-mono badge-emerald">{totalMessagesAcrossTopic.toString()} Total Records</span>
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

        <Murmur2SimulatorBar
          simulatorKey={simulatorKey}
          setSimulatorKey={setSimulatorKey}
          hashResult={hashResult}
        />
      </section>

      {/* 2. Partition Cards Matrix */}
      <PartitionCardGrid
        partitions={partitions}
        partitionCount={partitionCount}
        loading={loading}
        activeHighlight={activeHighlight}
        selectedPartition={selectedPartition}
        onSelectPartition={setSelectedPartition}
      />

      {/* 3. Live Broker Messages Table */}
      <section className="cyber-card" style={{ padding: '20px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '12px', marginBottom: '16px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <h3 style={{ fontSize: '1.05rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '6px' }}>
                <span>🔍</span> Live Broker Records — Partition #{selectedPartition}
              </h3>
              <span className="badge-mono badge-cyan">{messages.length} Records Peeked</span>
            </div>
            <p style={{ color: 'var(--text-muted)', fontSize: '0.8rem', marginTop: '2px' }}>
              Peeking raw records directly from Kafka partition log without mutating consumer group committed offsets.
            </p>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
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

        {/* Sorting Controls */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap', padding: '10px 14px', borderRadius: 'var(--radius-md)', background: 'var(--bg-subtle)', border: '1px solid var(--border-subtle)', marginBottom: '14px' }}>
          <span style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase' }}>
            Sort Records:
          </span>
          <button
            type="button"
            onClick={() => setSortOrder(sortOrder === 'latest' ? 'oldest' : 'latest')}
            className="btn-cyber btn-ghost"
            style={{ padding: '3px 8px', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}
          >
            {sortOrder === 'latest' ? '↓ Latest First' : '↑ Oldest First'}
          </button>
          <div style={{ display: 'flex', gap: '4px' }}>
            {(['offset', 'key', 'sequence'] as const).map((field) => (
              <button
                key={field}
                type="button"
                onClick={() => setSortField(field)}
                style={{
                  padding: '3px 8px',
                  borderRadius: 'var(--radius-sm)',
                  border: sortField === field ? '1px solid #0284c7' : '1px solid var(--border-subtle)',
                  background: sortField === field ? '#e0f2fe' : '#ffffff',
                  color: sortField === field ? '#0369a1' : 'var(--text-body)',
                  fontSize: '0.725rem',
                  fontFamily: 'var(--font-mono)',
                  cursor: 'pointer',
                  fontWeight: sortField === field ? 700 : 400,
                }}
              >
                {field}
              </button>
            ))}
          </div>
        </div>

        {/* Batch Invalidation Toolbar */}
        {selectedOffsets.length > 0 && (
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              flexWrap: 'wrap',
              gap: '12px',
              padding: '12px 16px',
              background: '#fff1f2',
              border: '1px solid #fecdd3',
              borderRadius: 'var(--radius-md)',
              marginBottom: '14px',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <span style={{ fontSize: '1rem', color: '#e11d48' }}>↩</span>
              <span style={{ fontSize: '0.85rem', fontWeight: 700, color: '#9f1239' }}>
                {selectedOffsets.length} Purchase{selectedOffsets.length > 1 ? 's' : ''} Selected
              </span>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
              <select
                value={invalidationReason}
                onChange={(e) => setInvalidationReason(e.target.value)}
                style={{
                  padding: '5px 10px',
                  fontSize: '0.785rem',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid var(--border-medium)',
                  background: '#ffffff',
                }}
              >
                <option value="DEMO_INVALIDATION">DEMO_INVALIDATION (Compensation)</option>
                <option value="FRAUD_CONFIRMED">FRAUD_CONFIRMED (Fraud Clawback)</option>
              </select>

              <button
                type="button"
                onClick={handleBatchInvalidate}
                disabled={isInvalidating}
                className="btn-cyber btn-rose"
                style={{ padding: '6px 14px', fontSize: '0.8rem', fontWeight: 700 }}
              >
                {isInvalidating ? 'Invalidating...' : `↩ Invalidate ${selectedOffsets.length} Selected`}
              </button>

              <button
                type="button"
                onClick={() => setSelectedOffsets([])}
                className="btn-cyber btn-ghost"
                style={{ padding: '6px 10px', fontSize: '0.785rem' }}
              >
                Deselect All
              </button>
            </div>
          </div>
        )}

        {/* Invalidation Status Result */}
        {invalidationStatus && (
          <div
            style={{
              padding: '10px 14px',
              borderRadius: 'var(--radius-sm)',
              background: invalidationStatus.type === 'success' ? '#ecfdf5' : '#fef2f2',
              border: invalidationStatus.type === 'success' ? '1px solid #a7f3d0' : '1px solid #fecaca',
              color: invalidationStatus.type === 'success' ? '#065f46' : '#991b1b',
              fontSize: '0.825rem',
              marginBottom: '14px',
            }}
          >
            {invalidationStatus.message}
          </div>
        )}

        {copiedNotification && (
          <div style={{ position: 'fixed', bottom: '24px', right: '24px', background: '#0f172a', color: '#38bdf8', padding: '8px 16px', borderRadius: 'var(--radius-sm)', fontSize: '0.8rem', fontFamily: 'var(--font-mono)', zIndex: 9999 }}>
            ✓ {copiedNotification}
          </div>
        )}

        {peekError && (
          <div style={{ padding: '10px 14px', borderRadius: 'var(--radius-sm)', background: '#fef2f2', border: '1px solid #fecaca', color: '#991b1b', fontSize: '0.8rem', marginBottom: '14px' }}>
            Failed to peek partition messages: {peekError}
          </div>
        )}

        {/* Message Table */}
        <PartitionMessageTable
          messages={processedMessages}
          selectedOffsets={selectedOffsets}
          selectableOffsets={selectableOffsets}
          allSelectableChecked={allSelectableChecked}
          expandedOffset={expandedOffset}
          isInvalidating={isInvalidating}
          onToggleSelectAll={toggleSelectAll}
          onToggleSelectOffset={toggleSelectOffset}
          onToggleExpandOffset={(offset) => setExpandedOffset(expandedOffset === offset ? null : offset)}
          onSingleInvalidate={handleSingleInvalidate}
          onCopy={copyToClipboard}
          getPurchaseDetails={getPurchaseDetails}
        />
      </section>

      {/* 4. Scale Partitions Modal */}
      <ScalePartitionsModal
        show={showScaleModal}
        onClose={() => setShowScaleModal(false)}
        topicName={topicName}
        partitionCount={partitionCount}
        targetCount={targetCount}
        setTargetCount={setTargetPartitionCount}
        scaling={scaling}
        scaleError={scaleError}
        scaleSuccess={scaleSuccess}
        onScale={handleScalePartitions}
      />
    </div>
  );
};
