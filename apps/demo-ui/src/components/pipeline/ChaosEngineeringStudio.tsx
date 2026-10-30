import React, { useState } from 'react';
import { api } from '../../api/client';

interface ChaosEngineeringStudioProps {
  onStatusUpdate: (message: string) => void;
}

export const ChaosEngineeringStudio: React.FC<ChaosEngineeringStudioProps> = ({ onStatusUpdate }) => {
  const [poisonKey, setPoisonKey] = useState<string>('corrupt_key_demo');
  const [injectingPoison, setInjectingPoison] = useState<boolean>(false);
  const [rawKey, setRawKey] = useState<string>('arbitrary_key_demo');
  const [rawPayload, setRawPayload] = useState<string>('{"experiment": "arbitrary_schema", "demo": true}');
  const [publishingRaw, setPublishingRaw] = useState<boolean>(false);

  const handleInjectPoison = async () => {
    setInjectingPoison(true);
    try {
      const res = await api.injectPoisonPill({ key: poisonKey.trim() || undefined });
      onStatusUpdate(
        `☠️ Poison Pill Published: Raw malformed non-JSON payload placed at partition ${res.partition}, offset ${res.offset} (key: "${res.key}"). Consumer caught PoisonPillError, isolated it, and committed offset N+1 safely.`,
      );
    } catch (err) {
      onStatusUpdate(`✕ Poison injection failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setInjectingPoison(false);
    }
  };

  const handlePublishRaw = async () => {
    setPublishingRaw(true);
    try {
      const res = await api.publishRawMessage({
        key: rawKey.trim() || `raw_${Date.now()}`,
        payload: rawPayload,
      });
      onStatusUpdate(
        `✓ Raw record published to "${res.topic}" partition ${res.partition}, offset ${res.offset} with arbitrary key "${res.key}".`,
      );
    } catch (err) {
      onStatusUpdate(`✕ Raw publish failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setPublishingRaw(false);
    }
  };

  return (
    <div
      style={{
        marginBottom: '16px',
        padding: '14px 18px',
        borderRadius: 'var(--radius-md)',
        background: '#fff1f2',
        border: '1px solid #fecdd3',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px', flexWrap: 'wrap', gap: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1.1rem', color: '#e11d48' }}>☠️</span>
          <span style={{ fontSize: '0.85rem', fontWeight: 700, color: '#9f1239' }}>
            Kafka Mechanics &amp; Chaos Engineering Studio
          </span>
        </div>
        <span className="badge-mono badge-rose">Poison Pills &amp; Raw Publishing</span>
      </div>

      <p style={{ fontSize: '0.775rem', color: '#881337', marginBottom: '10px', lineHeight: '1.4' }}>
        Test broker failure modes directly: inject non-JSON corrupted poison pills to prove consumer isolation (<code style={{ fontFamily: 'var(--font-mono)' }}>@lab/observability</code> commits offset N+1 without crash loops), or publish arbitrary raw payloads with custom keys.
      </p>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '12px', alignItems: 'center' }}>
        {/* Poison Pill Ingestion */}
        <div style={{ display: 'flex', gap: '6px', alignItems: 'center', flexWrap: 'wrap' }}>
          <input
            type="text"
            value={poisonKey}
            onChange={(e) => setPoisonKey(e.target.value)}
            placeholder="Poison Key..."
            style={{
              width: '140px',
              padding: '6px 8px',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid #f43f5e',
              fontSize: '0.775rem',
              fontFamily: 'var(--font-mono)',
              background: '#ffffff',
            }}
          />
          <button
            type="button"
            onClick={handleInjectPoison}
            disabled={injectingPoison}
            className="btn-cyber btn-rose"
            style={{ padding: '6px 12px', fontSize: '0.775rem', fontWeight: 700 }}
          >
            {injectingPoison ? 'Injecting...' : '☠️ Inject Poison Pill (Malformed Non-JSON)'}
          </button>
        </div>

        {/* Arbitrary Raw Message Ingestion */}
        <div style={{ display: 'flex', gap: '6px', alignItems: 'center', flexWrap: 'wrap' }}>
          <input
            type="text"
            value={rawKey}
            onChange={(e) => setRawKey(e.target.value)}
            placeholder="Custom Key..."
            style={{
              width: '120px',
              padding: '6px 8px',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-medium)',
              fontSize: '0.775rem',
              fontFamily: 'var(--font-mono)',
              background: '#ffffff',
            }}
          />
          <input
            type="text"
            value={rawPayload}
            onChange={(e) => setRawPayload(e.target.value)}
            placeholder='{"raw": "payload"}'
            style={{
              width: '180px',
              padding: '6px 8px',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-medium)',
              fontSize: '0.775rem',
              fontFamily: 'var(--font-mono)',
              background: '#ffffff',
            }}
          />
          <button
            type="button"
            onClick={handlePublishRaw}
            disabled={publishingRaw}
            className="btn-cyber btn-cyan"
            style={{ padding: '6px 12px', fontSize: '0.775rem', fontWeight: 600 }}
          >
            {publishingRaw ? 'Publishing...' : '📝 Publish Raw Record'}
          </button>
        </div>
      </div>
    </div>
  );
};
