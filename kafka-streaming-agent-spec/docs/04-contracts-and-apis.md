# 04 — Integration Events, HTTP APIs and Control Contracts

## Scope

Keep the external API minimal. Strongly typed contracts exist so Kafka demonstrations are deterministic, observable and testable. Kafka integration events are **not** the same object types as domain entities or NestJS CQRS commands.

## Event envelope v1

Kafka topic: `purchase.events`. Kafka record `key`: exactly `customerId`. Record `value`: UTF-8 JSON representing a versioned envelope. No sensitive/real customer data.

```ts
export type PurchaseEventType = 'PurchaseCompleted' | 'PurchaseInvalidated';

export interface IntegrationEventEnvelopeV1<TPayload> {
  schemaVersion: 1;
  eventId: string;           // Stable UUID or ULID across retries
  eventType: PurchaseEventType;
  aggregateType: 'CustomerPurchaseStream';
  aggregateId: string;       // customerId, must equal Kafka key
  customerSequence: number;  // Monotonic sequence scoped to customerId
  occurredAt: string;       // ISO 8601 UTC; NOT the ordering authority
  correlationId: string;
  payload: TPayload;
}

export interface PurchaseCompletedPayloadV1 {
  purchaseId: string;
  customerId: string;
  amountCents: number;       // positive safe integer
}

export interface PurchaseInvalidatedPayloadV1 {
  purchaseId: string;
  customerId: string;
  reason: 'FRAUD_CONFIRMED' | 'DEMO_INVALIDATION';
}
```

Example value, record key `U001`:

```json
{
  "schemaVersion": 1,
  "eventId": "evt-0001",
  "eventType": "PurchaseCompleted",
  "aggregateType": "CustomerPurchaseStream",
  "aggregateId": "U001",
  "customerSequence": 1,
  "occurredAt": "2026-10-27T07:00:00.000Z",
  "correlationId": "req-0001",
  "payload": { "purchaseId": "P001", "customerId": "U001", "amountCents": 12000 }
}
```

For an invalidation, `customerSequence` is the **next event sequence** (e.g. 4), while `purchaseId` references the older purchase. Never reuse the original purchase sequence as the new invalidation event's position.

Recommended Kafka headers: `event-id`, `event-type`, `schema-version`, `correlation-id`, `content-type=application/json`. They duplicate searchable metadata; the envelope remains authoritative. Make header/payload mismatches a validation failure.

## Contract compatibility policy

- Runtime validate using versioned Zod schemas at producer input/output and consumer ingress.
- Unknown `schemaVersion` or `eventType`: reject and surface visibly, **no silent acknowledgment**.
- Backward-compatible additive fields only inside v1; breaking change requires new `schemaVersion` and explicit consumer support/migration.
- `eventId` unique per logical event; retries and outbox republication preserve event identity.
- No PII; the synthetic `customerId` is safe lab data.

## Demo HTTP API (planned)

All routes are under `/api/v1`. `202 Accepted` means the **source command was accepted**, not that downstream consumers finished. In Phases 1–3 the implementation may wait for broker acknowledgment; in Phase 4 it waits for DB+outbox commit. The response contract stays consistent.

| Method | Path | Description |
|---|---|---|
| `POST` | `/demo/purchases` | Create synthetic completed purchase; optional idempotency header from Phase 4. |
| `POST` | `/demo/purchases/:purchaseId/invalidate` | Invalidate an existing synthetic purchase (Phase 5). |
| `GET` | `/demo/purchases/:purchaseId` | Purchase status and publish status (Phase 4+). |
| `GET` | `/demo/loyalty/:customerId` | Read loyalty state/processing status. |
| `GET` | `/demo/sales-summary` | Read independent Analytics projection. |
| `GET` | `/health/live` | Process liveness. |
| `GET` | `/health/ready` | Readiness against required dependencies. |

`POST /api/v1/demo/purchases` request:

```http
POST /api/v1/demo/purchases
Content-Type: application/json
Idempotency-Key: local-req-0001

{"customerId":"U001","amountCents":12000}
```

Response:

```json
{
  "purchaseId": "P001",
  "requestStatus": "ACCEPTED",
  "eventId": "evt-0001",
  "correlationId": "req-0001"
}
```

Status codes: `202` accepted, `400` invalid format, `409` idempotency key reuse with conflicting payload / invalidation conflict, `404` unknown purchase, `503` failed to accept due to unavailable required durability boundary. Never respond 202 if no durable DB/outbox commit occurs in Phase 4.

## Demo control/telemetry API (local-only)

| Method | Path | Availability | Guardrail |
|---|---|---|---|
| `POST` | `/ops/load/start` | Phase 2+ | Max rate/count limits. |
| `POST` | `/ops/load/stop` | Phase 2+ | Best-effort graceful stop. |
| `GET` | `/ops/kafka/topics/:topic` | Phase 1+ | Broker metadata, real offsets. |
| `GET` | `/ops/consumer-groups` | Phase 3+ | Per group/partition lag. |
| `POST` | `/ops/workers/:name/pause` | Phase 3+ | Allowlisted worker only. |
| `POST` | `/ops/workers/:name/resume` | Phase 3+ | Allowlisted worker only. |
| `POST` | `/ops/workers/loyalty/scale` | Phase 6+ | Local-only approved worker count. |
| `POST` | `/ops/faults/consumer-crash` | Phase 4+ | Deterministic test hook. |
| `POST` | `/ops/replay-runs` | Phase 5+ | Unique run ID, bounded dataset. |
| `GET` | `/ops/replay-runs/:runId` | Phase 5+ | Real progress and status. |
| `POST` | `/ops/faults/stop-broker/:id` | Phase 7+ | Disabled by default; localhost-only allowlist. |

This is a **proposed contract**, not a mandate to implement all endpoints immediately. CLI equivalents are acceptable and preferred for Phase 1–6 when simpler.

## Telemetry contracts

```ts
interface PartitionPosition {
  topic: string;
  partition: number;
  logStartOffset: string;
  logEndOffset: string;
}

interface ConsumerGroupPosition {
  groupId: string;
  topic: string;
  partition: number;
  committedNextOffset: string | null;
  lagRecords: string | null; // null when not knowable
  assignedConsumerId?: string;
}
```

Offsets are **strings** to avoid JavaScript integer precision loss. Use strings in APIs and `BigInt` for calculations, then serialize to strings. React should display `UNKNOWN/UNCOMMITTED` when appropriate rather than fabricate zero.

## Example sequence for demo event set

```text
U001: seq=1 PurchaseCompleted(P001, 12000)
U001: seq=2 PurchaseCompleted(P002, 20000)
U001: seq=3 PurchaseCompleted(P003, 10000)
U001: seq=4 PurchaseInvalidated(P001)
```

Do not introduce unrelated domain event types just to make the architecture look complex.

## Benchmark contracts and API versioning

Benchmark commands, report schemas and safe observation endpoints are **separate** from the synthetic Purchase HTTP API. The normative typed `BenchmarkManifestV1`, `BenchmarkResultV1`, CLI and optional `/api/v1/benchmarks` read/command APIs are in `16-comparison-runbook-and-data-contracts.md`.

`runId`/`eventId` identify benchmark samples independent of Kafka topic/partition offsets, Redis Stream IDs or RabbitMQ delivery tags. The benchmark corpus uses the same serialized bytes across transports. A `BenchmarkResultV1` must never claim a result from an unexecuted run; `null` means not measured, `UNSUPPORTED` means native capability absent. Separate datasets and run IDs from `PurchaseCompleted` fixture IDs to avoid cross-contamination.
