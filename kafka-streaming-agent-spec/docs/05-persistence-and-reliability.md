# 05 — Persistence, Outbox, Ordering, Idempotency and Replay

## Phase boundary

PostgreSQL is **deliberately deferred until Phase 4**. In Phases 1–3 events are synthetic and may be lost during process failure; the acceptance tests do not claim reliable database-to-Kafka delivery. Phase 4 introduces the durability boundary and outbox. Maintain the external API contract while changing adapter implementation.

## Database ownership and schema

Use one PostgreSQL container in the seminar; isolate context tables by schema (or explicit prefixes). All money is integer cents and primary keys are deterministic synthetic IDs or generated UUID/ULID.

Suggested tables/constraints:

```sql
-- Illustrative DDL; migrations are the authoritative implementation.
CREATE TABLE purchase_customers (
  customer_id text PRIMARY KEY,
  next_event_sequence bigint NOT NULL DEFAULT 1
);

CREATE TABLE purchases (
  purchase_id text PRIMARY KEY,
  customer_id text NOT NULL REFERENCES purchase_customers(customer_id),
  amount_cents bigint NOT NULL CHECK (amount_cents > 0),
  status text NOT NULL CHECK (status IN ('COMPLETED', 'INVALIDATED')),
  completion_sequence bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (customer_id, completion_sequence)
);

CREATE TABLE http_idempotency_keys (
  customer_id text NOT NULL,
  idempotency_key text NOT NULL,
  request_fingerprint text NOT NULL,
  purchase_id text NOT NULL,
  PRIMARY KEY (customer_id, idempotency_key)
);

CREATE TABLE outbox_events (
  event_id text PRIMARY KEY,
  aggregate_id text NOT NULL,
  customer_sequence bigint NOT NULL,
  event_type text NOT NULL,
  payload_json jsonb NOT NULL,
  status text NOT NULL DEFAULT 'PENDING',
  attempt_count integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  UNIQUE (aggregate_id, customer_sequence)
);
CREATE INDEX outbox_pending_order ON outbox_events (status, created_at, aggregate_id, customer_sequence);

CREATE TABLE loyalty_accounts (
  customer_id text PRIMARY KEY,
  eligible_spend_cents bigint NOT NULL DEFAULT 0,
  points bigint NOT NULL DEFAULT 0,
  tier text NOT NULL DEFAULT 'BRONZE',
  last_customer_sequence bigint NOT NULL DEFAULT 0,
  correction_required boolean NOT NULL DEFAULT false
);

CREATE TABLE processed_events (
  consumer_name text NOT NULL,
  event_id text NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);
```

Additional `analytics_summary`, `replay_runs` and `loyalty_replay_staging` tables may be added only when corresponding phases begin. Prefer migrations committed to version control rather than creating tables on startup. DB-generated `created_at` is **not** a substitute for `customer_sequence`.

## Transactional source write

**Create purchase use case (Phase 4):**

1. Start Postgres transaction.
2. Create/find synthetic customer; lock its sequence row (`SELECT ... FOR UPDATE`).
3. Enforce unique HTTP idempotency key and request fingerprint; on exact duplicate return the existing purchase result; on payload mismatch return `409`.
4. Assign current `next_event_sequence`, increment it in the same transaction.
5. Insert purchase and outbox envelope with a stable event ID and assigned `customerSequence`.
6. Commit. API returns 202; Loyalty may still be pending.

**Invalidation (Phase 5):** inside the same customer-sequence lock transaction, change status once, assign the **new event sequence**, append `PurchaseInvalidated` to outbox. Repeated request should be idempotent under an explicit rule.

If two purchase requests for the same customer race, the lock serializes sequence assignment and commit order. The outbox publisher then respects that order.

## Outbox publisher lifecycle

- Start with **one publisher instance**. Read pending rows in per-customer event sequence order; ensure no higher sequence for a customer is sent before lower pending sequence. One global sequential stream ordered by committed outbox insertion ID is acceptable if insertion order is explicitly aligned with commit sequence; prefer implementation that checks lower-unpublished entries for same aggregate.
- Publish one logical event with stable `eventId` and `key=customerId`; await Kafka acknowledgment.
- After ack, mark outbox row `PUBLISHED`. On failure record attempts and backoff, do not lose the row.
- If publisher crashes after ack and before SQL update, republish on restart with the **same event ID**. Consumers must deduplicate.
- A publisher can see an older uncommitted transaction only after it commits; serialized customer row locks plus durable per-customer sequence prevent higher same-customer commit from leapfrogging.
- Avoid naive `FOR UPDATE SKIP LOCKED` across multiple publisher processes: claiming later same-customer rows can reorder events. Multi-publisher ordering is out of scope.
- Keep completed outbox for limited debugging; deletion is independent from Kafka retention. DB and Kafka records are intentionally not one distributed transaction.

## Consumer idempotency: atomic database effect

The reliable pattern is: **dedup insert + state mutation in one DB transaction; offset commit after transaction success**.

```sql
BEGIN;
INSERT INTO processed_events (consumer_name, event_id)
VALUES ('loyalty-v1', $1)
ON CONFLICT DO NOTHING
RETURNING event_id;
-- Application code must inspect rowCount/RETURNING.
-- If inserted: lock/create account, check sequence, apply points and update state.
-- If not inserted: DO NOT run any business update.
COMMIT;
-- Only after this: consumer.commitOffsets(offset + 1)
```

If there is a sequence gap or a transient issue, roll back the transaction. Do not mark the event processed. In a pure replay/read-only worker, use separate idempotency/run state; do not pollute live `processed_events` with historical attempts.

At-least-once scenarios to test:

- Delivery → DB committed → worker crash **before** Kafka offset commit → redelivery → duplicate skipped → commit offset.
- DB rollback/failure → offset **not** committed → retry after recovery.
- Outbox broker ack → publisher crash before DB published marker → Kafka duplicate logical event → no duplicate Loyalty effects.

Broker idempotent producer does not replace the DB uniqueness constraint. Neither Kafka nor NestJS CQRS alone gives atomicity across Postgres and Kafka.

## Sequence checking and invalidation

- `PurchaseCompleted` for customer sequence `s` must not be applied to normal live projection unless `s == last_customer_sequence + 1`, except verified duplicate `eventId`.
- `PurchaseInvalidated` is itself a next-sequence event. It marks `correction_required`, sets `last_customer_sequence` to `s` in the **same atomic transaction**, but must not directly subtract old points if tier changes cascade.
- Keep a small per-customer proof/test: P001 $120, P002 $200, P003 $100; 82 points before; after invalidating P001, recompute 40 points.
- If `PurchaseInvalidated` appears before a related purchase due to malformed publisher order, surface error; do not pretend Kafka automatically resolves business-time order.

## Bounded replay without event sourcing framework (Phase 5)

Replay is **a Kafka consumer capability demonstration** with a tiny state recomputation fixture:

1. Pause purchase generation/acceptance and wait for pending outbox to drain.
2. Record a consistent per-partition log-end offset (exclusive high-watermark) and wait for normal consumers to reach it. Store run metadata. This makes the dataset fixed for the seminar.
3. Replay pass A: from each partition's earliest retained offset up to the recorded high-watermark; collect invalidated purchase IDs per customer. Record the read range and whether required original purchase events are present.
4. Replay pass B: reread the same bounded range; apply **completed purchases not invalidated** in per-customer event order to an **isolated** staging model (`runId`, customer ID, computed points/tier/spend).
5. Compare staging to live projection; for the demo, atomically replace/switch a read-model projection only after validation and while writes remain paused.
6. Resume the normal system. Do not send notifications or duplicate `processed_events` in normal consumer namespace during replay.

The live consumer may mark `correction_required` when it sees an invalidation, then a controlled replay corrects the aggregate. It must not present a still-stale balance as corrected before replay completes.

**Important:** New group IDs avoid altering committed offsets of production consumer groups. `fromBeginning` begins at earliest **retained** offsets, not necessarily zero. If required events expired, fail replay with a clear `INSUFFICIENT_RETAINED_HISTORY` state. Do not silently substitute a Postgres query and claim it is Kafka replay.

## No hidden promises

- Redeemed rewards would need compensations; rewards redemption is absent from this demo.
- Normal DB-backed state is the current source of truth; Kafka event history is not a permanent legal/audit ledger.
- There is no cross-Postgres/Kafka exactly-once guarantee; the approach provides atomic state mutations plus deduplication under at-least-once delivery.

## Benchmark storage isolation and publication semantics

Benchmark raw run manifests, histogram/timeseries results and transient transport resources live outside the Purchase/Loyalty database schema (`infra/benchmark/results/` is sufficient initially). Do **not** implement Kafka-vs-Redis-vs-RabbitMQ numbers by altering Loyalty state or reusing purchase transactions for the transport-only test. Transport-only uses a no-op or deterministic checksum handler and broker-native confirmation/acknowledgment; end-to-end tests with PostgreSQL are a **different track**. Broker guarantees differ; no generic interface can make Redis AOF, RabbitMQ publisher confirms and Kafka `acks=all` equivalent durability without separately documented configuration. See docs 13–16.
