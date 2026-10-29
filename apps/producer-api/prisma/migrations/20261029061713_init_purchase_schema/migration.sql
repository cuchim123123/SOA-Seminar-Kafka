-- CreateTable
CREATE TABLE "purchase_customers" (
    "customer_id" TEXT NOT NULL,
    "next_event_sequence" BIGINT NOT NULL DEFAULT 1,

    CONSTRAINT "purchase_customers_pkey" PRIMARY KEY ("customer_id")
);

-- CreateTable
CREATE TABLE "purchases" (
    "purchase_id" TEXT NOT NULL,
    "customer_id" TEXT NOT NULL,
    "amount_cents" BIGINT NOT NULL,
    "status" TEXT NOT NULL,
    "completion_sequence" BIGINT NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "purchases_pkey" PRIMARY KEY ("purchase_id")
);

-- CreateTable
CREATE TABLE "http_idempotency_keys" (
    "customer_id" TEXT NOT NULL,
    "idempotency_key" TEXT NOT NULL,
    "request_fingerprint" TEXT NOT NULL,
    "purchase_id" TEXT NOT NULL,

    CONSTRAINT "http_idempotency_keys_pkey" PRIMARY KEY ("customer_id","idempotency_key")
);

-- CreateTable
CREATE TABLE "outbox_events" (
    "event_id" TEXT NOT NULL,
    "aggregate_id" TEXT NOT NULL,
    "customer_sequence" BIGINT NOT NULL,
    "event_type" TEXT NOT NULL,
    "payload_json" JSONB NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "attempt_count" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "published_at" TIMESTAMPTZ,

    CONSTRAINT "outbox_events_pkey" PRIMARY KEY ("event_id")
);

-- CreateIndex
CREATE UNIQUE INDEX "purchases_customer_id_completion_sequence_key" ON "purchases"("customer_id", "completion_sequence");

-- CreateIndex
CREATE INDEX "outbox_pending_order" ON "outbox_events"("status", "created_at", "aggregate_id", "customer_sequence");

-- CreateIndex
CREATE UNIQUE INDEX "outbox_events_aggregate_id_customer_sequence_key" ON "outbox_events"("aggregate_id", "customer_sequence");

-- AddForeignKey
ALTER TABLE "purchases" ADD CONSTRAINT "purchases_customer_id_fkey" FOREIGN KEY ("customer_id") REFERENCES "purchase_customers"("customer_id") ON DELETE RESTRICT ON UPDATE CASCADE;
