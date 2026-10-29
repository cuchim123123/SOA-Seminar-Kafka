-- CreateTable
CREATE TABLE IF NOT EXISTS "analytics_summary" (
    "id" TEXT NOT NULL DEFAULT 'global',
    "total_revenue_cents" BIGINT NOT NULL DEFAULT 0,
    "purchase_count" BIGINT NOT NULL DEFAULT 0,
    "customer_ids" JSONB NOT NULL DEFAULT '[]',
    "last_event_id" TEXT,
    "last_processed_at" TIMESTAMPTZ,
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_summary_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE IF NOT EXISTS "processed_events" (
    "consumer_name" TEXT NOT NULL,
    "event_id" TEXT NOT NULL,
    "processed_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_events_pkey" PRIMARY KEY ("consumer_name","event_id")
);
