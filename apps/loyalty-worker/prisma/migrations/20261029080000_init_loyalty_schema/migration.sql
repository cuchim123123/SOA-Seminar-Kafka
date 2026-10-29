-- CreateTable
CREATE TABLE "loyalty_accounts" (
    "customer_id" TEXT NOT NULL,
    "eligible_spend_cents" BIGINT NOT NULL DEFAULT 0,
    "points" BIGINT NOT NULL DEFAULT 0,
    "tier" TEXT NOT NULL DEFAULT 'BRONZE',
    "last_customer_sequence" BIGINT NOT NULL DEFAULT 0,
    "correction_required" BOOLEAN NOT NULL DEFAULT false,

    CONSTRAINT "loyalty_accounts_pkey" PRIMARY KEY ("customer_id")
);

-- CreateTable
CREATE TABLE "processed_events" (
    "consumer_name" TEXT NOT NULL,
    "event_id" TEXT NOT NULL,
    "processed_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_events_pkey" PRIMARY KEY ("consumer_name","event_id")
);
