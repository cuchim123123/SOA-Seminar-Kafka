-- CreateTable
CREATE TABLE "replay_runs" (
    "id" TEXT NOT NULL,
    "group_id" TEXT NOT NULL,
    "topic" TEXT NOT NULL DEFAULT 'purchase.events',
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "start_offsets" JSONB NOT NULL,
    "high_watermarks" JSONB NOT NULL,
    "scanned_count" INTEGER NOT NULL DEFAULT 0,
    "error_message" TEXT,
    "started_at" TIMESTAMPTZ,
    "completed_at" TIMESTAMPTZ,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "replay_runs_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "replay_staging" (
    "id" TEXT NOT NULL,
    "run_id" TEXT NOT NULL,
    "customer_id" TEXT NOT NULL,
    "points" BIGINT NOT NULL DEFAULT 0,
    "tier" TEXT NOT NULL DEFAULT 'BRONZE',
    "eligible_spend_cents" BIGINT NOT NULL DEFAULT 0,
    "scanned_event_count" INTEGER NOT NULL DEFAULT 0,
    "last_customer_sequence" BIGINT NOT NULL DEFAULT 0,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "replay_staging_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "replay_staging_run_id_customer_id_key" ON "replay_staging"("run_id", "customer_id");

-- AddForeignKey
ALTER TABLE "replay_staging" ADD CONSTRAINT "replay_staging_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "replay_runs"("id") ON DELETE CASCADE ON UPDATE CASCADE;
