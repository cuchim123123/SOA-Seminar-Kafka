-- AlterTable
ALTER TABLE "purchases" ADD COLUMN "invalidation_sequence" BIGINT;
ALTER TABLE "purchases" ADD COLUMN "invalidation_event_id" TEXT;
ALTER TABLE "purchases" ADD COLUMN "invalidation_reason" TEXT;
