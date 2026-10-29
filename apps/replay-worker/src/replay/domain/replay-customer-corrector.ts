import { uuidv7 } from 'uuidv7';
import { pointsForPurchase, Tier, tierForEligibleSpend } from './loyalty-policy';
import { ReplayStagingRecord } from './replay-staging-record.entity';

export interface ReplayInputEvent {
  readonly eventType: string;
  readonly aggregateId: string;
  readonly customerSequence: number;
  readonly payload: {
    readonly customerId?: string;
    readonly purchaseId?: string;
    readonly amountCents?: number;
    readonly reason?: string;
  };
}

export class ReplayCustomerCorrector {
  /**
   * Executes two-pass stream correction over a bounded set of historical Kafka events:
   *
   * Pass A:
   * - Scans all events to collect invalidated purchase IDs per customer.
   * - Collects and partitions completed purchases per customer.
   *
   * Pass B:
   * - For each customer, replays valid (non-invalidated) purchases in ascending customerSequence order.
   * - Correctly cascades tier multipliers (BRONZE -> SILVER -> GOLD) based on actual eligible spend.
   * - Produces isolated ReplayStagingRecord instances ready for staging persistence.
   */
  static executeTwoPassCorrection(params: { runId: string; events: ReplayInputEvent[] }): {
    stagingRecords: ReplayStagingRecord[];
    invalidatedPurchaseIds: Set<string>;
    processedCustomerIds: string[];
  } {
    const { runId, events } = params;

    // ── PASS A: Collect invalidations and group events by customer ──────────
    const invalidatedPurchaseIds = new Set<string>();
    const customerEventsMap = new Map<string, ReplayInputEvent[]>();

    for (const event of events) {
      const customerId = event.payload.customerId || event.aggregateId;
      if (!customerId) continue;

      if (!customerEventsMap.has(customerId)) {
        customerEventsMap.set(customerId, []);
      }
      customerEventsMap.get(customerId)!.push(event);

      const isInvalidated =
        event.eventType === 'PurchaseInvalidated' ||
        event.eventType === 'lab.purchase.invalidated.v1';

      if (isInvalidated && event.payload.purchaseId) {
        invalidatedPurchaseIds.add(event.payload.purchaseId);
      }
    }

    // ── PASS B: Compute corrected state per customer in sequence order ──────
    const stagingRecords: ReplayStagingRecord[] = [];
    const processedCustomerIds: string[] = [];

    for (const [customerId, customerEvents] of customerEventsMap.entries()) {
      processedCustomerIds.push(customerId);

      // Sort strictly by customerSequence ascending
      const sortedEvents = [...customerEvents].sort(
        (a, b) => a.customerSequence - b.customerSequence,
      );

      let eligibleSpendCents = 0;
      let totalPoints = 0;
      let currentTier: Tier = 'BRONZE';
      let maxSequence = 0;

      for (const event of sortedEvents) {
        if (event.customerSequence > maxSequence) {
          maxSequence = event.customerSequence;
        }

        const isPurchase =
          event.eventType === 'PurchaseCompleted' ||
          event.eventType === 'lab.purchase.completed.v1';

        if (!isPurchase) {
          // Invalidation or other auxiliary event: sequence advanced, but no points
          continue;
        }

        const purchaseId = event.payload.purchaseId;
        const amountCents = event.payload.amountCents ?? 0;

        // Skip invalidated purchases!
        if (purchaseId && invalidatedPurchaseIds.has(purchaseId)) {
          continue;
        }

        if (amountCents > 0) {
          // 1. Calculate tier based on spend BEFORE current purchase
          currentTier = tierForEligibleSpend(eligibleSpendCents);

          // 2. Calculate points earned using current tier multiplier
          const earned = pointsForPurchase(amountCents, currentTier);

          // 3. Accumulate spend and points
          totalPoints += earned;
          eligibleSpendCents += amountCents;

          // 4. Update tier for subsequent purchases
          currentTier = tierForEligibleSpend(eligibleSpendCents);
        }
      }

      const stagingRecord = new ReplayStagingRecord({
        id: uuidv7(),
        runId,
        customerId,
        points: BigInt(totalPoints),
        tier: currentTier,
        eligibleSpendCents: BigInt(eligibleSpendCents),
        scannedEventCount: sortedEvents.length,
        lastCustomerSequence: BigInt(maxSequence),
        createdAt: new Date(),
      });

      stagingRecords.push(stagingRecord);
    }

    return {
      stagingRecords,
      invalidatedPurchaseIds,
      processedCustomerIds,
    };
  }
}
