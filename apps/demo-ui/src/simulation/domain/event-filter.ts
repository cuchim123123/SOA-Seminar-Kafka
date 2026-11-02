import type { SimEvent } from './events';

export function filterEvents(
  events: SimEvent[],
  type: string,
  group: string,
  query: string,
): SimEvent[] {
  const text = query.trim().toLowerCase();
  return events.filter(
    (event) =>
      (!type || event.type === type) &&
      (!group || event.metadata?.groupId === group) &&
      (!text ||
        `${event.message?.id ?? ''} ${event.message?.key ?? ''} ${event.message?.payload.purchaseId ?? ''} ${event.description}`
          .toLowerCase()
          .includes(text)),
  );
}
