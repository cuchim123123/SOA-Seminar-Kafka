import { createContext, useContext, useSyncExternalStore } from 'react';
import { VirtualClockStore } from '../../store/virtual-clock-store';

const fallback = new VirtualClockStore();
export const SimulationClockContext = createContext(fallback);
const noSubscription = () => () => {};
const zero = () => 0;
export function useVirtualTime(active = true) {
  const clock = useContext(SimulationClockContext);
  return useSyncExternalStore(
    active ? clock.subscribe : noSubscription,
    active ? clock.getSnapshot : zero,
    zero,
  );
}
