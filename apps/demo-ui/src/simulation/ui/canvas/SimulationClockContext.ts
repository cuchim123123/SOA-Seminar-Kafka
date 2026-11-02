import { createContext, useContext, useSyncExternalStore } from 'react';
import { VirtualClockStore, EMPTY_EDGE_FRAME } from '../../store/virtual-clock-store';
const fallback = new VirtualClockStore();
export const SimulationClockContext = createContext(fallback);
export function useEdgeFrame(id: string) {
  const clock = useContext(SimulationClockContext);
  return useSyncExternalStore(
    clock.subscribe,
    () => clock.getEdgeSnapshot(id),
    () => EMPTY_EDGE_FRAME,
  );
}
