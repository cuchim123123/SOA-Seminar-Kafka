import { createContext, useContext, type ReactNode } from 'react';
import { useSimulationState } from './useSimulationStore';

type Session = ReturnType<typeof useSimulationState>;
const SessionContext = createContext<Session | null>(null);
export function SimulationSessionProvider({
  active,
  children,
}: {
  active: boolean;
  children: ReactNode;
}) {
  const session = useSimulationState(active);
  return <SessionContext.Provider value={session}>{children}</SessionContext.Provider>;
}
export function useSimulationStore() {
  const session = useContext(SessionContext);
  if (!session) throw new Error('Simulation session provider is missing.');
  return session;
}
