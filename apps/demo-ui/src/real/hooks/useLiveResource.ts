import { useEffect, useRef, useState } from 'react';
import { changeSubscriptions } from '../state/change-subscriptions';
import type { ConnectionState, LiveService } from '../state/change-subscriptions';
import { observeResource } from '../state/resource-observer';
import type { ResourceState } from '../state/resource-observer';

export function useLiveResource<T>(
  service: LiveService | null,
  key: string,
  load: (signal: AbortSignal) => Promise<T>,
  enabled = true,
) {
  const identity = `${service}:${key}`;
  const [state, setState] = useState<ResourceState<T> & { identity: string }>({
    identity,
    data: undefined,
    loading: true,
    error: null,
    observedAt: null,
  });
  const [connection, setConnection] = useState<ConnectionState>('connecting');
  const loader = useRef(load);
  loader.current = load;
  const refresh = useRef<() => void>(() => undefined);
  useEffect(() => {
    if (!enabled) {
      refresh.current = () => undefined;
      return;
    }
    const resourceIdentity = `${service}:${key}`;
    setState({
      identity: resourceIdentity,
      data: undefined,
      loading: true,
      error: null,
      observedAt: null,
    });
    const observer = observeResource(
      (signal) => loader.current(signal),
      (next) => setState({ ...next, identity: resourceIdentity }),
    );
    refresh.current = () => {
      void observer.refresh();
    };
    if (!service) {
      void observer.refresh();
      return () => {
        observer.stop();
        refresh.current = () => undefined;
      };
    }
    const unsubscribe = changeSubscriptions.subscribe(service, {
      changed: () => {
        void observer.refresh();
      },
      connection: setConnection,
    });
    return () => {
      observer.stop();
      unsubscribe();
      refresh.current = () => undefined;
    };
  }, [service, key, enabled]);
  return {
    ...state,
    // A new selection must never render the previous customer's snapshot, even
    // during the render before the effect cleanup/subscription has executed.
    ...(state.identity !== identity
      ? { data: undefined, loading: true, error: null, observedAt: null }
      : {}),
    connection,
    stale: (service !== null && connection !== 'live') || state.error !== null,
    refresh: () => refresh.current(),
  };
}
