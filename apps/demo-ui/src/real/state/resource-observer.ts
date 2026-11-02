export interface ResourceState<T> {
  data: T | undefined;
  loading: boolean;
  error: string | null;
  observedAt: string | null;
}

/** Snapshot reads driven by invalidations, with no timer and no lost dirty signal. */
export function observeResource<T>(
  load: (signal: AbortSignal) => Promise<T>,
  update: (state: ResourceState<T>) => void,
) {
  const abort = new AbortController();
  let active = true;
  let pending = false;
  let dirty = false;
  let state: ResourceState<T> = { data: undefined, loading: false, error: null, observedAt: null };
  const refresh = async () => {
    if (!active) return;
    dirty = true;
    if (pending) return;
    pending = true;
    try {
      while (active && dirty) {
        dirty = false;
        state = { ...state, loading: true };
        update(state);
        try {
          const data = await load(abort.signal);
          if (!active) return;
          state = { data, loading: false, error: null, observedAt: new Date().toISOString() };
        } catch (error) {
          if (!active) return;
          state = {
            ...state,
            loading: false,
            error: error instanceof Error ? error.message : String(error),
          };
        }
        update(state);
      }
    } finally {
      pending = false;
    }
  };
  return {
    refresh,
    stop: () => {
      active = false;
      abort.abort();
    },
  };
}
