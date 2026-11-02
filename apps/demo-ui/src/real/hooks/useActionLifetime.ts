import { useEffect, useRef } from 'react';

/** An HTTP mutation may keep running after navigation; its UI callback must not. */
export function useActionLifetime() {
  const active = useRef<object | null>(null);
  useEffect(() => {
    active.current = {};
    return () => {
      active.current = null;
    };
  }, []);
  return () => {
    const generation = active.current;
    return () => generation !== null && active.current === generation;
  };
}
