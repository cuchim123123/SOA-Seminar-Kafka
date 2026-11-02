import { uiConfig } from '../config/ui-config';
/** Preserve HTTP evidence; a failed request is never an empty projection. */
export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly details: unknown,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const controller = new AbortController();
  const cancel = () => controller.abort(options?.signal?.reason);
  if (options?.signal?.aborted) cancel();
  options?.signal?.addEventListener('abort', cancel, { once: true });
  const timeout = setTimeout(
    () =>
      controller.abort(
        new DOMException(
          'Request timed out. A submitted operation may still have committed; reconcile before retrying.',
          'TimeoutError',
        ),
      ),
    uiConfig.VITE_HTTP_TIMEOUT_MS,
  );
  try {
    const headers = new Headers(options?.headers);
    if (options?.body !== undefined && !headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json');
    }
    // Deliberately no automatic retries: a disconnected mutation may have committed.
    const response = await fetch(url, { ...options, headers, signal: controller.signal });
    if (!response.ok) {
      const details: unknown = await response.json().catch(() => null);
      const body =
        details && typeof details === 'object' ? (details as Record<string, unknown>) : {};
      const message = body.message ?? body.error;
      throw new ApiError(
        typeof message === 'string'
          ? message
          : Array.isArray(message)
            ? message.join('; ')
            : `Request failed: ${response.status} ${response.statusText}`,
        response.status,
        details,
      );
    }
    return (await response.json()) as T;
  } finally {
    clearTimeout(timeout);
    options?.signal?.removeEventListener('abort', cancel);
  }
}
