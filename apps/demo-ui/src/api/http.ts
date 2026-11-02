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
  const headers = new Headers(options?.headers);
  if (options?.body !== undefined && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }
  // Deliberately no automatic retries: a disconnected mutation may have committed.
  const response = await fetch(url, { ...options, headers });
  if (!response.ok) {
    const details: unknown = await response.json().catch(() => null);
    const body = details && typeof details === 'object' ? (details as Record<string, unknown>) : {};
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
  return response.json() as Promise<T>;
}
