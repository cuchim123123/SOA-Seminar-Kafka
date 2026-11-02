import { OnModuleDestroy, OnModuleInit, ServiceUnavailableException } from '@nestjs/common';

/** Structural port: services supply their existing pg driver; no new runtime dependency. */
export interface NotificationConnection {
  connect(): Promise<unknown>;
  query(sql: string): Promise<unknown>;
  end(): Promise<unknown>;
  on(event: 'notification', callback: (message: { channel: string }) => void): unknown;
  on(event: 'error' | 'end', callback: () => void): unknown;
}

export interface ChangeFeedOptions {
  channel: string;
  heartbeatMs: number;
  reconnectMs: number;
  coalesceMs: number;
  maxClients: number;
}

export interface EventStreamResponse {
  setHeader(name: string, value: string): unknown;
  flushHeaders(): void;
  write(data: string): boolean;
  end(): unknown;
  destroy(): unknown;
  once(event: 'close', callback: () => void): unknown;
}

/** Transient invalidations only. Every connection/reconnection must reload snapshots. */
export class PostgresChangeFeed implements OnModuleInit, OnModuleDestroy {
  private connection?: NotificationConnection;
  private ready = false;
  private stopped = false;
  private retry?: ReturnType<typeof setTimeout>;
  private flush?: ReturnType<typeof setTimeout>;
  private heartbeat?: ReturnType<typeof setInterval>;
  private readonly clients = new Set<EventStreamResponse>();

  constructor(
    private readonly options: ChangeFeedOptions,
    private readonly createConnection: () => NotificationConnection,
  ) {
    if (!/^[a-z_][a-z0-9_]{0,62}$/.test(options.channel))
      throw new Error('Invalid notification channel');
  }

  async onModuleInit(): Promise<void> {
    await this.connect();
    this.heartbeat = setInterval(() => {
      if (this.ready) this.broadcast(': heartbeat\n\n');
    }, this.options.heartbeatMs);
    this.heartbeat.unref?.();
  }

  private async connect(): Promise<void> {
    if (this.stopped) return;
    const connection = this.createConnection();
    this.connection = connection;
    connection.on('error', () => this.disconnected(connection));
    connection.on('end', () => this.disconnected(connection));
    connection.on('notification', (message) => {
      if (
        this.connection !== connection ||
        !this.ready ||
        message.channel !== this.options.channel ||
        this.flush
      )
        return;
      this.invalidate();
    });
    try {
      await connection.connect();
      // Validated identifier, not arbitrary caller SQL. LISTEN is committed before ready.
      await connection.query(`LISTEN "${this.options.channel}"`);
      if (this.connection === connection && !this.stopped) this.ready = true;
    } catch {
      this.disconnected(connection);
    }
  }

  /** Application-local controls have no database row, but still invalidate reads. */
  invalidate(): void {
    if (!this.ready || this.stopped || this.flush) return;
    this.flush = setTimeout(() => {
      this.flush = undefined;
      this.broadcast('event: resync-required\ndata: {}\n\n');
    }, this.options.coalesceMs);
  }

  private disconnected(connection: NotificationConnection): void {
    if (this.connection !== connection) return;
    this.connection = undefined;
    this.ready = false;
    if (this.flush) clearTimeout(this.flush);
    this.flush = undefined;
    // Never leave a healthy-looking SSE connection when its database feed is lost.
    for (const client of this.clients) client.end();
    this.clients.clear();
    void connection.end().catch(() => undefined);
    if (!this.stopped && !this.retry) {
      this.retry = setTimeout(() => {
        this.retry = undefined;
        void this.connect();
      }, this.options.reconnectMs);
      this.retry.unref?.();
    }
  }

  attach(response: EventStreamResponse): void {
    if (!this.ready || this.stopped)
      throw new ServiceUnavailableException(
        'Live updates unavailable; refresh manually or reconnect',
      );
    if (this.clients.size >= this.options.maxClients)
      throw new ServiceUnavailableException('Live update connection limit reached');
    response.setHeader('Content-Type', 'text/event-stream');
    response.setHeader('Cache-Control', 'no-cache, no-transform');
    response.setHeader('X-Accel-Buffering', 'no');
    response.flushHeaders();
    this.clients.add(response);
    response.once('close', () => this.clients.delete(response));
    // No durable notification log: never advertise resumable event IDs.
    this.write(response, 'event: resync-required\ndata: {}\n\n');
  }

  private write(response: EventStreamResponse, frame: string): void {
    try {
      if (response.write(frame)) return;
    } catch {
      /* A disconnected/slow peer must not affect other clients. */
    }
    this.clients.delete(response);
    response.destroy(); // Bound memory; native SSE reconnect requires a fresh snapshot.
  }

  private broadcast(frame: string): void {
    for (const client of this.clients) this.write(client, frame);
  }

  async onModuleDestroy(): Promise<void> {
    this.stopped = true;
    this.ready = false;
    if (this.retry) clearTimeout(this.retry);
    if (this.flush) clearTimeout(this.flush);
    if (this.heartbeat) clearInterval(this.heartbeat);
    for (const client of this.clients) client.end();
    this.clients.clear();
    const connection = this.connection;
    this.connection = undefined;
    await connection?.end().catch(() => undefined);
  }
}
