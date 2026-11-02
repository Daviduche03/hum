export type LiveStatus =
  | "idle"
  | "connecting"
  | "live"
  | "reconnecting"
  | "closed";

export interface LiveSnapshot<T = unknown> {
  data: T | undefined;
  error: Error | undefined;
  loading: boolean;
  /** Cursor this data is at least as fresh as. */
  seq: number;
  updatedAt: number | undefined;
}

export interface LiveEntry<T = unknown> {
  key: string;
  /** Client-local id; the server echoes it in change events. */
  id: string;
  name: string;
  params: unknown;
  refs: number;
  fetcher: () => Promise<T>;
  snapshot: LiveSnapshot<T>;
  listeners: Set<() => void>;
  inFlight: boolean;
  dirty: boolean;
}

interface EventSourceLike {
  addEventListener(type: string, listener: (event: any) => void): void;
  close(): void;
}

export interface LiveClientOptions {
  basePath?: string;
  connectUrl?: string;
  streamUrl?: string;
  subUrl?: string;
  unsubUrl?: string;
  credentials?: RequestCredentials;
  fetch?: typeof fetch;
  EventSource?: new (url: string, init?: any) => EventSourceLike;
  retryBaseMs?: number;
  retryMaxMs?: number;
  onError?: (error: Error) => void;
  onStatus?: (status: LiveStatus) => void;
}

export function emptySnapshot<T>(): LiveSnapshot<T> {
  return {
    data: undefined,
    error: undefined,
    loading: true,
    seq: 0,
    updatedAt: undefined,
  };
}

/**
 * One connection per tab, shared by every `useLiveQuery`.
 *
 * Hooks refcount subscriptions: twenty components asking for the same query
 * produce one server subscription and one fetch.
 */
export class LiveClient {
  readonly options: LiveClientOptions;
  private readonly entries = new Map<string, LiveEntry<any>>();
  private readonly byId = new Map<string, LiveEntry<any>>();
  private readonly statusListeners = new Set<(status: LiveStatus) => void>();

  private status: LiveStatus = "idle";
  private connId: string | null = null;
  private source: EventSourceLike | null = null;
  private connecting: Promise<void> | null = null;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private retry = 0;
  private counter = 0;
  private generation = 0;
  private disposed = false;
  private everConnected = false;
  private hiddenAt: number | null = null;

  constructor(options: LiveClientOptions = {}) {
    this.options = options;
    this.installVisibilityHandler();
  }

  getStatus(): LiveStatus {
    return this.status;
  }

  onStatus(listener: (status: LiveStatus) => void): () => void {
    this.statusListeners.add(listener);
    return () => this.statusListeners.delete(listener);
  }

  keyFor(name: string, params: unknown): string {
    return `${name}\u0000${stableStringify(params ?? null)}`;
  }

  /**
   * Register interest in a query. Returns the shared entry; call `release`
   * when the last consumer goes away.
   */
  entry<T>(
    name: string,
    params: unknown,
    fetcher: () => Promise<T>,
  ): LiveEntry<T> {
    const key = this.keyFor(name, params);
    let entry = this.entries.get(key) as LiveEntry<T> | undefined;

    if (!entry) {
      entry = {
        key,
        id: `q${++this.counter}`,
        name,
        params,
        refs: 0,
        fetcher,
        snapshot: emptySnapshot<T>(),
        listeners: new Set(),
        inFlight: false,
        dirty: false,
      };
      this.entries.set(key, entry);
      this.byId.set(entry.id, entry);
    }

    entry.refs += 1;

    if (entry.refs === 1) {
      entry.fetcher = fetcher;
      this.ensureConnected();
      if (this.status === "live") this.sendSub(entry);
      void this.refetchEntry(entry);
    }

    return entry;
  }

  /** Release one reference. The query is unsubscribed at zero. */
  release(entry: LiveEntry<any>): void {
    entry.refs -= 1;
    if (entry.refs > 0) return;

    this.entries.delete(entry.key);
    this.byId.delete(entry.id);

    if (this.status === "live" && this.connId) {
      void this.post(this.unsubUrl, { id: entry.id }).catch(() => {});
    }
  }

  subscribe<T>(
    name: string,
    params: unknown,
    fetcher: () => Promise<T>,
  ): () => void {
    const entry = this.entry(name, params, fetcher);
    return () => this.release(entry);
  }

  refetchEntry(entry: LiveEntry<any>): Promise<void> {
    if (entry.inFlight) {
      entry.dirty = true;
      return Promise.resolve();
    }

    entry.inFlight = true;
    if (entry.snapshot.data === undefined) {
      this.patch(entry, { loading: true });
    }

    let result: Promise<unknown>;
    try {
      result = entry.fetcher();
    } catch (error) {
      result = Promise.reject(error);
    }

    return Promise.resolve(result)
      .then(
        (data) => {
          this.patch(entry, {
            data,
            error: undefined,
            loading: false,
            updatedAt: Date.now(),
          });
        },
        (error) => {
          this.patch(entry, { error: toError(error), loading: false });
        },
      )
      .finally(() => {
        entry.inFlight = false;
        if (entry.dirty) {
          entry.dirty = false;
          void this.refetchEntry(entry);
        }
      });
  }

  refetch(name: string, params: unknown): Promise<void> {
    const entry = this.entries.get(this.keyFor(name, params));
    if (!entry) return Promise.resolve();
    return this.refetchEntry(entry);
  }

  dispose(): void {
    this.disposed = true;
    this.generation += 1;
    this.source?.close();
    this.source = null;
    if (this.retryTimer) clearTimeout(this.retryTimer);
    this.retryTimer = null;
    this.connId = null;
    this.entries.clear();
    this.byId.clear();
    this.setStatus("closed");
    this.removeVisibilityHandler();
  }

  // ---------------------------------------------------------------- internals

  private get base(): string {
    return (this.options.basePath ?? "/live").replace(/\/$/, "");
  }

  private get connectUrl(): string {
    return this.options.connectUrl ?? `${this.base}/connect`;
  }

  private get streamUrl(): string {
    return this.options.streamUrl ?? `${this.base}/stream`;
  }

  private get subUrl(): string {
    return this.options.subUrl ?? `${this.base}/sub`;
  }

  private get unsubUrl(): string {
    return this.options.unsubUrl ?? `${this.base}/unsub`;
  }

  private get fetchImpl(): typeof fetch {
    return this.options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  private ensureConnected(): void {
    if (this.disposed || this.status === "live" || this.connecting) return;

    const attempt = this.connect().catch((error) => {
      this.onError(toError(error));
      this.setStatus("reconnecting");
      this.scheduleRetry();
    });

    let chained: Promise<void>;
    chained = attempt.finally(() => {
      if (this.connecting === chained) this.connecting = null;
    });
    this.connecting = chained;
    void this.connecting;
  }

  private async connect(): Promise<void> {
    this.setStatus(this.everConnected ? "reconnecting" : "connecting");

    const response = await this.fetchImpl(this.connectUrl, {
      method: "POST",
      credentials: this.options.credentials ?? "same-origin",
      headers: { "content-type": "application/json" },
      body: "{}",
    });

    if (!response.ok) {
      throw new Error(`live: connect failed (${response.status})`);
    }

    const body = (await response.json()) as { connId: string; ticket: string };
    if (this.disposed) return;

    const wasConnected = this.everConnected;
    this.connId = body.connId;
    this.everConnected = true;
    this.retry = 0;

    this.openStream(body.ticket);
    this.setStatus("live");

    // Re-subscribe everything. Reconnect means we may have missed changes, so
    // refetch as well.
    for (const entry of this.entries.values()) {
      if (entry.refs > 0) this.sendSub(entry);
      if (wasConnected) void this.refetchEntry(entry);
    }
  }

  private openStream(ticket: string): void {
    const EventSourceImpl =
      this.options.EventSource ??
      (globalThis as { EventSource?: new (url: string, init?: any) => EventSourceLike })
        .EventSource;

    if (!EventSourceImpl) {
      throw new Error("live: no EventSource implementation available");
    }

    this.generation += 1;
    const generation = this.generation;

    this.source?.close();
    const source = new EventSourceImpl(
      `${this.streamUrl}?ticket=${encodeURIComponent(ticket)}`,
      { withCredentials: true },
    );
    this.source = source;

    source.addEventListener("hello", (event) => {
      if (generation !== this.generation) return;
      try {
        const data = JSON.parse(event.data) as { seq: number };
        if (data.seq) this.setCursor(data.seq);
      } catch {
        // ignore malformed frames
      }
    });

    source.addEventListener("change", (event) => {
      if (generation !== this.generation) return;
      try {
        const data = JSON.parse(event.data) as { seq: number; ids: string[] };
        this.onChange(data.seq, data.ids ?? []);
      } catch {
        // ignore malformed frames
      }
    });

    source.addEventListener("resync", () => {
      if (generation !== this.generation) return;
      this.refetchAll();
    });

    source.addEventListener("error", () => {
      if (generation !== this.generation) return;
      this.onDisconnect();
    });
  }

  private onDisconnect(): void {
    if (this.disposed) return;
    this.generation += 1;
    this.source?.close();
    this.source = null;
    this.connId = null;
    this.setStatus("reconnecting");
    this.scheduleRetry();
  }

  private scheduleRetry(): void {
    if (this.disposed || this.retryTimer) return;

    this.retry += 1;
    const base = this.options.retryBaseMs ?? 300;
    const max = this.options.retryMaxMs ?? 10_000;
    const ceiling = Math.min(max, base * 2 ** (this.retry - 1));
    const delay = ceiling / 2 + Math.random() * (ceiling / 2);

    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.ensureConnected();
    }, delay);
  }

  private onChange(seq: number, ids: string[]): void {
    for (const id of ids) {
      const entry = this.byId.get(id);
      if (!entry) continue;
      entry.snapshot = { ...entry.snapshot, seq };
      void this.refetchEntry(entry);
    }
  }

  private setCursor(seq: number): void {
    for (const entry of this.entries.values()) {
      entry.snapshot = { ...entry.snapshot, seq };
    }
  }

  private refetchAll(): void {
    for (const entry of this.entries.values()) {
      void this.refetchEntry(entry);
    }
  }

  private sendSub(entry: LiveEntry<any>): void {
    void this.post(this.subUrl, {
      id: entry.id,
      name: entry.name,
      params: entry.params ?? null,
    })
      .then((response) => {
        const seq = Number((response as { seq?: number }).seq ?? 0);
        if (seq) this.patch(entry, { seq });
      })
      .catch((error) => {
        this.patch(entry, { error: toError(error), loading: false });
      });
  }

  private async post(path: string, body: Record<string, unknown>): Promise<unknown> {
    if (!this.connId) throw new Error("live: not connected");
    const response = await this.fetchImpl(path, {
      method: "POST",
      credentials: this.options.credentials ?? "same-origin",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ connId: this.connId, ...body }),
    });
    if (!response.ok) {
      throw new Error(`live: ${path} failed (${response.status})`);
    }
    return response.json();
  }

  private patch(entry: LiveEntry<any>, partial: Partial<LiveSnapshot>): void {
    entry.snapshot = { ...entry.snapshot, ...partial } as LiveSnapshot;
    for (const listener of entry.listeners) listener();
  }

  private setStatus(status: LiveStatus): void {
    if (this.status === status) return;
    this.status = status;
    this.options.onStatus?.(status);
    for (const listener of this.statusListeners) listener(status);
  }

  private onError(error: Error): void {
    this.options.onError?.(error);
  }

  private installVisibilityHandler(): void {
    const doc = (globalThis as { document?: Document }).document;
    if (!doc?.addEventListener) return;

    this.visibilityHandler = () => {
      if (doc.visibilityState === "hidden") {
        this.hiddenAt = Date.now();
        return;
      }
      const hiddenFor = this.hiddenAt ? Date.now() - this.hiddenAt : 0;
      this.hiddenAt = null;
      // Browsers kill streams in the background without always firing error.
      if (hiddenFor > 10_000) this.refetchAll();
    };
    doc.addEventListener("visibilitychange", this.visibilityHandler);
  }

  private removeVisibilityHandler(): void {
    const doc = (globalThis as { document?: Document }).document;
    if (doc?.removeEventListener && this.visibilityHandler) {
      doc.removeEventListener("visibilitychange", this.visibilityHandler);
    }
    this.visibilityHandler = null;
  }

  private visibilityHandler: (() => void) | null = null;
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function stableStringify(value: unknown): string {
  return JSON.stringify(normalize(value));
}

function normalize(value: unknown): unknown {
  if (value === undefined || value === null) return null;
  if (Array.isArray(value)) return value.map(normalize);
  if (typeof value === "object") {
    const input = value as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(input).sort()) {
      if (input[key] === undefined) continue;
      out[key] = normalize(input[key]);
    }
    return out;
  }
  return value;
}
