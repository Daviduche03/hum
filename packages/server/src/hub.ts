import {
  compact,
  currentSeq,
  stableStringify,
  Tailer,
  type ChangeBatch,
  type LiveDb,
} from "@hum/core";
import { Connection } from "./connection.ts";
import {
  deny,
  LiveHubError,
  type Ctx,
  type QueryDef,
  type QueryRegistry,
} from "./types.ts";

export interface HubOptions {
  db: LiveDb;
  queries: QueryRegistry;
  outbox?: string;
  meta?: string;
  /** Poll interval for the outbox tailer. Default 250ms. */
  pollMs?: number;
  /**
   * Reap connections that never opened a stream. Default 30s. 0 disables.
   *
   * A client that POSTs `/connect` and then dies before opening the
   * `EventSource` would otherwise sit in memory forever.
   */
  connectionTtlMs?: number;
  /** SSE keepalive interval. Default 25s. */
  heartbeatMs?: number;
  /** How often retention runs. Default 5s. 0 disables. */
  compactEveryMs?: number;
  /** Rows of history to keep even with no readers. Default 1000. */
  keep?: number;
  /**
   * Stable identity bucket for subscription dedupe.
   *
   * Two contexts with the same scope can share a subscription. Set this to
   * your tenant id. With a shared multi-tenant DB, leaving it at the default
   * means users receive timing notifications for each other's writes (never
   * data) and do redundant refetches.
   */
  scope?: (ctx: Ctx) => string;
  /** Session identity, used to verify a POST belongs to the SSE connection. */
  identity?: (ctx: Ctx) => string;
}

interface Subscription {
  key: string;
  name: string;
  tables: Set<string>;
  /** connId -> client-local ids that want this query */
  conns: Map<string, Set<string>>;
}

function randomId(): string {
  return globalThis.crypto?.randomUUID
    ? globalThis.crypto.randomUUID()
    : Math.random().toString(36).slice(2);
}

export class LiveHub {
  readonly options: HubOptions;
  readonly heartbeatMs: number;
  readonly connections = new Map<string, Connection>();

  private readonly db: LiveDb;
  private readonly queries: QueryRegistry;
  private readonly outbox: string;
  private readonly meta: string;
  private readonly scopeOf: (ctx: Ctx) => string;
  private readonly identityOf: (ctx: Ctx) => string;
  private readonly tailer: Tailer;

  private readonly subs = new Map<string, Subscription>();
  private readonly byTable = new Map<string, Set<Subscription>>();
  private readonly localIndex = new Map<string, Subscription>();
  private compactTimer: ReturnType<typeof setInterval> | null = null;

  constructor(options: HubOptions) {
    this.options = options;
    this.db = options.db;
    this.queries = options.queries;
    this.outbox = options.outbox ?? "_live_changes";
    this.meta = options.meta ?? "_live_meta";
    this.heartbeatMs = options.heartbeatMs ?? 25_000;
    this.scopeOf = options.scope ?? (() => "");
    this.identityOf =
      options.identity ??
      ((ctx) => String(ctx.userId ?? ctx.sessionId ?? ctx.orgId ?? ""));

    // Fail at boot, not on the first subscription: a query without tables can
    // never be invalidated, and silently never waking up is the worst failure
    // this library can have.
    for (const [name, def] of Object.entries(this.queries)) {
      if (def.tables === undefined) {
        throw new LiveHubError(
          "E_CONFIG",
          `query "${name}" needs \`tables\``,
          500,
        );
      }
    }

    this.tailer = new Tailer({
      db: this.db,
      outbox: this.outbox,
      meta: this.meta,
      pollMs: options.pollMs ?? 250,
      onBatch: (batch) => this.onBatch(batch),
      onResync: (info) => {
        for (const connection of this.connections.values()) {
          connection.lastSeq = info.seq;
          connection.send("resync", info);
        }
      },
    });
  }

  start(): void {
    this.tailer.start();
    const every = this.options.compactEveryMs ?? 5_000;
    if (every > 0) {
      this.compactTimer = setInterval(() => this.runRetention(), every);
      (this.compactTimer as { unref?: () => void }).unref?.();
    }
  }

  stop(): void {
    this.tailer.stop();
    if (this.compactTimer) clearInterval(this.compactTimer);
    this.compactTimer = null;
    for (const connection of this.connections.values()) connection.close();
    this.connections.clear();
    this.subs.clear();
    this.byTable.clear();
    this.localIndex.clear();
  }

  /** Read the outbox once, right now. Call after in-process writes. */
  flush(): void {
    this.tailer.poll();
  }

  createConnection(ctx: Ctx): Connection {
    const connection = new Connection(
      randomId(),
      ctx,
      this.identityOf(ctx),
      currentSeq(this.db, this.outbox),
    );
    this.connections.set(connection.id, connection);
    return connection;
  }

  disconnect(connId: string): void {
    const connection = this.connections.get(connId);
    if (!connection) return;

    for (const localId of [...this.localIds(connId)]) {
      this.unsubscribe(connId, localId);
    }
    connection.close();
    this.connections.delete(connId);
  }

  async subscribe(
    connId: string,
    localId: string,
    name: string,
    rawParams: unknown,
  ): Promise<{ seq: number; tables: string[] }> {
    const connection = this.connections.get(connId);
    if (!connection) throw new LiveHubError("E_CONN", "unknown connection", 410);

    const def = this.queries[name];
    if (!def) throw deny();

    let params: unknown = rawParams;
    if (def.params) {
      try {
        params = def.params(rawParams);
      } catch {
        throw new LiveHubError("E_PARAMS", "invalid params", 400);
      }
    }

    if (def.authorize && !(await def.authorize(connection.ctx, params))) {
      throw deny();
    }

    let tables: string[];
    try {
      tables = this.tablesFor(name, def, connection.ctx, params);
    } catch {
      throw deny();
    }

    const seq = currentSeq(this.db, this.outbox);
    const key = `${this.scopeOf(connection.ctx)}\u0000${name}\u0000${stableStringify(
      params ?? null,
    )}`;

    this.unsubscribe(connId, localId);

    let subscription = this.subs.get(key);
    if (!subscription) {
      subscription = {
        key,
        name,
        tables: new Set(tables),
        conns: new Map(),
      };
      this.subs.set(key, subscription);
      for (const table of tables) {
        let bucket = this.byTable.get(table);
        if (!bucket) {
          bucket = new Set();
          this.byTable.set(table, bucket);
        }
        bucket.add(subscription);
      }
    }

    let ids = subscription.conns.get(connId);
    if (!ids) {
      ids = new Set();
      subscription.conns.set(connId, ids);
    }
    ids.add(localId);
    this.localIndex.set(`${connId}\u0000${localId}`, subscription);

    return { seq, tables };
  }

  unsubscribe(connId: string, localId: string): void {
    const indexKey = `${connId}\u0000${localId}`;
    const subscription = this.localIndex.get(indexKey);
    if (!subscription) return;
    this.localIndex.delete(indexKey);

    const ids = subscription.conns.get(connId);
    ids?.delete(localId);
    if (ids && ids.size === 0) subscription.conns.delete(connId);

    if (subscription.conns.size === 0) {
      this.subs.delete(subscription.key);
      for (const table of subscription.tables) {
        const bucket = this.byTable.get(table);
        bucket?.delete(subscription);
        if (bucket && bucket.size === 0) this.byTable.delete(table);
      }
    }
  }

  stats(): {
    connections: number;
    subscriptions: number;
    tables: string[];
    cursors: number[];
  } {
    return {
      connections: this.connections.size,
      subscriptions: this.subs.size,
      tables: [...this.byTable.keys()].sort(),
      cursors: [...this.connections.values()].map((c) => c.lastSeq),
    };
  }

  private tablesFor(
    name: string,
    def: QueryDef,
    ctx: Ctx,
    params: unknown,
  ): string[] {
    if (typeof def.tables === "function") {
      const tables = def.tables(ctx, params);
      if (!Array.isArray(tables)) {
        throw new LiveHubError(
          "E_CONFIG",
          `query "${name}" must return an array of tables`,
          500,
        );
      }
      return tables;
    }

    if (!Array.isArray(def.tables)) {
      throw new LiveHubError(
        "E_CONFIG",
        `query "${name}" needs \`tables\``,
        500,
      );
    }

    return def.tables;
  }

  private localIds(connId: string): string[] {
    const ids: string[] = [];
    for (const [indexKey] of this.localIndex) {
      if (indexKey.startsWith(`${connId}\u0000`)) {
        ids.push(indexKey.slice(connId.length + 1));
      }
    }
    return ids;
  }

  private onBatch(batch: ChangeBatch): void {
    for (const connection of this.connections.values()) {
      if (batch.to <= connection.lastSeq) continue;
      const gap = connection.lastSeq > 0 && batch.from > connection.lastSeq + 1;
      connection.lastSeq = batch.to;
      if (gap) {
        connection.send("resync", { seq: batch.to, reason: "gap" });
      }
    }

    if (batch.tables.length === 0) return;

    const dirty = new Map<string, Set<string>>();
    for (const table of batch.tables) {
      const bucket = this.byTable.get(table);
      if (!bucket) continue;
      for (const subscription of bucket) {
        for (const [connId, ids] of subscription.conns) {
          let set = dirty.get(connId);
          if (!set) {
            set = new Set();
            dirty.set(connId, set);
          }
          for (const id of ids) set.add(id);
        }
      }
    }

    for (const [connId, ids] of dirty) {
      const connection = this.connections.get(connId);
      if (!connection) continue;
      connection.send("change", { seq: batch.to, ids: [...ids] });
    }
  }

  private runRetention(): void {
    this.sweepConnections();
    try {
      const cursors = [...this.connections.values()]
        .filter((connection) => connection.attached)
        .map((connection) => connection.lastSeq);
      const minCursor =
        cursors.length > 0
          ? Math.min(...cursors)
          : currentSeq(this.db, this.outbox);
      compact({
        db: this.db,
        outbox: this.outbox,
        meta: this.meta,
        minCursor,
        keep: this.options.keep ?? 1000,
      });
    } catch {
      // Retention is best-effort; a failure must not take down the hub.
    }
  }

  /**
   * Drop connections that were created but never attached to a stream.
   * Attached connections are owned by their stream and closed with it.
   */
  private sweepConnections(): void {
    const ttl = this.options.connectionTtlMs ?? 30_000;
    if (ttl <= 0) return;

    const cutoff = Date.now() - ttl;
    for (const connection of [...this.connections.values()]) {
      if (connection.attached) continue;
      if (connection.createdAt > cutoff) continue;
      this.disconnect(connection.id);
    }
  }
}
