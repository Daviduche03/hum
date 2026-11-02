import {
  compact,
  currentSeq,
  knownTables,
  stableStringify,
  tablesForSql,
  Tailer,
  type ChangeBatch,
  type LiveDb,
} from "@livelite/core";
import { Connection } from "./connection.ts";
import {
  deny,
  LiveHubError,
  resolveSql,
  type Ctx,
  type QueryDef,
  type QueryRegistry,
} from "./types.ts";

export interface HubOptions {
  db: LiveDb;
  queries: QueryRegistry;
  outbox?: string;
  meta?: string;
  /** Poll interval for the outbox tailer. Default 40ms. */
  pollMs?: number;
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
  private known: Set<string>;
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
    this.known = knownTables(this.db);

    this.tailer = new Tailer({
      db: this.db,
      outbox: this.outbox,
      meta: this.meta,
      pollMs: options.pollMs ?? 40,
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

  /** Re-read the table list after a migration. */
  refreshTables(): void {
    this.known = knownTables(this.db);
  }

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
      tables = this.tablesFor(def, connection.ctx, params);
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
    def: QueryDef,
    ctx: Ctx,
    params: unknown,
  ): string[] {
    if (typeof def.tables === "function") return def.tables(ctx, params);
    if (Array.isArray(def.tables)) return def.tables;

    const sql = resolveSql(def.build(ctx, params));
    const inferred = tablesForSql(this.db, sql.sql, sql.params, this.known);
    // Falling back to every table is pessimistic but never silently stale.
    return inferred.length > 0 ? inferred : [...this.known];
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
}
