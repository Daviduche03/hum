export type Ctx = Record<string, unknown>;

export interface SqlSource {
  toSQL(): { sql: string; params?: unknown[] };
}

export interface RawSql {
  sql: string;
  params?: unknown[];
}

/**
 * A server-owned query that clients may subscribe to.
 *
 * Clients send the `name` and params. They never send SQL, so the set of
 * subscribable queries is an explicit allowlist and identity-derived filters
 * live in `build`, not in client input.
 */
export interface QueryDef<P = any> {
  /** Validate/coerce client params. Throwing rejects the subscription. */
  params?: (raw: unknown) => P;
  build: (ctx: Ctx, params: P) => SqlSource | RawSql;
  /**
   * Tables this query depends on. When omitted we infer them with
   * `EXPLAIN QUERY PLAN`. When inference finds nothing we fall back to
   * "all tables", which is correct but noisy.
   */
  tables?: string[] | ((ctx: Ctx, params: P) => string[]);
  authorize?: (ctx: Ctx, params: P) => boolean | Promise<boolean>;
}

export type QueryRegistry = Record<string, QueryDef>;

export class LiveHubError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, message: string, status = 403) {
    super(message);
    this.name = "LiveHubError";
    this.code = code;
    this.status = status;
  }
}

export function deny(message = "unavailable"): LiveHubError {
  return new LiveHubError("E_DENY", message, 403);
}

export function resolveSql(source: SqlSource | RawSql): {
  sql: string;
  params: unknown[];
} {
  if (typeof (source as SqlSource).toSQL === "function") {
    const compiled = (source as SqlSource).toSQL();
    return { sql: compiled.sql, params: compiled.params ?? [] };
  }
  const raw = source as RawSql;
  return { sql: raw.sql, params: raw.params ?? [] };
}
