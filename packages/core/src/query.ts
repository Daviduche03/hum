/**
 * The server-facing shape of a subscribable query.
 *
 * These live in core because both the server and the Drizzle adapter describe
 * the same thing, and core must not depend on the server package.
 */

/** Whatever your `authenticate` returns. */
export type Ctx = Record<string, unknown>;

/** Anything with a `.toSQL()` — Drizzle's query builders, mostly. */
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
