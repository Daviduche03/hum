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

export interface QueryHooks<P = any> {
  /** Validate/coerce client params. Throwing rejects the subscription. */
  params?: (raw: unknown) => P;
  authorize?: (ctx: Ctx, params: P) => boolean | Promise<boolean>;
}

/**
 * Give the query so the planner can work out which tables it reads.
 *
 * `build` is never executed by the hub — it is compiled and passed to
 * `EXPLAIN QUERY PLAN`. Your own route still fetches the rows.
 */
export interface BuildableQuery<P = any> extends QueryHooks<P> {
  build: (ctx: Ctx, params: P) => SqlSource | RawSql;
  /** Overrides inference. When set, `build` is never called. */
  tables?: string[] | ((ctx: Ctx, params: P) => string[]);
}

/**
 * Declare the tables directly. No SQL is compiled, and `build` is not needed.
 * Use this when you know the dependencies — a view, a CTE, or a query the
 * planner describes too vaguely.
 */
export interface TablesQuery<P = any> extends QueryHooks<P> {
  build?: (ctx: Ctx, params: P) => SqlSource | RawSql;
  tables: string[] | ((ctx: Ctx, params: P) => string[]);
}

/**
 * A server-owned query that clients may subscribe to.
 *
 * Clients send the `name` and params. They never send SQL, so the set of
 * subscribable queries is an explicit allowlist and identity-derived filters
 * live in `build`, not in client input.
 *
 * Exactly one of `build` or `tables` is required: `build` to infer, `tables`
 * to state it. `LiveHub` throws at construction when neither is present, so a
 * broken registry fails at boot rather than as a mysterious 403.
 */
export type QueryDef<P = any> = BuildableQuery<P> | TablesQuery<P>;

export type QueryRegistry = Record<string, QueryDef>;
