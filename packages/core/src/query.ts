/**
 * The server-facing shape of a subscribable query.
 *
 * These live in core because the server and the examples describe the same
 * thing, and core must not depend on the server package.
 */

/** Whatever your `authenticate` returns. */
export type Ctx = Record<string, unknown>;

/**
 * A server-owned query that clients may subscribe to.
 *
 * Clients send the `name` and params. They never send SQL, so the set of
 * subscribable queries is an explicit allowlist, and identity-derived filters
 * live in your loader, not in client input.
 *
 * `tables` is the whole dependency model: declare the tables the query reads
 * and a write to any of them invalidates it. There is no inference and no SQL
 * here — one less thing to keep in sync, and nothing to parse.
 */
export interface QueryDef<P = any> {
  /** Validate/coerce client params. Throwing rejects the subscription. */
  params?: (raw: unknown) => P;
  /**
   * Tables the query depends on. A function is evaluated per subscription,
   * which is how you scope dependencies by params or identity.
   */
  tables: string[] | ((ctx: Ctx, params: P) => string[]);
  authorize?: (ctx: Ctx, params: P) => boolean | Promise<boolean>;
}

export type QueryRegistry = Record<string, QueryDef>;
