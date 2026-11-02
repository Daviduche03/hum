import { knownTables, tablesForSql } from "./deps.ts";
import type {
  BuildableQuery,
  Ctx,
  QueryDef,
  QueryRegistry,
  RawSql,
  TablesQuery,
} from "./query.ts";
import type { LiveDb } from "./types.ts";

/** Anything Drizzle returns from `.select()` / `.insert()` / raw `sql`. */
export interface DrizzleQuery {
  toSQL(): { sql: string; params?: unknown[] };
}

/**
 * Adapt a Drizzle query builder into a live query definition.
 *
 * The compiled SQL is used twice: it is the thing `EXPLAIN QUERY PLAN` reads to
 * derive table dependencies, and it is the stable identity behind the
 * subscription key (together with params and the auth scope).
 *
 *     const queries = {
 *       "users.byOrg": defineDrizzleQuery({
 *         build: (ctx) => db.select().from(users).where(eq(users.orgId, ctx.orgId)),
 *       }),
 *     };
 *
 * Pass `tables` instead of `build` when you already know the dependencies; no
 * SQL is compiled in that case.
 */
export function defineDrizzleQuery<P = unknown>(definition: {
  params?: (raw: unknown) => P;
  build: (ctx: Ctx, params: P) => DrizzleQuery;
  tables?: string[] | ((ctx: Ctx, params: P) => string[]);
  authorize?: (ctx: Ctx, params: P) => boolean | Promise<boolean>;
}): BuildableQuery<P>;
export function defineDrizzleQuery<P = unknown>(definition: {
  params?: (raw: unknown) => P;
  build?: (ctx: Ctx, params: P) => DrizzleQuery;
  tables: string[] | ((ctx: Ctx, params: P) => string[]);
  authorize?: (ctx: Ctx, params: P) => boolean | Promise<boolean>;
}): TablesQuery<P>;
export function defineDrizzleQuery<P = unknown>(definition: {
  params?: (raw: unknown) => P;
  build?: (ctx: Ctx, params: P) => DrizzleQuery;
  tables?: string[] | ((ctx: Ctx, params: P) => string[]);
  authorize?: (ctx: Ctx, params: P) => boolean | Promise<boolean>;
}): QueryDef<P> {
  if (!definition.build && !definition.tables) {
    throw new Error("defineDrizzleQuery needs either `build` or `tables`");
  }
  return definition as QueryDef<P>;
}

/** Compile a Drizzle builder (or a raw `{ sql, params }`) into plain SQL. */
export function compileDrizzle(query: DrizzleQuery | RawSql): {
  sql: string;
  params: unknown[];
} {
  if (typeof (query as DrizzleQuery).toSQL === "function") {
    const compiled = (query as DrizzleQuery).toSQL();
    return { sql: compiled.sql, params: compiled.params ?? [] };
  }
  const raw = query as RawSql;
  return { sql: raw.sql, params: raw.params ?? [] };
}

/**
 * Derive table dependencies straight from the planner.
 *
 * Useful when you register a plain query registry entry but still want the
 * compiled SQL to decide what to subscribe to. Real table names are filtered
 * against `sqlite_master`, so planner noise and aliases are discarded.
 */
export function drizzleTables(
  db: LiveDb,
  query: DrizzleQuery | RawSql,
): string[] {
  const { sql, params } = compileDrizzle(query);
  return tablesForSql(db, sql, params, knownTables(db));
}

export function defineRegistry(definitions: QueryRegistry): QueryRegistry {
  return definitions;
}
