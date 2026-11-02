import { tablesForSql, type LiveDb } from "@hum/core";
import type { Ctx, QueryDef, QueryRegistry } from "@hum/server";

/** Anything Drizzle returns from `.select()` / `.insert()` / raw `sql`. */
export interface DrizzleQuery {
  toSQL(): { sql: string; params: unknown[] };
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
 */
export function defineDrizzleQuery<P = unknown>(definition: {
  params?: (raw: unknown) => P;
  build: (ctx: Ctx, params: P) => DrizzleQuery;
  tables?: string[] | ((ctx: Ctx, params: P) => string[]);
  authorize?: (ctx: Ctx, params: P) => boolean | Promise<boolean>;
}): QueryDef<P> {
  return definition;
}

/** Compile a Drizzle query into plain SQL + params. */
export function compileDrizzle(query: DrizzleQuery): {
  sql: string;
  params: unknown[];
} {
  const compiled = query.toSQL();
  return { sql: compiled.sql, params: compiled.params ?? [] };
}

/**
 * Derive table dependencies straight from the planner.
 *
 * Useful when you want to register a plain query registry entry but let the
 * `drizzle` package own the inference.
 */
export function drizzleTables(db: LiveDb, query: DrizzleQuery): string[] {
  const { sql, params } = compileDrizzle(query);
  return tablesForSql(db, sql, params);
}

export function defineRegistry(definitions: QueryRegistry): QueryRegistry {
  return definitions;
}
