import { isSystemTable, unquoteIdent } from "./sql.ts";
import type { LiveDb } from "./types.ts";

/**
 * Derive the tables a query reads by asking SQLite itself.
 *
 * Parsing SQL is a losing game, and ORMs emit all kinds of shapes. The query
 * planner already knows the answer, so we run `EXPLAIN QUERY PLAN` and read the
 * `SCAN` / `SEARCH` lines.
 *
 * When `known` is supplied we keep only real table names, which filters out
 * planner noise like `SCAN SUBQUERY 1` and `SCAN CONSTANT ROW`.
 */
export function tablesForSql(
  db: LiveDb,
  sql: string,
  params: unknown[] = [],
  known?: Set<string>,
): string[] {
  const rows = db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...params) as Array<{
    detail?: string;
  }>;

  const found = new Set<string>();
  for (const row of rows) {
    const detail = String(row.detail ?? "").trim();
    const match = /^(?:SCAN|SEARCH)\s+(.+?)(?:\s+USING\b|\s*$)/i.exec(detail);
    if (!match?.[1]) continue;
    const name = unquoteIdent(match[1]);
    if (!name) continue;
    if (known) {
      if (known.has(name)) found.add(name);
    } else {
      found.add(name);
    }
  }

  // The planner reports the *alias* (`SCAN u`), not the table, whenever the
  // query aliases. Recover real table names from the FROM/JOIN clauses so
  // aliased and joined queries do not silently subscribe to nothing.
  if (known) {
    for (const name of referencedTables(sql, known)) found.add(name);
  }

  return [...found];
}

/**
 * Extract table names following FROM/JOIN, resolving quoted identifiers and
 * schema prefixes. Deliberately shallow: the planner output above is the
 * primary source, this only fills in aliases and unexecuted subqueries.
 */
function referencedTables(sql: string, known: Set<string>): string[] {
  const pattern =
    /\b(?:from|join)\s+("(?:[^"]|"")*"|`(?:[^`]|``)*`|\[[^\]]*\]|[A-Za-z_][\w$.]*)/gi;
  const found = new Set<string>();
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(sql))) {
    const raw = match[1];
    if (!raw) continue;
    const name = unquoteIdent(raw).split(".").pop() ?? "";
    if (known.has(name)) found.add(name);
  }
  return [...found];
}

/** Snapshot the user tables so dependency inference can filter planner noise. */
export function knownTables(db: LiveDb): Set<string> {
  const rows = db
    .prepare(
      `SELECT "name" FROM sqlite_master
       WHERE "type" = 'table' AND "name" NOT LIKE 'sqlite\\_%' ESCAPE '\\'`,
    )
    .all() as Array<{ name: string }>;
  return new Set(
    rows
      .map((row) => String(row.name))
      .filter((name) => !isSystemTable(name)),
  );
}
