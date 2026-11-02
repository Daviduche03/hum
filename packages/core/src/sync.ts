import { ensureOutbox } from "./outbox.ts";
import {
  isSystemTable,
  META_TABLE,
  OUTBOX_TABLE,
  quoteIdent,
  quoteLiteral,
  triggerName,
} from "./sql.ts";
import type { LiveDb, LiveOp, SyncedTable } from "./types.ts";
import { LiveSqliteError } from "./types.ts";

const OPS: LiveOp[] = ["insert", "update", "delete"];

/**
 * What a trigger writes into the outbox.
 *
 * - `"row"` (default) writes the primary key of every affected row as JSON.
 *   Nothing in the delivered pipeline reads it today; it is kept for the
 *   deferred row-patch feature and for debugging.
 * - `"table"` writes `NULL`. Use it when bulk writes dominate (`UPDATE t SET
 *   x = 1` over 50k rows) and the per-row `json_array(...)` is measurable.
 *   Invalidation is table-level either way, so behaviour is identical.
 */
export type CaptureMode = "row" | "table";

export interface SyncOptions {
  db: LiveDb;
  outbox?: string;
  meta?: string;
  /** Return false to skip a table. */
  include?: (table: string) => boolean;
  /** Return true to skip a table. Applied after `include`. */
  exclude?: (table: string) => boolean;
  /** Attempt `PRAGMA journal_mode = WAL`. Default true. */
  wal?: boolean;
  /** Wrap trigger DDL in a transaction. Default true. */
  transaction?: boolean;
  /** Per-row primary keys or table-level markers. Default `"row"`. */
  capture?: CaptureMode;
}

export interface SyncResult {
  tables: SyncedTable[];
  triggers: string[];
  changed: boolean;
  outbox: string;
  meta: string;
}

export interface TableRow {
  name: string;
  sql: string | null;
}

/**
 * Install (or repair) the change-capture layer.
 *
 * Idempotent and *cheap* when nothing changed: trigger DDL is compared against
 * `sqlite_master` and only rewritten when it differs. That keeps a 100-table
 * boot from opening 100 transactions to recreate identical triggers.
 *
 * Triggers dropped by `drizzle-kit push` are recreated, because a missing
 * trigger is exactly the "differs from expected" case.
 */
export function syncLive(options: SyncOptions): SyncResult {
  const {
    db,
    outbox = OUTBOX_TABLE,
    meta = META_TABLE,
    include,
    exclude,
    wal = true,
    transaction = true,
    capture = "row",
  } = options;

  if (wal) {
    try {
      db.exec("PRAGMA journal_mode = WAL");
    } catch {
      // :memory: and read-only connections cannot switch. Not fatal.
    }
  }

  ensureOutbox(db, outbox, meta);

  const tables = syncedTables(db, include, exclude);
  const existing = triggerSql(db);
  const expected = new Set<string>();
  let changed = false;

  const apply = (): void => {
    for (const table of tables) {
      for (const op of OPS) {
        const name = triggerName(table.name, op);
        expected.add(name);

        const ddl = createTriggerSql(table, op, outbox, capture);
        const current = existing.get(name);
        if (current !== undefined && normalizeSql(current) === normalizeSql(ddl)) {
          continue;
        }
        if (current !== undefined) {
          db.exec(`DROP TRIGGER IF EXISTS ${quoteIdent(name)}`);
        }
        db.exec(ddl);
        changed = true;
      }
    }

    // Drop triggers we own that no longer correspond to a live table, or that
    // a filter now excludes.
    for (const name of existing.keys()) {
      if (!name.startsWith("_live_")) continue;
      if (expected.has(name)) continue;
      db.exec(`DROP TRIGGER IF EXISTS ${quoteIdent(name)}`);
      changed = true;
    }
  };

  if (transaction) {
    db.exec("BEGIN IMMEDIATE");
    try {
      apply();
      db.exec("COMMIT");
    } catch (error) {
      try {
        db.exec("ROLLBACK");
      } catch {
        // ignore
      }
      throw new LiveSqliteError(
        "E_TRIGGER",
        `Failed to sync triggers: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  } else {
    apply();
  }

  return {
    tables,
    triggers: [...expected].sort(),
    changed,
    outbox,
    meta,
  };
}

export interface TriggerCheck {
  missing: string[];
  orphaned: string[];
}

export interface CheckOptions {
  outbox?: string;
  meta?: string;
  /** Must match the filters passed to `syncLive`, or you get false alarms. */
  include?: (table: string) => boolean;
  exclude?: (table: string) => boolean;
}

/**
 * Compare what is installed against what should be. Useful as a boot check,
 * and as the thing you assert before shipping.
 *
 * The filters must match `syncLive`'s. A partial sync without the same
 * `include`/`exclude` here would report every skipped table as missing.
 */
export function checkTriggers(
  db: LiveDb,
  options: CheckOptions = {},
): TriggerCheck {
  const { include, exclude } = options;

  const installed = new Set(triggerSql(db).keys());
  const expected = new Set<string>();

  for (const table of syncedTables(db, include, exclude)) {
    for (const op of OPS) expected.add(triggerName(table.name, op));
  }

  return {
    missing: [...expected].filter((name) => !installed.has(name)).sort(),
    orphaned: [...installed]
      .filter((name) => name.startsWith("_live_") && !expected.has(name))
      .sort(),
  };
}

function syncedTables(
  db: LiveDb,
  include?: (table: string) => boolean,
  exclude?: (table: string) => boolean,
): SyncedTable[] {
  const rows = db
    .prepare(
      `SELECT "name", "sql" FROM sqlite_master
       WHERE "type" = 'table' AND "name" NOT LIKE 'sqlite\\_%' ESCAPE '\\'`,
    )
    .all() as TableRow[];

  const tables: SyncedTable[] = [];
  for (const row of rows) {
    const name = String(row.name);
    if (isSystemTable(name)) continue;
    if (include && !include(name)) continue;
    if (exclude?.(name)) continue;
    tables.push(describeTable(db, name, String(row.sql ?? "")));
  }
  return tables;
}

function describeTable(db: LiveDb, name: string, sql: string): SyncedTable {
  const columns = db
    .prepare(`PRAGMA table_info(${quoteIdent(name)})`)
    .all() as Array<{ name: string; pk: number }>;

  const pk = columns
    .filter((column) => Number(column.pk) > 0)
    .sort((a, b) => Number(a.pk) - Number(b.pk))
    .map((column) => String(column.name));

  const withoutRowid = /\bWITHOUT\s+ROWID\b/i.test(sql);
  if (pk.length === 0 && withoutRowid) {
    throw new LiveSqliteError(
      "E_NO_PK",
      `Table "${name}" is WITHOUT ROWID and has no primary key, so changes cannot be identified.`,
    );
  }

  return { name, pk, rowid: pk.length === 0 };
}

/** Trigger name → its DDL, as SQLite stored it. */
function triggerSql(db: LiveDb): Map<string, string> {
  const rows = db
    .prepare(`SELECT "name", "sql" FROM sqlite_master WHERE "type" = 'trigger'`)
    .all() as Array<{ name: string; sql: string | null }>;

  const out = new Map<string, string>();
  for (const row of rows) out.set(String(row.name), String(row.sql ?? ""));
  return out;
}

/** SQLite preserves the original DDL text, so compare it whitespace-blind. */
function normalizeSql(sql: string): string {
  return sql.replace(/\s+/g, " ").trim();
}

function pkExpression(alias: "NEW" | "OLD", table: SyncedTable): string {
  if (table.rowid) return `${alias}.rowid`;
  return table.pk.map((column) => `${alias}.${quoteIdent(column)}`).join(", ");
}

export function createTriggerSql(
  table: SyncedTable,
  op: LiveOp,
  outbox: string = OUTBOX_TABLE,
  capture: CaptureMode = "row",
): string {
  const alias = op === "delete" ? "OLD" : "NEW";
  const value = capture === "table" ? "NULL" : `json_array(${pkExpression(alias, table)})`;
  return `CREATE TRIGGER ${quoteIdent(triggerName(table.name, op))}
AFTER ${op.toUpperCase()} ON ${quoteIdent(table.name)}
BEGIN
  INSERT INTO ${quoteIdent(outbox)} ("tbl", "op", "pk")
  VALUES (${quoteLiteral(table.name)}, '${op}', ${value});
END`;
}
