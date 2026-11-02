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
}

export interface SyncResult {
  tables: SyncedTable[];
  triggers: string[];
  changed: boolean;
  outbox: string;
  meta: string;
}

interface TableRow {
  name: string;
  sql: string | null;
}

/**
 * Install (or repair) the change-capture layer.
 *
 * Idempotent: safe to call on every boot and after every migration. Existing
 * triggers are dropped and recreated so that schema changes are picked up and
 * triggers deleted by `drizzle-kit push` come back.
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
  } = options;

  if (wal) {
    try {
      db.exec("PRAGMA journal_mode = WAL");
    } catch {
      // :memory: and read-only connections cannot switch. Not fatal.
    }
  }

  ensureOutbox(db, outbox, meta);

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

  let changed = false;
  for (const table of tables) {
    const before = listTriggers(db);
    const expected = OPS.map((op) => triggerName(table.name, op));

    if (transaction) db.exec("BEGIN IMMEDIATE");
    try {
      for (const op of OPS) {
        const name = triggerName(table.name, op);
        db.exec(`DROP TRIGGER IF EXISTS ${quoteIdent(name)}`);
        db.exec(createTriggerSql(table, op, outbox));
      }
      if (transaction) db.exec("COMMIT");
    } catch (error) {
      if (transaction) {
        try {
          db.exec("ROLLBACK");
        } catch {
          // ignore
        }
      }
      throw new LiveSqliteError(
        "E_TRIGGER",
        `Failed to create triggers for table "${table.name}": ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }

    const after = new Set(listTriggers(db));
    if (expected.some((name) => !after.has(name)) || before.length === 0) {
      changed = true;
    }
  }

  // Drop triggers we own that no longer correspond to a live table.
  const expectedNames = new Set<string>();
  for (const table of tables) {
    for (const op of OPS) expectedNames.add(triggerName(table.name, op));
  }
  for (const name of listTriggers(db)) {
    if (!name.startsWith("_live_")) continue;
    if (expectedNames.has(name)) continue;
    db.exec(`DROP TRIGGER IF EXISTS ${quoteIdent(name)}`);
    changed = true;
  }

  return {
    tables,
    triggers: [...expectedNames],
    changed,
    outbox,
    meta,
  };
}

export interface TriggerCheck {
  missing: string[];
  orphaned: string[];
}

/** Compare what is installed against what should be. Useful as a boot check. */
export function checkTriggers(
  db: LiveDb,
  options: { outbox?: string; meta?: string } = {},
): TriggerCheck {
  const { outbox = OUTBOX_TABLE, meta = META_TABLE } = options;
  void meta;

  const rows = db
    .prepare(
      `SELECT "name", "sql" FROM sqlite_master
       WHERE "type" = 'table' AND "name" NOT LIKE 'sqlite\\_%' ESCAPE '\\'`,
    )
    .all() as TableRow[];

  const installed = new Set(listTriggers(db));
  const expected = new Set<string>();

  for (const row of rows) {
    const name = String(row.name);
    if (isSystemTable(name)) continue;
    const table = describeTable(db, name, String(row.sql ?? ""));
    for (const op of OPS) expected.add(triggerName(table.name, op));
  }

  const missing = [...expected].filter((name) => !installed.has(name));
  const orphaned = [...installed].filter(
    (name) => name.startsWith("_live_") && !expected.has(name),
  );
  return { missing, orphaned };
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

function listTriggers(db: LiveDb): string[] {
  const rows = db
    .prepare(`SELECT "name" FROM sqlite_master WHERE "type" = 'trigger'`)
    .all() as Array<{ name: string }>;
  return rows.map((row) => String(row.name));
}

function pkExpression(alias: "NEW" | "OLD", table: SyncedTable): string {
  if (table.rowid) return `${alias}.rowid`;
  return table.pk.map((column) => `${alias}.${quoteIdent(column)}`).join(", ");
}

export function createTriggerSql(
  table: SyncedTable,
  op: LiveOp,
  outbox: string = OUTBOX_TABLE,
): string {
  const alias = op === "delete" ? "OLD" : "NEW";
  return `CREATE TRIGGER ${quoteIdent(triggerName(table.name, op))}
AFTER ${op.toUpperCase()} ON ${quoteIdent(table.name)}
BEGIN
  INSERT INTO ${quoteIdent(outbox)} ("tbl", "op", "pk")
  VALUES (${quoteLiteral(table.name)}, '${op}', json_array(${pkExpression(alias, table)}));
END`;
}
