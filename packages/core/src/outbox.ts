import { META_TABLE, OUTBOX_TABLE, quoteIdent } from "./sql.ts";
import type { LiveDb } from "./types.ts";

/**
 * Create the change outbox and the small metadata table.
 *
 * `AUTOINCREMENT` is deliberate: seq values must never be reused, even after
 * retention deletes old rows. Cursors are compared numerically, so a reused
 * value would silently skip changes.
 *
 * `pk` is nullable because `syncLive({ capture: "table" })` writes `NULL`
 * instead of a per-row primary key.
 */
export function ensureOutbox(
  db: LiveDb,
  outbox: string = OUTBOX_TABLE,
  meta: string = META_TABLE,
): void {
  const created = createOutbox(db, outbox);
  if (!created) relaxPk(db, outbox);

  db.exec(
    `CREATE TABLE IF NOT EXISTS ${quoteIdent(meta)} (
  "k" TEXT PRIMARY KEY,
  "v" TEXT NOT NULL
)`,
  );
}

function createOutbox(db: LiveDb, outbox: string): boolean {
  const existing = db
    .prepare(
      `SELECT "name" FROM sqlite_master WHERE "type" = 'table' AND "name" = ?`,
    )
    .get(outbox);
  if (existing) return false;

  db.exec(
    `CREATE TABLE ${quoteIdent(outbox)} (
  "seq" INTEGER PRIMARY KEY AUTOINCREMENT,
  "tbl" TEXT NOT NULL,
  "op" TEXT NOT NULL,
  "pk" TEXT,
  "ts" INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
)`,
  );
  return true;
}

/**
 * Older databases created `pk` as `NOT NULL`. Rebuild once so table-level
 * capture can write `NULL`; this only runs for outboxes from before that
 * change, and triggers are recreated by `syncLive` immediately afterwards.
 */
function relaxPk(db: LiveDb, outbox: string): void {
  const columns = db
    .prepare(`PRAGMA table_info(${quoteIdent(outbox)})`)
    .all() as Array<{ name: string; notnull: number }>;

  const pk = columns.find((column) => String(column.name) === "pk");
  if (!pk || Number(pk.notnull) === 0) return;

  const legacy = `${outbox}_legacy`;
  db.exec("BEGIN IMMEDIATE");
  try {
    db.exec(`DROP TABLE IF EXISTS ${quoteIdent(legacy)}`);
    db.exec(`ALTER TABLE ${quoteIdent(outbox)} RENAME TO ${quoteIdent(legacy)}`);
    createOutbox(db, outbox);
    db.exec(
      `INSERT INTO ${quoteIdent(outbox)} ("seq", "tbl", "op", "pk", "ts")
       SELECT "seq", "tbl", "op", "pk", "ts" FROM ${quoteIdent(legacy)}`,
    );
    db.exec(`DROP TABLE ${quoteIdent(legacy)}`);
    db.exec("COMMIT");
  } catch (error) {
    try {
      db.exec("ROLLBACK");
    } catch {
      // ignore
    }
    throw error;
  }
}

/** The newest seq in the outbox, or 0 when empty. */
export function currentSeq(db: LiveDb, outbox: string = OUTBOX_TABLE): number {
  const row = db
    .prepare(`SELECT COALESCE(MAX("seq"), 0) AS seq FROM ${quoteIdent(outbox)}`)
    .get();
  return Number(row?.seq ?? 0);
}

export function getMeta(
  db: LiveDb,
  key: string,
  meta: string = META_TABLE,
): string | undefined {
  const row = db
    .prepare(`SELECT "v" FROM ${quoteIdent(meta)} WHERE "k" = ?`)
    .get(key);
  return row ? String(row.v) : undefined;
}

export function setMeta(
  db: LiveDb,
  key: string,
  value: string,
  meta: string = META_TABLE,
): void {
  db.prepare(
    `INSERT INTO ${quoteIdent(meta)} ("k", "v") VALUES (?, ?)
     ON CONFLICT("k") DO UPDATE SET "v" = excluded."v"`,
  ).run(key, value);
}
