import { META_TABLE, OUTBOX_TABLE, quoteIdent } from "./sql.ts";
import type { LiveDb } from "./types.ts";

/**
 * Create the change outbox and the small metadata table.
 *
 * `AUTOINCREMENT` is deliberate: seq values must never be reused, even after
 * retention deletes old rows. Cursors are compared numerically, so a reused
 * value would silently skip changes.
 */
export function ensureOutbox(
  db: LiveDb,
  outbox: string = OUTBOX_TABLE,
  meta: string = META_TABLE,
): void {
  db.exec(
    `CREATE TABLE IF NOT EXISTS ${quoteIdent(outbox)} (
  "seq" INTEGER PRIMARY KEY AUTOINCREMENT,
  "tbl" TEXT NOT NULL,
  "op" TEXT NOT NULL,
  "pk" TEXT NOT NULL,
  "ts" INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
)`,
  );
  db.exec(
    `CREATE TABLE IF NOT EXISTS ${quoteIdent(meta)} (
  "k" TEXT PRIMARY KEY,
  "v" TEXT NOT NULL
)`,
  );
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
