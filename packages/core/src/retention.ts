import { currentSeq, getMeta, setMeta } from "./outbox.ts";
import {
  META_TABLE,
  OUTBOX_TABLE,
  quoteIdent,
  TRUNCATED_KEY,
} from "./sql.ts";
import type { LiveDb } from "./types.ts";

export interface CompactOptions {
  db: LiveDb;
  outbox?: string;
  meta?: string;
  /**
   * The oldest cursor any live connection still needs. Anything below this can
   * be deleted. When there are no connections, pass the current seq.
   */
  minCursor?: number;
  /** Always keep at least this many recent rows. Default 1000. */
  keep?: number;
  /** Rows deleted per statement. Default 5000. */
  chunk?: number;
}

export interface CompactResult {
  deleted: number;
  truncatedThrough: number;
}

/**
 * Trim the outbox.
 *
 * This is not housekeeping, it is part of the correctness contract: once rows
 * below a client's cursor are gone, that client cannot catch up and must
 * resync. We record how far we truncated so the tailer can detect exactly that.
 */
export function compact(options: CompactOptions): CompactResult {
  const {
    db,
    outbox = OUTBOX_TABLE,
    meta = META_TABLE,
    keep = 1000,
    chunk = 5000,
  } = options;

  const maxSeq = currentSeq(db, outbox);
  const floor = Number(getMeta(db, TRUNCATED_KEY, meta) ?? 0);
  const minCursor = options.minCursor ?? maxSeq + 1;
  const target = Math.min(minCursor - 1, maxSeq - keep);

  if (!Number.isFinite(target) || target <= floor) {
    return { deleted: 0, truncatedThrough: floor };
  }

  db.exec("BEGIN IMMEDIATE");
  let deleted = 0;
  try {
    const statement = db.prepare(
      `DELETE FROM ${quoteIdent(outbox)}
       WHERE "seq" IN (
         SELECT "seq" FROM ${quoteIdent(outbox)}
         WHERE "seq" <= ? ORDER BY "seq" ASC LIMIT ?
       )`,
    );

    while (floor + deleted < target) {
      const result = statement.run(target, Math.min(chunk, target - floor - deleted));
      const changes = Number(result.changes ?? 0);
      deleted += changes;
      if (changes === 0) break;
    }

    setMeta(db, TRUNCATED_KEY, String(target), meta);
    db.exec("COMMIT");
  } catch (error) {
    try {
      db.exec("ROLLBACK");
    } catch {
      // ignore
    }
    throw error;
  }

  return { deleted, truncatedThrough: target };
}
