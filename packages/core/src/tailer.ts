import { currentSeq, getMeta } from "./outbox.ts";
import { OUTBOX_TABLE, META_TABLE, quoteIdent, TRUNCATED_KEY } from "./sql.ts";
import type {
  ChangeBatch,
  LiveDb,
  ResyncInfo,
} from "./types.ts";

export type StartAt = "now" | "beginning" | number;

export interface TailerOptions {
  db: LiveDb;
  outbox?: string;
  meta?: string;
  /**
   * How often to check for new change rows. Default 250ms.
   *
   * This is the fallback path for writes made by *other* processes. Writes
   * through the same process should call `LiveHub.flush()` (or wrap the
   * database with `onWrite()`), which is what keeps in-process latency near
   * zero without paying for a tight poll.
   */
  pollMs?: number;
  /** Rows per read. Default 500. */
  batchSize?: number;
  /** Max read iterations per poll before yielding. Default 8. */
  maxBatches?: number;
  startAt?: StartAt;
  onBatch: (batch: ChangeBatch) => void;
  onResync?: (info: ResyncInfo) => void;
  onError?: (error: unknown) => void;
}

/**
 * Reads the outbox and folds commits into coalesced batches of table names.
 *
 * The cursor is a seq number, never a timestamp. The outbox is the bus: every
 * process tails it independently, so no cross-process pub/sub is required.
 */
export class Tailer {
  private readonly options: Required<
    Pick<TailerOptions, "pollMs" | "batchSize" | "maxBatches" | "startAt">
  > &
    TailerOptions;
  private timer: ReturnType<typeof setInterval> | null = null;
  private polling = false;
  private stopped = false;
  cursor = 0;

  constructor(options: TailerOptions) {
    this.options = {
      pollMs: 250,
      batchSize: 500,
      maxBatches: 8,
      startAt: "now",
      ...options,
    };
    this.cursor = this.initialCursor();
  }

  private initialCursor(): number {
    const { startAt, db, outbox = OUTBOX_TABLE } = this.options;
    if (typeof startAt === "number") return startAt;
    if (startAt === "beginning") return 0;
    return currentSeq(db, outbox);
  }

  start(): void {
    if (this.timer) return;
    this.stopped = false;
    this.timer = setInterval(() => this.poll(), this.options.pollMs);
    // Do not keep a process alive just to poll.
    (this.timer as { unref?: () => void }).unref?.();
    this.poll();
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  /** Read once, right now. Also used by tests and by the write-path fast flush. */
  poll(): void {
    if (this.polling || this.stopped) return;
    this.polling = true;
    try {
      this.drain();
    } catch (error) {
      this.options.onError?.(error);
    } finally {
      this.polling = false;
    }
  }

  private drain(): void {
    const { db, outbox = OUTBOX_TABLE, meta = META_TABLE } = this.options;

    const maxSeq = currentSeq(db, outbox);
    if (this.cursor > maxSeq) {
      this.resync(maxSeq, "outbox-reset");
      return;
    }

    const truncated = Number(getMeta(db, TRUNCATED_KEY, meta) ?? 0);
    if (this.cursor > 0 && this.cursor < truncated) {
      this.resync(truncated, "truncated");
      // Fall through: continue reading from the new cursor so we do not stall.
    }

    const statement = db.prepare(
      `SELECT "seq", "tbl" FROM ${quoteIdent(outbox)}
       WHERE "seq" > ? ORDER BY "seq" ASC LIMIT ?`,
    );

    let from = 0;
    let to = this.cursor;
    let count = 0;
    const tables = new Set<string>();

    for (let i = 0; i < this.options.maxBatches; i += 1) {
      const rows = statement.all(this.cursor, this.options.batchSize) as Array<{
        seq: number;
        tbl: string;
      }>;
      if (rows.length === 0) break;

      for (const row of rows) {
        const seq = Number(row.seq);
        if (from === 0) from = seq;
        to = seq;
        count += 1;
        tables.add(String(row.tbl));
      }

      this.cursor = Number(rows[rows.length - 1]!.seq);
      if (rows.length < this.options.batchSize) break;
    }

    if (count === 0) return;

    this.options.onBatch({
      from,
      to,
      seq: to,
      tables: [...tables],
      count,
    });
  }

  private resync(seq: number, reason: string): void {
    this.cursor = seq;
    this.options.onResync?.({ seq, reason });
  }
}
