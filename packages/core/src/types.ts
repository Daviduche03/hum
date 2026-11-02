export type LiveOp = "insert" | "update" | "delete";

export interface RunResult {
  changes: number | bigint;
  lastInsertRowid: number | bigint;
}

/**
 * The minimum surface we need from a SQLite driver.
 *
 * `better-sqlite3`, `node:sqlite` (DatabaseSync) and `bun:sqlite` all satisfy
 * this shape structurally, so we never import a driver ourselves.
 */
export interface PreparedStatement {
  run(...params: unknown[]): RunResult;
  get(...params: unknown[]): any;
  all(...params: unknown[]): any[];
}

export interface LiveDb {
  exec(sql: string): unknown;
  prepare(sql: string): PreparedStatement;
}

export interface SyncedTable {
  name: string;
  /** Primary key columns, in key order. Empty when the table is a rowid table. */
  pk: string[];
  /** True when we fall back to `rowid` (no declared primary key). */
  rowid: boolean;
}

export interface ChangeBatch {
  /** First seq in this batch (inclusive). */
  from: number;
  /** Last seq in this batch (inclusive). */
  to: number;
  /** Alias of `to`, the cursor to resume from. */
  seq: number;
  /** Distinct table names touched by this batch. */
  tables: string[];
  /** Number of change rows folded into this batch. */
  count: number;
}

export interface ResyncInfo {
  seq: number;
  reason: string;
}

export class LiveSqliteError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "LiveSqliteError";
    this.code = code;
  }
}
