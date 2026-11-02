import type { LiveOp } from "./types.ts";

export const OUTBOX_TABLE = "_live_changes";
export const META_TABLE = "_live_meta";
export const TRUNCATED_KEY = "truncated_through";

/** Quote an identifier for SQLite. Doubles embedded quotes. */
export function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/** Quote a string literal for SQLite. Doubles embedded single quotes. */
export function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

const OP_SUFFIX: Record<LiveOp, string> = {
  insert: "ai",
  update: "au",
  delete: "ad",
};

export function triggerName(table: string, op: LiveOp): string {
  return `_live_${table}_${OP_SUFFIX[op]}`;
}

/** Strip the quoting SQLite uses when printing table names in query plans. */
export function unquoteIdent(raw: string): string {
  const trimmed = raw.trim();
  if (trimmed.length >= 2) {
    const first = trimmed[0];
    const last = trimmed[trimmed.length - 1];
    if (first === '"' && last === '"') {
      return trimmed.slice(1, -1).replace(/""/g, '"');
    }
    if (first === "`" && last === "`") {
      return trimmed.slice(1, -1).replace(/``/g, "`");
    }
    if (first === "[" && last === "]") {
      return trimmed.slice(1, -1);
    }
  }
  return trimmed;
}

/** Default tables we never put triggers on. */
export function isSystemTable(name: string): boolean {
  return (
    name.startsWith("sqlite_") ||
    name.startsWith("_live_") ||
    name.startsWith("__drizzle")
  );
}
