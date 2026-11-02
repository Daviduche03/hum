import type { RawSql, SqlSource } from "@hum/core";

export type { Ctx, QueryDef, QueryRegistry, RawSql, SqlSource } from "@hum/core";

export class LiveHubError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, message: string, status = 403) {
    super(message);
    this.name = "LiveHubError";
    this.code = code;
    this.status = status;
  }
}

export function deny(message = "unavailable"): LiveHubError {
  return new LiveHubError("E_DENY", message, 403);
}

export function resolveSql(source: SqlSource | RawSql): {
  sql: string;
  params: unknown[];
} {
  if (typeof (source as SqlSource).toSQL === "function") {
    const compiled = (source as SqlSource).toSQL();
    return { sql: compiled.sql, params: compiled.params ?? [] };
  }
  const raw = source as RawSql;
  return { sql: raw.sql, params: raw.params ?? [] };
}
