export type { Ctx, QueryDef, QueryRegistry } from "@hum/core";

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
