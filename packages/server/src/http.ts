import type { LiveHub } from "./hub.ts";
import type { TicketStore } from "./tickets.ts";
import type { Ctx } from "./types.ts";
import { LiveHubError } from "./types.ts";

export interface LiveHandlerOptions {
  authenticate: (request: Request) => Ctx | Promise<Ctx>;
  /** Must match the identity stored on the connection. Defaults to hub's. */
  identity?: (ctx: Ctx) => string;
  /** Mount point. Default "/live". */
  basePath?: string;
  /** Origin allowlist for state-changing routes. */
  origin?: string | string[] | ((origin: string) => boolean);
  /** Request body cap in bytes. Default 64 KiB. */
  bodyLimit?: number;
}

type Handler = (request: Request) => Promise<Response | undefined>;

const JSON_HEADERS = { "content-type": "application/json; charset=utf-8" };

/**
 * Framework-agnostic endpoint.
 *
 * Everything is `(Request) => Response`, which is the one contract shared by
 * Hono, Bun, Deno, Next route handlers, Cloudflare Workers and Express (via a
 * three-line adapter). Returns `undefined` for paths it does not own so it can
 * be mounted as middleware.
 */
export function createLiveHandler(
  hub: LiveHub,
  tickets: TicketStore,
  options: LiveHandlerOptions,
): Handler {
  const base = (options.basePath ?? "/live").replace(/\/$/, "");
  const bodyLimit = options.bodyLimit ?? 64 * 1024;
  const identityOf =
    options.identity ??
    hub.options.identity ??
    ((ctx: Ctx) => String(ctx.userId ?? ctx.sessionId ?? ctx.orgId ?? ""));

  const isOriginAllowed = (request: Request): boolean => {
    if (!options.origin) return true;
    const origin = request.headers.get("origin");
    // Browsers always send Origin on cross-origin (and same-origin POST)
    // requests. A missing Origin means a non-browser client, which carries no
    // ambient cookie and therefore cannot be CSRF'd.
    if (!origin) return true;
    if (typeof options.origin === "function") return options.origin(origin);
    const list = Array.isArray(options.origin)
      ? options.origin
      : [options.origin];
    return list.includes(origin);
  };

  const requireOrigin = (request: Request): void => {
    if (!isOriginAllowed(request)) {
      throw new LiveHubError("E_ORIGIN", "origin not allowed", 403);
    }
  };

  return async function handler(request: Request): Promise<Response | undefined> {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/$/, "");

    if (!path.startsWith(base)) return undefined;

    try {
      if (request.method === "POST" && path === `${base}/connect`) {
        requireOrigin(request);
        const ctx = await options.authenticate(request);
        const connection = hub.createConnection(ctx);
        const { ticket, expiresAt } = tickets.issue(connection.id);
        return json({ connId: connection.id, ticket, expiresAt });
      }

      if (request.method === "GET" && path === `${base}/stream`) {
        const ticket = url.searchParams.get("ticket");
        if (!ticket) return json({ error: "unavailable" }, 403);
        const connId = tickets.redeem(ticket);
        if (!connId) return json({ error: "unavailable" }, 403);
        const connection = hub.connections.get(connId);
        if (!connection) return json({ error: "unavailable" }, 403);

        connection.startHeartbeat(hub.heartbeatMs);
        return streamResponse(connection, request, () => hub.disconnect(connId));
      }

      if (request.method === "POST" && path === `${base}/sub`) {
        requireOrigin(request);
        const body = await readJson(request, bodyLimit);
        const ctx = await options.authenticate(request);
        const connection = requireConnection(hub, body.connId, ctx, identityOf);
        const result = await hub.subscribe(
          connection.id,
          String(body.id),
          String(body.name),
          body.params,
        );
        return json(result);
      }

      if (request.method === "POST" && path === `${base}/unsub`) {
        requireOrigin(request);
        const body = await readJson(request, bodyLimit);
        const ctx = await options.authenticate(request);
        const connection = requireConnection(hub, body.connId, ctx, identityOf);
        hub.unsubscribe(connection.id, String(body.id));
        return json({ ok: true });
      }

      if (request.method === "POST" && path === `${base}/close`) {
        requireOrigin(request);
        const body = await readJson(request, bodyLimit);
        const ctx = await options.authenticate(request);
        const connection = requireConnection(hub, body.connId, ctx, identityOf);
        hub.disconnect(connection.id);
        return json({ ok: true });
      }

      if (request.method === "GET" && path === `${base}/health`) {
        return json({ ok: true, ...hub.stats() });
      }

      return undefined;
    } catch (error) {
      if (error instanceof LiveHubError) {
        return json({ error: "unavailable" }, error.status);
      }
      return json({ error: "unavailable" }, 500);
    }
  };
}

function requireConnection(
  hub: LiveHub,
  rawConnId: unknown,
  ctx: Ctx,
  identityOf: (ctx: Ctx) => string,
) {
  const connection = hub.connections.get(String(rawConnId));
  if (!connection) {
    throw new LiveHubError("E_CONN", "unknown connection", 410);
  }
  const identity = identityOf(ctx);
  if (connection.identity && identity && connection.identity !== identity) {
    throw new LiveHubError("E_IDENTITY", "unavailable", 403);
  }
  if (connection.identity && !identity) {
    throw new LiveHubError("E_IDENTITY", "unavailable", 403);
  }
  return connection;
}

function streamResponse(
  connection: import("./connection.ts").Connection,
  request: Request,
  onClose: () => void,
): Response {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      connection.attach(controller);
    },
    cancel() {
      onClose();
    },
  });

  request.signal?.addEventListener?.("abort", () => onClose(), { once: true });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
      "x-accel-buffering": "no",
    },
  });
}

async function readJson(request: Request, limit: number): Promise<any> {
  const text = await request.text();
  if (text.length > limit) {
    throw new LiveHubError("E_BODY", "body too large", 413);
  }
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    throw new LiveHubError("E_BODY", "invalid json", 400);
  }
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), { status, headers: JSON_HEADERS });
}
