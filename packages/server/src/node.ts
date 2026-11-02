import type { IncomingMessage, ServerResponse } from "node:http";

type FetchHandler = (request: Request) => Promise<Response | undefined>;

/**
 * Run the fetch-style handler on a plain `node:http` server.
 *
 * This is also the Express/Fastify escape hatch: their `req`/`res` are the same
 * objects, so `app.use(toNodeHandler(handler))` is enough.
 */
export function toNodeHandler(handler: FetchHandler) {
  return async function nodeHandler(
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<void> {
    const request = await toRequest(req);
    const response = await handler(request);

    if (!response) {
      res.statusCode = 404;
      res.setHeader("content-type", "text/plain; charset=utf-8");
      res.end("Not Found");
      return;
    }

    res.statusCode = response.status;
    for (const [key, value] of response.headers) {
      res.setHeader(key, value);
    }
    res.flushHeaders?.();

    if (!response.body) {
      res.end();
      return;
    }

    const reader = response.body.getReader();
    let aborted = false;
    res.on("close", () => {
      if (res.writableEnded) return;
      aborted = true;
      void reader.cancel().catch(() => {});
    });

    try {
      while (!aborted) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value) res.write(value);
      }
    } catch {
      // client went away
    } finally {
      if (!aborted) res.end();
    }
  };
}

async function toRequest(req: IncomingMessage): Promise<Request> {
  const host = req.headers.host ?? "localhost";
  const url = new URL(req.url ?? "/", `http://${host}`);

  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (value === undefined) continue;
    if (Array.isArray(value)) {
      for (const item of value) headers.append(key, item);
    } else {
      headers.set(key, value);
    }
  }

  const method = (req.method ?? "GET").toUpperCase();
  let body: Uint8Array<ArrayBuffer> | undefined;
  if (method !== "GET" && method !== "HEAD") {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    const merged = Buffer.concat(chunks);
    const bytes = new Uint8Array(merged.length);
    bytes.set(merged);
    body = bytes;
  }

  return new Request(url, {
    method,
    headers,
    body: body && body.length > 0 ? body : undefined,
  });
}
