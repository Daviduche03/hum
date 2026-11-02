import { mkdtempSync, rmSync } from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import assert from "node:assert/strict";

import { syncLive } from "@hum/core";
import {
  createLiveHandler,
  LiveHub,
  TicketStore,
  toNodeHandler,
  type QueryRegistry,
} from "../src/index.ts";

export interface Harness {
  db: DatabaseSync;
  dbPath: string;
  base: string;
  hub: LiveHub;
  close: () => Promise<void>;
}

export interface HarnessOptions {
  pollMs?: number;
  compactEveryMs?: number;
  keep?: number;
  connectionTtlMs?: number;
  bodyLimit?: number;
}

export async function start(options: HarnessOptions = {}): Promise<Harness> {
  const dir = mkdtempSync(join(tmpdir(), "hum-server-"));
  const dbPath = join(dir, "test.db");
  const db = new DatabaseSync(dbPath);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec(`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL
    );
  `);
  syncLive({ db });

  const queries: QueryRegistry = {
    "users.all": { tables: ["users"] },
    "users.byId": {
      params: (raw: any) => {
        const id = Number(raw?.id);
        if (!Number.isInteger(id)) throw new Error("id");
        return { id };
      },
      // Function form: dependencies can depend on the params or the identity.
      tables: (_ctx, params: any) => (params.id > 0 ? ["users"] : []),
    },
    "secret.only": {
      tables: ["users"],
      authorize: (ctx) => ctx.role !== "guest",
    },
  };

  const hub = new LiveHub({
    db,
    queries,
    pollMs: options.pollMs ?? 5,
    compactEveryMs: options.compactEveryMs ?? 0,
    keep: options.keep,
    connectionTtlMs: options.connectionTtlMs,
  });
  hub.start();

  const handler = createLiveHandler(hub, new TicketStore(), {
    authenticate: (request) => ({
      userId: request.headers.get("x-user") ?? "u1",
      role: request.headers.get("x-role") ?? "admin",
    }),
    origin: (origin) => origin.startsWith("http://127.0.0.1"),
    bodyLimit: options.bodyLimit,
  });

  const server = http.createServer(toNodeHandler(handler));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as AddressInfo).port;

  return {
    db,
    dbPath,
    base: `http://127.0.0.1:${port}`,
    hub,
    close: async () => {
      hub.stop();
      server.closeAllConnections?.();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      db.close();
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

export function sseReader(response: Response) {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  return {
    async next(eventName: string): Promise<any> {
      for (;;) {
        const boundary = buffer.indexOf("\n\n");
        if (boundary >= 0) {
          const block = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const lines = block.split("\n");
          const event = lines.find((line) => line.startsWith("event: "))?.slice(7);
          if (event === eventName) {
            const data = lines.find((line) => line.startsWith("data: "))?.slice(6);
            return JSON.parse(data ?? "null");
          }
          continue;
        }
        const result = await reader.read();
        if (result.done) throw new Error("stream closed");
        buffer += decoder.decode(result.value, { stream: true });
      }
    },
    cancel() {
      void reader.cancel().catch(() => {});
    },
  };
}

export interface OpenStream {
  connId: string;
  events: Array<{ event: string; data: any }>;
  close: () => Promise<void>;
}

/**
 * Open a stream and record every event in the background. Unlike `sseReader`
 * this keeps draining, which is what a real browser does.
 */
export async function openStream(
  base: string,
  headers: Record<string, string> = {},
): Promise<OpenStream> {
  const { connId, ticket } = await connect(base, headers);

  const response = await fetch(`${base}/live/stream?ticket=${ticket}`);
  assert.equal(response.status, 200);

  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  const events: Array<{ event: string; data: any }> = [];
  let buffer = "";

  const pump = (async () => {
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        buffer += decoder.decode(value, { stream: true });

        let boundary = buffer.indexOf("\n\n");
        while (boundary >= 0) {
          const block = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const lines = block.split("\n");
          const event = lines.find((line) => line.startsWith("event: "))?.slice(7);
          const data = lines.find((line) => line.startsWith("data: "))?.slice(6);
          if (event) events.push({ event, data: data ? JSON.parse(data) : null });
          boundary = buffer.indexOf("\n\n");
        }
      }
    } catch {
      // stream cancelled
    }
  })();

  return {
    connId,
    events,
    close: async () => {
      await reader.cancel().catch(() => {});
      await pump;
    },
  };
}

export async function connect(
  base: string,
  headers: Record<string, string> = {},
) {
  const response = await fetch(`${base}/live/connect`, {
    method: "POST",
    headers,
  });
  assert.equal(response.status, 200);
  return (await response.json()) as { connId: string; ticket: string };
}

export async function subscribe(
  base: string,
  connId: string,
  id: string,
  name: string,
  params?: unknown,
): Promise<Response> {
  return fetch(`${base}/live/sub`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ connId, id, name, params }),
  });
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
