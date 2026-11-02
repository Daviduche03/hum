import { mkdtempSync, rmSync } from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import assert from "node:assert/strict";
import { test } from "node:test";

import { syncLive } from "@livelite/core";
import {
  createLiveHandler,
  LiveHub,
  TicketStore,
  toNodeHandler,
  type QueryRegistry,
} from "../src/index.ts";

interface Harness {
  db: DatabaseSync;
  base: string;
  hub: LiveHub;
  close: () => Promise<void>;
}

async function start(): Promise<Harness> {
  const dir = mkdtempSync(join(tmpdir(), "livelite-server-"));
  const db = new DatabaseSync(join(dir, "test.db"));
  db.exec("PRAGMA journal_mode = WAL");
  db.exec(`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL
    );
  `);
  syncLive({ db });

  const queries: QueryRegistry = {
    "users.all": {
      build: () => ({
        sql: "SELECT id, name FROM users ORDER BY id",
        params: [],
      }),
    },
    "users.byId": {
      params: (raw: any) => {
        const id = Number(raw?.id);
        if (!Number.isInteger(id)) throw new Error("id");
        return { id };
      },
      build: (_ctx, params: any) => ({
        sql: "SELECT id, name FROM users WHERE id = ?",
        params: [params.id],
      }),
      tables: ["users"],
    },
    "secret.only": {
      build: () => ({ sql: "SELECT 1 AS ok", params: [] }),
      tables: ["users"],
      authorize: (ctx) => ctx.role !== "guest",
    },
  };

  const hub = new LiveHub({ db, queries, pollMs: 5, compactEveryMs: 0 });
  hub.start();

  const tickets = new TicketStore();
  const handler = createLiveHandler(hub, tickets, {
    authenticate: (request) => ({
      userId: request.headers.get("x-user") ?? "u1",
      role: request.headers.get("x-role") ?? "admin",
    }),
    origin: (origin) => origin.startsWith("http://127.0.0.1"),
  });

  const server = http.createServer(toNodeHandler(handler));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as AddressInfo).port;

  return {
    db,
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

function sseReader(response: Response) {
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
          const event = lines
            .find((line) => line.startsWith("event: "))
            ?.slice(7);
          if (event === eventName) {
            const data = lines
              .find((line) => line.startsWith("data: "))
              ?.slice(6);
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

async function connect(base: string, headers: Record<string, string> = {}) {
  const response = await fetch(`${base}/live/connect`, {
    method: "POST",
    headers,
  });
  assert.equal(response.status, 200);
  return (await response.json()) as { connId: string; ticket: string };
}

test("delivers a change for a subscribed table", async () => {
  const h = await start();
  try {
    const { connId, ticket } = await connect(h.base);

    const stream = await fetch(`${h.base}/live/stream?ticket=${ticket}`);
    assert.equal(stream.status, 200);
    assert.match(stream.headers.get("content-type") ?? "", /text\/event-stream/);

    const events = sseReader(stream);
    await events.next("hello");

    const sub = await fetch(`${h.base}/live/sub`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ connId, id: "q1", name: "users.all" }),
    });
    assert.equal(sub.status, 200);
    const subBody = (await sub.json()) as { seq: number; tables: string[] };
    assert.deepEqual(subBody.tables, ["users"]);

    h.db.prepare("INSERT INTO users (id, name) VALUES (?, ?)").run(1, "ada");
    h.hub.flush();

    const change = await events.next("change");
    assert.deepEqual(change.ids, ["q1"]);
    assert.ok(change.seq >= 1);

    events.cancel();
  } finally {
    await h.close();
  }
});

test("infers dependencies for a query with no explicit tables list", async () => {
  const h = await start();
  try {
    const { connId, ticket } = await connect(h.base);
    const stream = await fetch(`${h.base}/live/stream?ticket=${ticket}`);
    const events = sseReader(stream);
    await events.next("hello");

    const sub = await fetch(`${h.base}/live/sub`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ connId, id: "q1", name: "users.byId", params: { id: 1 } }),
    });
    // users.byId declares tables explicitly; users.all relies on inference.
    assert.equal(sub.status, 200);

    const inferred = await fetch(`${h.base}/live/sub`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ connId, id: "q2", name: "users.all" }),
    });
    const body = (await inferred.json()) as { tables: string[] };
    assert.deepEqual(body.tables, ["users"]);

    h.db.prepare("INSERT INTO users (id, name) VALUES (?, ?)").run(1, "ada");
    h.hub.flush();

    const change = await events.next("change");
    assert.deepEqual(change.ids.sort(), ["q1", "q2"]);
    events.cancel();
  } finally {
    await h.close();
  }
});

test("unknown queries and unauthorized queries look identical", async () => {
  const h = await start();
  try {
    const { connId } = await connect(h.base);

    const unknown = await fetch(`${h.base}/live/sub`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ connId, id: "q1", name: "does.not.exist" }),
    });
    assert.equal(unknown.status, 403);
    assert.deepEqual(await unknown.json(), { error: "unavailable" });

    const guest = await connect(h.base, { "x-role": "guest" });
    const denied = await fetch(`${h.base}/live/sub`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-role": "guest" },
      body: JSON.stringify({
        connId: guest.connId,
        id: "q1",
        name: "secret.only",
      }),
    });
    assert.equal(denied.status, 403);
    assert.deepEqual(await denied.json(), { error: "unavailable" });
  } finally {
    await h.close();
  }
});

test("a connection cannot be used by a different identity", async () => {
  const h = await start();
  try {
    const alice = await connect(h.base, { "x-user": "alice" });

    const hijack = await fetch(`${h.base}/live/sub`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-user": "bob" },
      body: JSON.stringify({ connId: alice.connId, id: "q1", name: "users.all" }),
    });
    assert.equal(hijack.status, 403);
  } finally {
    await h.close();
  }
});

test("stream tickets are single-use", async () => {
  const h = await start();
  try {
    const { ticket } = await connect(h.base);

    const first = await fetch(`${h.base}/live/stream?ticket=${ticket}`);
    assert.equal(first.status, 200);
    const reader = first.body!.getReader();
    await reader.read();
    await reader.cancel();

    const second = await fetch(`${h.base}/live/stream?ticket=${ticket}`);
    assert.equal(second.status, 403);
  } finally {
    await h.close();
  }
});

test("malformed params are rejected without revealing the query", async () => {
  const h = await start();
  try {
    const { connId } = await connect(h.base);
    const bad = await fetch(`${h.base}/live/sub`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ connId, id: "q1", name: "users.byId", params: {} }),
    });
    assert.equal(bad.status, 400);
  } finally {
    await h.close();
  }
});
