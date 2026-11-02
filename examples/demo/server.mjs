import http from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

import { syncLive } from "@livelite/core";
import {
  createLiveHandler,
  LiveHub,
  TicketStore,
  toNodeHandler,
} from "@livelite/server";

const local = (name) => fileURLToPath(new URL(name, import.meta.url));
const PORT = Number(process.env.PORT ?? 4477);

// ---------------------------------------------------------------- database

const db = new DatabaseSync(local("./demo.db"));
db.exec("PRAGMA journal_mode = WAL");
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    org_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS INTEGER))
  );
`);

// Installs the outbox and one trigger per table per operation.
const synced = syncLive({ db, wal: false });

if (Number(db.prepare("SELECT COUNT(*) AS c FROM users").get().c) === 0) {
  const insert = db.prepare("INSERT INTO users (name, org_id) VALUES (?, ?)");
  insert.run("Ada Lovelace", 1);
  insert.run("Grace Hopper", 1);
  insert.run("Margaret Hamilton", 1);
  insert.run("Barbara Liskov", 2);
}

// ---------------------------------------------------------------- queries

/**
 * The allowlist. Clients send a name + params, never SQL.
 *
 * `users.all` and `users.byOrg` deliberately omit `tables`, so the server
 * derives dependencies with EXPLAIN QUERY PLAN.
 */
const queries = {
  "users.all": {
    build: () => ({
      sql: "SELECT id, name, org_id, created_at FROM users ORDER BY id DESC LIMIT 50",
      params: [],
    }),
  },
  "users.byOrg": {
    params: (raw) => {
      const orgId = Number(raw?.org_id);
      if (!Number.isInteger(orgId)) throw new Error("org_id required");
      return { org_id: orgId };
    },
    build: (_ctx, params) => ({
      sql: "SELECT id, name, org_id, created_at FROM users WHERE org_id = ? ORDER BY id DESC LIMIT 50",
      params: [params.org_id],
    }),
  },
};

const hub = new LiveHub({
  db,
  queries,
  pollMs: 40,
  compactEveryMs: 2_000,
  keep: 500,
  // In a shared multi-tenant database this is what keeps two accounts from
  // sharing a subscription entry. Here every tab is "alice".
  scope: (ctx) => ctx.userId,
  identity: (ctx) => ctx.userId,
});
hub.start();

const tickets = new TicketStore();

const live = createLiveHandler(hub, tickets, {
  authenticate: (request) => {
    const cookie = request.headers.get("cookie") ?? "";
    const match = /(?:^|;\s*)uid=([^;]+)/.exec(cookie);
    return { userId: decodeURIComponent(match?.[1] ?? "alice") };
  },
  origin: (origin) =>
    origin === `http://localhost:${PORT}` ||
    origin === `http://127.0.0.1:${PORT}`,
});

// ---------------------------------------------------------------- routes

const nodeLive = toNodeHandler(live);

const server = http.createServer(async (request, response) => {
  const url = new URL(request.url ?? "/", `http://${request.headers.host}`);
  const path = url.pathname;

  if (request.method === "GET" && path === "/") {
    response.setHeader("set-cookie", "uid=alice; Path=/; SameSite=Lax");
    return sendFile(response, "./index.html", "text/html; charset=utf-8");
  }

  if (request.method === "GET" && path === "/client.js") {
    return sendFile(response, "./public/client.js", "text/javascript; charset=utf-8");
  }

  if (request.method === "GET" && path === "/api/users") {
    const orgId = url.searchParams.get("org_id");
    const rows = orgId
      ? db
          .prepare(
            "SELECT id, name, org_id, created_at FROM users WHERE org_id = ? ORDER BY id DESC LIMIT 50",
          )
          .all(Number(orgId))
      : db
          .prepare(
            "SELECT id, name, org_id, created_at FROM users ORDER BY id DESC LIMIT 50",
          )
          .all();
    return json(response, {
      rows: rows.map((row) => ({ ...row })),
      at: Date.now(),
    });
  }

  if (request.method === "POST" && path === "/api/seed") {
    const body = await readBody(request);
    const name = String(body.name ?? "").trim() || "anonymous";
    const orgId = Number(body.org_id) === 2 ? 2 : 1;
    db.prepare("INSERT INTO users (name, org_id) VALUES (?, ?)").run(name, orgId);
    // Fast path: flush the tailer now instead of waiting for the next poll.
    // Comment this out and the UI still updates, ~40ms later.
    hub.flush();
    return json(response, { ok: true });
  }

  if (request.method === "POST" && path === "/api/reset") {
    db.prepare("DELETE FROM users").run();
    hub.flush();
    return json(response, { ok: true });
  }

  if (path === "/live" || path.startsWith("/live/")) {
    return nodeLive(request, response);
  }

  response.statusCode = 404;
  response.end("Not found");
});

server.listen(PORT, () => {
  console.log(`\n  livelite demo → http://localhost:${PORT}`);
  console.log(`  tables: ${synced.tables.map((t) => t.name).join(", ")}`);
  console.log(`  try:    sqlite3 examples/demo/demo.db "insert into users (name, org_id) values ('from the CLI', 1)"\n`);
});

// ---------------------------------------------------------------- helpers

async function sendFile(response, name, type) {
  try {
    const body = await readFile(local(name));
    response.setHeader("content-type", type);
    response.setHeader("cache-control", "no-store");
    response.end(body);
  } catch {
    response.statusCode = 500;
    response.end(`Missing ${name}. Run: pnpm demo:build`);
  }
}

function json(response, data, status = 200) {
  const body = JSON.stringify(data);
  response.statusCode = status;
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.setHeader("cache-control", "no-store");
  response.end(body);
}

async function readBody(request, limit = 64 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > limit) throw new Error("body too large");
    chunks.push(chunk);
  }
  const text = Buffer.concat(chunks).toString("utf8");
  if (!text) return {};
  return JSON.parse(text);
}
