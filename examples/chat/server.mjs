import http from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

import { currentSeq, onWrite, syncLive } from "@hum/core";
import {
  createLiveHandler,
  LiveHub,
  TicketStore,
  toNodeHandler,
} from "@hum/server";

const local = (name) => fileURLToPath(new URL(name, import.meta.url));
const PORT = Number(process.env.PORT ?? 4478);
const DB_FILE = process.env.CHAT_DB ?? local("./chat.db");
const SEED_USERS = ["ada", "grace", "margaret", "barbara"];

// ---------------------------------------------------------------- database

const db = new DatabaseSync(DB_FILE);
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA busy_timeout = 3000");
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
  );
  CREATE TABLE IF NOT EXISTS rooms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
  );
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    body TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
  );
  CREATE TABLE IF NOT EXISTS reads (
    room_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    last_seen_id INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (room_id, user_id)
  );
  CREATE INDEX IF NOT EXISTS messages_room ON messages(room_id, id DESC);
`);

// Installs the outbox and one trigger per table per operation.
const synced = syncLive({ db, wal: false });

const ROOMS_SQL = `SELECT r.id, r.name,
     (SELECT COUNT(*) FROM messages m
        WHERE m.room_id = r.id
          AND m.id > COALESCE((SELECT last_seen_id FROM reads rd
                                WHERE rd.room_id = r.id AND rd.user_id = ?), 0)) AS unread,
     (SELECT MAX(m.id) FROM messages m WHERE m.room_id = r.id) AS last_id
   FROM rooms r
   ORDER BY r.id`;

const roomsFor = db.prepare(ROOMS_SQL);
const userByName = db.prepare("SELECT id, name FROM users WHERE name = ?");

if (Number(db.prepare("SELECT COUNT(*) AS c FROM users").get().c) === 0) {
  const addUser = db.prepare("INSERT INTO users (name) VALUES (?)");
  for (const name of SEED_USERS) addUser.run(name);

  const addRoom = db.prepare("INSERT INTO rooms (name) VALUES (?)");
  addRoom.run("general");
  addRoom.run("random");

  const say = db.prepare(
    "INSERT INTO messages (room_id, author_id, body) VALUES (?, ?, ?)",
  );
  const id = (name) => Number(userByName.get(name).id);
  say.run(1, id("ada"), "morning all — this room is backed by one SQLite file");
  say.run(1, id("grace"), "and every message is a plain insert, no socket code");
  say.run(1, id("margaret"), "open a second tab as someone else and say hi");
  say.run(2, id("barbara"), "random is quieter, but it still updates live");
}

// ---------------------------------------------------------------- identity

const users = new Map();

/** Demo-only: a cookie names the user, unknown names are created on demand. */
function identityForName(raw) {
  const name = String(raw ?? "").slice(0, 32) || "ada";

  let user = users.get(name);
  if (!user) {
    liveDb.prepare("INSERT OR IGNORE INTO users (name) VALUES (?)").run(name);
    user = { ...userByName.get(name) };
    users.set(name, user);
  }
  return { userId: Number(user.id), name: String(user.name) };
}

function identify(source) {
  // Accepts both a fetch `Request` (live handler) and a node `IncomingMessage`.
  const headers = source?.headers ?? source;
  const cookie =
    (typeof headers?.get === "function" ? headers.get("cookie") : headers?.cookie) ??
    "";
  const match = /(?:^|;\s*)uid=([^;]+)/.exec(cookie);
  return identityForName(decodeURIComponent(match?.[1] ?? "ada"));
}

// ---------------------------------------------------------------- queries

/**
 * The allowlist. Clients send a name + params, never SQL.
 *
 * `tables` is declared rather than inferred here: the unread count reads
 * `messages` and `reads` through correlated subqueries, and being explicit is
 * cheaper and more obvious than trusting planner output.
 */
const queries = {
  "rooms.forUser": {
    build: (ctx) => ({ sql: ROOMS_SQL, params: [ctx.userId] }),
    tables: ["rooms", "messages", "reads"],
  },
  "messages.latest": {
    params: (raw) => {
      const roomId = Number(raw?.room_id);
      if (!Number.isInteger(roomId)) throw new Error("room_id required");
      return { room_id: roomId };
    },
    build: (_ctx, params) => ({
      sql: `SELECT m.id, m.room_id, m.body, m.created_at, m.author_id, u.name AS author
            FROM messages m JOIN users u ON u.id = m.author_id
            WHERE m.room_id = ?
            ORDER BY m.id DESC
            LIMIT 50`,
      params: [params.room_id],
    }),
    tables: ["messages", "users"],
  },
};

const hub = new LiveHub({
  db,
  queries,
  pollMs: 250,
  compactEveryMs: 5_000,
  keep: 1000,
  // Unread counts are per-user, so subscriptions must not be shared across
  // identities. This is the tenant bucket from the docs, with one user = one
  // tenant.
  scope: (ctx) => String(ctx.userId),
  identity: (ctx) => String(ctx.userId),
});
hub.start();

// In-process writes flush immediately; the CLI/CLI-style writes still land
// within the poll interval.
const liveDb = onWrite(db, () => hub.flush());

const tickets = new TicketStore();

const live = createLiveHandler(hub, tickets, {
  authenticate: identify,
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
    const requested = (url.searchParams.get("as") ?? "ada").slice(0, 32);
    response.setHeader("set-cookie", `uid=${encodeURIComponent(requested)}; Path=/; SameSite=Lax`);
    const me = identityForName(requested);
    const html = await readFile(local("./index.html"), "utf8");
    response.setHeader("content-type", "text/html; charset=utf-8");
    response.setHeader("cache-control", "no-store");
    return response.end(
      html.replace(
        "__ME_JSON__",
        JSON.stringify(me).replace(/</g, "\\u003c"),
      ),
    );
  }

  if (request.method === "GET" && path === "/client.js") {
    return sendFile(response, "./public/client.js", "text/javascript; charset=utf-8");
  }

  if (request.method === "GET" && path === "/api/me") {
    return json(response, identify(request));
  }

  if (request.method === "GET" && path === "/api/users") {
    const rows = db
      .prepare("SELECT id, name FROM users ORDER BY id")
      .all()
      .map((row) => ({ ...row }));
    return json(response, { rows });
  }

  if (request.method === "GET" && path === "/api/rooms") {
    const me = identify(request);
    const rows = roomsFor.all(me.userId).map((row) => ({ ...row }));
    return json(response, { rows });
  }

  if (request.method === "GET" && path === "/api/messages") {
    const roomId = Number(url.searchParams.get("room_id"));
    if (!Number.isInteger(roomId)) return json(response, { error: "room_id" }, 400);
    const before = Number(url.searchParams.get("before"));
    const rows = Number.isInteger(before) && before > 0
      ? db
          .prepare(
            `SELECT m.id, m.room_id, m.body, m.created_at, m.author_id, u.name AS author
             FROM messages m JOIN users u ON u.id = m.author_id
             WHERE m.room_id = ? AND m.id < ?
             ORDER BY m.id DESC LIMIT 30`,
          )
          .all(roomId, before)
      : db
          .prepare(
            `SELECT m.id, m.room_id, m.body, m.created_at, m.author_id, u.name AS author
             FROM messages m JOIN users u ON u.id = m.author_id
             WHERE m.room_id = ?
             ORDER BY m.id DESC LIMIT 50`,
          )
          .all(roomId);
    return json(response, { rows: rows.map((row) => ({ ...row })) });
  }

  if (request.method === "POST" && path === "/api/messages") {
    const me = identify(request);
    const body = await readBody(request);
    const roomId = Number(body.room_id);
    const text = String(body.body ?? "").trim();
    if (!Number.isInteger(roomId) || !text) {
      return json(response, { error: "room_id and body required" }, 400);
    }
    if (text.length > 2000) return json(response, { error: "too long" }, 413);

    const result = liveDb
      .prepare("INSERT INTO messages (room_id, author_id, body) VALUES (?, ?, ?)")
      .run(roomId, me.userId, text);

    return json(response, {
      ok: true,
      id: Number(result.lastInsertRowid),
      seq: currentSeq(db),
    });
  }

  if (request.method === "POST" && path === "/api/read") {
    const me = identify(request);
    const body = await readBody(request);
    const roomId = Number(body.room_id);
    const lastSeen = Number(body.last_seen_id);
    if (!Number.isInteger(roomId) || !Number.isInteger(lastSeen)) {
      return json(response, { error: "room_id and last_seen_id required" }, 400);
    }

    liveDb
      .prepare(
        `INSERT INTO reads (room_id, user_id, last_seen_id) VALUES (?, ?, ?)
         ON CONFLICT(room_id, user_id)
         DO UPDATE SET last_seen_id = MAX(last_seen_id, excluded.last_seen_id)`,
      )
      .run(roomId, me.userId, lastSeen);

    return json(response, { ok: true });
  }

  if (path === "/live" || path.startsWith("/live/")) {
    return nodeLive(request, response);
  }

  response.statusCode = 404;
  response.end("Not found");
});

server.listen(PORT, () => {
  console.log(`\n  hum chat → http://localhost:${PORT}`);
  console.log(`  tables: ${synced.tables.map((t) => t.name).join(", ")}`);
  console.log(`  try:   open ?as=grace in a second window, or run pnpm chat:bench\n`);
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
    response.end(`Missing ${name}. Run: pnpm chat:build`);
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
