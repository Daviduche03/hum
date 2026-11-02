import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import assert from "node:assert/strict";
import { test } from "node:test";

import {
  checkTriggers,
  compact,
  currentSeq,
  onWrite,
  syncLive,
  Tailer,
  type ChangeBatch,
} from "../src/index.ts";

function openDb() {
  const dir = mkdtempSync(join(tmpdir(), "hum-"));
  const db = new DatabaseSync(join(dir, "test.db"));
  db.exec("PRAGMA journal_mode = WAL");
  db.exec(`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      org_id INTEGER NOT NULL,
      name TEXT NOT NULL
    );
    CREATE TABLE posts (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      title TEXT NOT NULL
    );
  `);
  return {
    db,
    dir,
    close() {
      db.close();
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

function outbox(db: DatabaseSync) {
  return db
    .prepare(`SELECT seq, tbl, op, pk FROM _live_changes ORDER BY seq`)
    .all()
    .map((row: any) => ({ ...row }));
}

test("captures insert, update and delete as pk-only change rows", () => {
  const h = openDb();
  try {
    syncLive({ db: h.db });

    h.db.prepare("INSERT INTO users (id, org_id, name) VALUES (?, ?, ?)").run(1, 7, "ada");
    h.db.prepare("UPDATE users SET name = ? WHERE id = ?").run("ada l", 1);
    h.db.prepare("DELETE FROM users WHERE id = ?").run(1);

    const rows = outbox(h.db);
    assert.equal(rows.length, 3);
    assert.deepEqual(
      rows.map((r) => [r.tbl, r.op, r.pk]),
      [
        ["users", "insert", "[1]"],
        ["users", "update", "[1]"],
        ["users", "delete", "[1]"],
      ],
    );
  } finally {
    h.close();
  }
});

test("handles composite primary keys", () => {
  const h = openDb();
  try {
    h.db.exec(`CREATE TABLE memberships (
      org_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      role TEXT NOT NULL,
      PRIMARY KEY (org_id, user_id)
    )`);
    syncLive({ db: h.db });

    h.db.prepare("INSERT INTO memberships VALUES (?, ?, ?)").run(7, 42, "admin");
    const rows = outbox(h.db);
    assert.equal(rows.length, 1);
    assert.equal(rows[0]!.pk, "[7,42]");
  } finally {
    h.close();
  }
});

test("falls back to rowid when a table has no primary key", () => {
  const h = openDb();
  try {
    h.db.exec(`CREATE TABLE notes (body TEXT NOT NULL)`);
    syncLive({ db: h.db });

    h.db.prepare("INSERT INTO notes (body) VALUES (?)").run("hello");
    const rows = outbox(h.db);
    assert.equal(rows[0]!.pk, "[1]");
  } finally {
    h.close();
  }
});

test("sync is idempotent and leaves no orphan triggers", () => {
  const h = openDb();
  try {
    syncLive({ db: h.db });
    const first = h.db
      .prepare(`SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name`)
      .all()
      .map((r: any) => r.name);

    syncLive({ db: h.db });
    const second = h.db
      .prepare(`SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name`)
      .all()
      .map((r: any) => r.name);

    assert.deepEqual(first, second);
    assert.equal(first.length, 6); // 2 tables x 3 operations

    h.db.exec("DROP TABLE posts");
    const result = syncLive({ db: h.db });
    assert.ok(result.triggers.every((name: string) => !name.includes("posts")));
    const remaining = h.db
      .prepare(`SELECT name FROM sqlite_master WHERE type = 'trigger'`)
      .all()
      .map((r: any) => r.name);
    assert.equal(remaining.length, 3);
  } finally {
    h.close();
  }
});

test("checkTriggers reports missing and orphaned triggers", () => {
  const h = openDb();
  try {
    syncLive({ db: h.db });
    assert.deepEqual(checkTriggers(h.db), { missing: [], orphaned: [] });

    h.db.exec(`DROP TRIGGER _live_users_au`);
    assert.deepEqual(checkTriggers(h.db).missing, ["_live_users_au"]);
  } finally {
    h.close();
  }
});

test("tailer coalesces a burst into one batch of table names", () => {
  const h = openDb();
  try {
    syncLive({ db: h.db });
    h.db.prepare("INSERT INTO users (id, org_id, name) VALUES (?, ?, ?)").run(1, 1, "a");
    h.db.prepare("INSERT INTO users (id, org_id, name) VALUES (?, ?, ?)").run(2, 1, "b");
    h.db.prepare("INSERT INTO posts (id, user_id, title) VALUES (?, ?, ?)").run(1, 1, "t");

    const batches: ChangeBatch[] = [];
    const tailer = new Tailer({
      db: h.db,
      startAt: "beginning",
      pollMs: 10_000,
      onBatch: (batch) => batches.push(batch),
    });
    tailer.poll();

    assert.equal(batches.length, 1);
    assert.equal(batches[0]!.count, 3);
    assert.deepEqual(batches[0]!.tables.sort(), ["posts", "users"]);
    assert.equal(batches[0]!.to, currentSeq(h.db));
    tailer.stop();
  } finally {
    h.close();
  }
});

test("retention truncates and the tailer asks for a resync", () => {
  const h = openDb();
  try {
    syncLive({ db: h.db });
    const insert = h.db.prepare(
      "INSERT INTO users (id, org_id, name) VALUES (?, ?, ?)",
    );
    for (let i = 1; i <= 10; i += 1) insert.run(i, 1, `u${i}`);

    const result = compact({ db: h.db, minCursor: 6, keep: 0 });
    assert.equal(result.truncatedThrough, 5);
    assert.equal(result.deleted, 5);

    let resync: { seq: number; reason: string } | null = null;
    const tailer = new Tailer({
      db: h.db,
      startAt: 2,
      pollMs: 10_000,
      onBatch: () => {
        throw new Error("should not emit a batch");
      },
      onResync: (info) => {
        resync = info;
      },
    });
    tailer.poll();

    assert.ok(resync);
    assert.equal((resync as any).reason, "truncated");
    assert.equal((resync as any).seq, 5);

    // And a fresh cursor past the truncation still gets the tail.
    const batches: ChangeBatch[] = [];
    const fresh = new Tailer({
      db: h.db,
      startAt: 5,
      pollMs: 10_000,
      onBatch: (batch) => batches.push(batch),
    });
    fresh.poll();
    assert.equal(batches[0]!.from, 6);
    assert.equal(batches[0]!.count, 5);
    tailer.stop();
    fresh.stop();
  } finally {
    h.close();
  }
});

test("syncLive does not rewrite triggers that already match", () => {
  const h = openDb();
  try {
    const first = syncLive({ db: h.db });
    assert.equal(first.changed, true);

    const second = syncLive({ db: h.db });
    assert.equal(second.changed, false);
    assert.deepEqual(second.triggers, first.triggers);
  } finally {
    h.close();
  }
});

test("capture: table writes table-level markers instead of primary keys", () => {
  const h = openDb();
  try {
    syncLive({ db: h.db, capture: "table" });

    const insert = h.db.prepare(
      "INSERT INTO users (id, org_id, name) VALUES (?, ?, ?)",
    );
    for (let i = 1; i <= 200; i += 1) insert.run(i, 1, `u${i}`);

    const rows = outbox(h.db);
    assert.equal(rows.length, 200);
    assert.ok(rows.every((row) => row.pk === null));

    // Switching back rewrites the triggers, since their DDL differs.
    const back = syncLive({ db: h.db, capture: "row" });
    assert.equal(back.changed, true);
  } finally {
    h.close();
  }
});

test("checkTriggers honours the same include/exclude filters as syncLive", () => {
  const h = openDb();
  try {
    const exclude = (table: string) => table === "posts";
    syncLive({ db: h.db, exclude });

    assert.deepEqual(checkTriggers(h.db, { exclude }), {
      missing: [],
      orphaned: [],
    });

    // Without the filter, the skipped table looks like drift.
    assert.ok(checkTriggers(h.db).missing.some((name) => name.includes("posts")));
  } finally {
    h.close();
  }
});

test("onWrite notifies after writes, not after reads", () => {
  const h = openDb();
  try {
    syncLive({ db: h.db });

    let notifications = 0;
    const live = onWrite(h.db, () => {
      notifications += 1;
    });

    live.prepare("INSERT INTO users (id, org_id, name) VALUES (?, ?, ?)").run(1, 1, "a");
    assert.equal(notifications, 1);

    live.prepare("SELECT COUNT(*) AS c FROM users").all();
    assert.equal(notifications, 1);

    live.prepare("UPDATE users SET name = ? WHERE id = ?").run("b", 1);
    assert.equal(notifications, 2);

    live.exec("PRAGMA user_version = 1");
    assert.equal(notifications, 3);

    // Reads through the proxy still return real data.
    const row = live.prepare("SELECT name FROM users WHERE id = ?").get(1) as {
      name: string;
    };
    assert.equal(row.name, "b");
  } finally {
    h.close();
  }
});

test("an outbox created with a NOT NULL pk is migrated in place", () => {
  const h = openDb();
  try {
    h.db.exec(`CREATE TABLE _live_changes (
      "seq" INTEGER PRIMARY KEY AUTOINCREMENT,
      "tbl" TEXT NOT NULL,
      "op" TEXT NOT NULL,
      "pk" TEXT NOT NULL,
      "ts" INTEGER NOT NULL DEFAULT 0
    )`);
    h.db.prepare(
      `INSERT INTO _live_changes ("tbl", "op", "pk") VALUES ('users', 'insert', '[9]')`,
    ).run();

    syncLive({ db: h.db, capture: "table" });

    const rows = outbox(h.db);
    assert.equal(rows.length, 1);
    assert.equal(rows[0]!.pk, "[9]");

    // Table-level capture can now write NULL.
    h.db.prepare("INSERT INTO users (id, org_id, name) VALUES (?, ?, ?)").run(1, 1, "a");
    assert.equal(outbox(h.db).at(-1)!.pk, null);
  } finally {
    h.close();
  }
});
