import assert from "node:assert/strict";
import { test } from "node:test";

import { Connection, LiveHub } from "../src/index.ts";
import {
  connect,
  delay,
  openStream,
  sseReader,
  start,
  subscribe,
} from "./harness.ts";

test("delivers a change for a subscribed table", async () => {
  const h = await start();
  try {
    const { connId, ticket } = await connect(h.base);

    const stream = await fetch(`${h.base}/live/stream?ticket=${ticket}`);
    assert.equal(stream.status, 200);
    assert.match(stream.headers.get("content-type") ?? "", /text\/event-stream/);

    const events = sseReader(stream);
    await events.next("hello");

    const sub = await subscribe(h.base, connId, "q1", "users.all");
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

    // users.byId declares tables explicitly; users.all relies on inference.
    assert.equal((await subscribe(h.base, connId, "q1", "users.byId", { id: 1 })).status, 200);

    const inferred = await subscribe(h.base, connId, "q2", "users.all");
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

    const unknown = await subscribe(h.base, connId, "q1", "does.not.exist");
    assert.equal(unknown.status, 403);
    assert.deepEqual(await unknown.json(), { error: "unavailable" });

    const guest = await connect(h.base, { "x-role": "guest" });
    const denied = await fetch(`${h.base}/live/sub`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-role": "guest" },
      body: JSON.stringify({ connId: guest.connId, id: "q1", name: "secret.only" }),
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
    const bad = await subscribe(h.base, connId, "q1", "users.byId", {});
    assert.equal(bad.status, 400);
  } finally {
    await h.close();
  }
});

test("a query that declares tables needs no SQL at all", async () => {
  const h = await start();
  try {
    const { connId } = await connect(h.base);
    const response = await subscribe(h.base, connId, "q1", "users.tablesOnly");
    assert.equal(response.status, 200);

    const body = (await response.json()) as { tables: string[] };
    assert.deepEqual(body.tables, ["users"]);
  } finally {
    await h.close();
  }
});

test("a query with neither build nor tables fails at construction", async () => {
  const h = await start();
  try {
    assert.throws(
      () =>
        new LiveHub({
          db: h.db,
          queries: { broken: {} as never },
        }),
      /needs either/,
    );
  } finally {
    await h.close();
  }
});

test("an oversized body is rejected before it is buffered", async () => {
  const h = await start({ bodyLimit: 32 });
  try {
    const { connId } = await connect(h.base);
    const response = await fetch(`${h.base}/live/sub`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ connId, id: "q1", name: "users.all", pad: "x".repeat(256) }),
    });
    assert.equal(response.status, 413);
    assert.deepEqual(await response.json(), { error: "unavailable" });
  } finally {
    await h.close();
  }
});

class FakeController {
  desiredSize: number | null = 1;
  closed = false;
  readonly chunks: string[] = [];
  private readonly decoder = new TextDecoder();

  enqueue(chunk: Uint8Array): void {
    this.chunks.push(this.decoder.decode(chunk));
  }

  close(): void {
    this.closed = true;
  }

  text(): string {
    return this.chunks.join("");
  }
}

test("a backpressure resync is delivered even if the stream goes quiet", () => {
  const connection = new Connection("c1", {}, "u1", 0);
  const controller = new FakeController();
  connection.attach(controller as unknown as ReadableStreamDefaultController<Uint8Array>);

  // Consumer stops reading.
  controller.desiredSize = 0;
  connection.lastSeq = 7;
  connection.send("change", { seq: 7, ids: ["q1"] });

  assert.doesNotMatch(controller.text(), /event: change/);
  assert.doesNotMatch(controller.text(), /event: resync/);

  // The consumer drains the queue. No further writes happen.
  controller.desiredSize = 1;
  connection.flushResync();

  const text = controller.text();
  assert.match(text, /event: resync/);
  const frame = text.slice(text.indexOf("event: resync"));
  const data = JSON.parse(frame.split("\ndata: ")[1]!.split("\n")[0]!);
  assert.equal(data.seq, 7);
  assert.equal(data.reason, "backpressure");
  assert.equal(data.dropped, 1);

  connection.close();
});

test("a pending resync is flushed before the next notification", () => {
  const connection = new Connection("c1", {}, "u1", 0);
  const controller = new FakeController();
  connection.attach(controller as unknown as ReadableStreamDefaultController<Uint8Array>);

  controller.desiredSize = 0;
  connection.send("change", { seq: 1, ids: ["q1"] });
  connection.send("change", { seq: 2, ids: ["q1"] });

  controller.desiredSize = 1;
  connection.send("change", { seq: 3, ids: ["q1"] });

  const text = controller.text();
  const resync = text.indexOf("event: resync");
  const change = text.indexOf("event: change");
  assert.ok(resync >= 0 && change >= 0);
  assert.ok(resync < change, "resync must precede the newer change frame");

  connection.close();
});

test("a connection that overflows before attaching gets a resync, in order", () => {
  const connection = new Connection("c1", {}, "u1", 0);
  for (let seq = 1; seq <= 260; seq += 1) {
    connection.send("change", { seq, ids: ["q1"] });
  }

  const controller = new FakeController();
  connection.attach(controller as unknown as ReadableStreamDefaultController<Uint8Array>);

  const text = controller.text();
  assert.ok(text.indexOf("event: hello") >= 0, "handshake first");
  assert.ok(
    text.indexOf("event: hello") < text.indexOf("event: resync"),
    "resync must not precede the handshake",
  );
  assert.equal((text.match(/event: change/g) ?? []).length, 256);

  connection.close();
});

test("connections that never open a stream are reaped", async () => {
  const h = await start({ connectionTtlMs: 1, compactEveryMs: 5 });
  try {
    const connection = h.hub.createConnection({ userId: "u1" });
    assert.equal(h.hub.connections.has(connection.id), true);

    await delay(50);
    assert.equal(h.hub.connections.has(connection.id), false);
  } finally {
    await h.close();
  }
});

test("a stream that was opened is not reaped by the connection TTL", async () => {
  const h = await start({ connectionTtlMs: 30, compactEveryMs: 5 });
  try {
    const stream = await openStream(h.base);
    await delay(100);

    assert.equal(h.hub.connections.has(stream.connId), true);
    await stream.close();
  } finally {
    await h.close();
  }
});
