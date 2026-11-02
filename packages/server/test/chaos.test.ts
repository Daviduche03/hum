import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";

import { compact, currentSeq } from "@hum/core";
import { delay, openStream, start, subscribe } from "./harness.ts";

/**
 * The invariant the whole product rests on: a subscriber may refetch more than
 * necessary, but must never be left believing stale data is current.
 *
 * This drives it the ugly way — writes from a second connection (another
 * process), retention deleting rows out from under the tailer, and a client
 * reconnecting halfway through — then asserts every live stream observed a
 * frame at or beyond the final write.
 */
test("no subscriber is left stale across writers, truncation and reconnects", async () => {
  const h = await start({ pollMs: 5, compactEveryMs: 0, keep: 0 });
  const writer = new DatabaseSync(h.dbPath);
  writer.exec("PRAGMA journal_mode = WAL");

  const streams = [];
  try {
    for (let i = 0; i < 3; i += 1) {
      const stream = await openStream(h.base);
      const response = await subscribe(h.base, stream.connId, "q1", "users.all");
      assert.equal(response.status, 200);
      streams.push(stream);
    }

    const insert = writer.prepare("INSERT INTO users (id, name) VALUES (?, ?)");

    for (let i = 1; i <= 80; i += 1) {
      insert.run(i, `user-${i}`);

      // Truncate everything the tailer has not read yet. Clients must recover
      // via resync, not silently skip.
      if (i % 9 === 0) {
        compact({ db: h.db, minCursor: currentSeq(h.db) + 1, keep: 0 });
      }

      // Sometimes flush immediately, sometimes let the poll find it.
      if (i % 4 === 0) h.hub.flush();

      // Reconnect a client mid-stream.
      if (i === 40) {
        await streams[1]!.close();
        const replacement = await openStream(h.base);
        const response = await subscribe(h.base, replacement.connId, "q1", "users.all");
        assert.equal(response.status, 200);
        streams[1] = replacement;
      }

      if (i % 5 === 0) await delay(2);
    }

    const finalInsert = insert.run(81, "user-81");
    assert.ok(finalInsert);
    const finalSeq = currentSeq(h.db);
    h.hub.flush();

    // Let the last frames land.
    await delay(150);

    assert.equal(
      Number(writer.prepare("SELECT COUNT(*) AS c FROM users").get()!.c),
      81,
    );

    for (const [index, stream] of streams.entries()) {
      const covered = stream.events
        .filter((event) => event.event === "change" || event.event === "resync")
        .reduce((max, event) => Math.max(max, Number(event.data?.seq ?? 0)), 0);

      assert.ok(
        covered >= finalSeq,
        `stream ${index} only reached seq ${covered}, expected ${finalSeq}`,
      );

      // And it really did receive something, rather than starting at the head.
      assert.ok(
        stream.events.some(
          (event) => event.event === "change" || event.event === "resync",
        ),
        `stream ${index} received no notifications`,
      );
    }
  } finally {
    for (const stream of streams) await stream.close();
    writer.close();
    await h.close();
  }
});
