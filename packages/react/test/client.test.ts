import assert from "node:assert/strict";
import { test } from "node:test";

import { LiveClient } from "../src/client.ts";

class FakeEventSource {
  static instances: FakeEventSource[] = [];

  readonly url: string;
  closed = false;
  private listeners = new Map<string, Set<(event: any) => void>>();

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (event: any) => void): void {
    let set = this.listeners.get(type);
    if (!set) {
      set = new Set();
      this.listeners.set(type, set);
    }
    set.add(listener);
  }

  close(): void {
    this.closed = true;
  }

  emit(type: string, data: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener({ data: JSON.stringify(data) });
    }
  }
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function setup() {
  FakeEventSource.instances = [];
  const calls: string[] = [];
  let version = 1;

  const fakeFetch = async (input: any, init?: any): Promise<Response> => {
    const url = String(input);
    calls.push(`${init?.method ?? "GET"} ${url}`);
    if (url.endsWith("/live/connect")) {
      return json({
        connId: `c${FakeEventSource.instances.length + 1}`,
        ticket: "t",
      });
    }
    if (url.endsWith("/live/sub")) return json({ seq: 1, tables: ["users"] });
    if (url.endsWith("/live/unsub")) return json({ ok: true });
    if (url.endsWith("/api/users")) {
      return json({ version, rows: [{ id: version }] });
    }
    return json({ error: "not found" }, 404);
  };

  const client = new LiveClient({
    basePath: "/live",
    fetch: fakeFetch as typeof fetch,
    EventSource: FakeEventSource as any,
    retryBaseMs: 5,
    retryMaxMs: 20,
  });

  const load = async () => {
    const response = await fakeFetch("/api/users");
    return response.json();
  };

  return {
    client,
    calls,
    load,
    bump: () => {
      version += 1;
      return version;
    },
  };
}

async function waitFor(check: () => boolean, label = "condition"): Promise<void> {
  const deadline = Date.now() + 2000;
  while (!check()) {
    if (Date.now() > deadline) throw new Error(`timeout waiting for ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

test("fetches on mount and refetches when the server reports a change", async () => {
  const { client, load, bump } = setup();
  try {
    const entry = client.entry("users.all", {}, load);

    await waitFor(() => entry.snapshot.loading === false, "initial fetch");
    assert.deepEqual(entry.snapshot.data, { version: 1, rows: [{ id: 1 }] });
    assert.equal(entry.snapshot.error, undefined);

    await waitFor(
      () => FakeEventSource.instances.length > 0,
      "event source created",
    );

    const source = FakeEventSource.instances.at(-1)!;
    bump();
    source.emit("change", { seq: 2, ids: [entry.id] });

    await waitFor(
      () => (entry.snapshot.data as any)?.version === 2,
      "refetch after change",
    );
    assert.equal(entry.snapshot.seq, 2);
  } finally {
    client.dispose();
  }
});

test("dedupes identical queries and unsubscribes at zero references", async () => {
  const { client, calls, load } = setup();
  try {
    const a = client.entry("users.all", { org: 1 }, load);
    const b = client.entry("users.all", { org: 1 }, load);

    assert.equal(a, b);
    assert.equal(a.refs, 2);

    await waitFor(
      () => calls.some((call) => call.includes("/live/sub")),
      "subscribe sent",
    );

    assert.equal(
      calls.filter((call) => call.includes("/live/sub")).length,
      1,
    );

    client.release(b);
    assert.equal(a.refs, 1);

    client.release(a);
    assert.equal(a.refs, 0);
    await waitFor(
      () => calls.some((call) => call.includes("/live/unsub")),
      "unsubscribe sent",
    );
  } finally {
    client.dispose();
  }
});

test("reconnects with a fresh ticket and resubscribes", async () => {
  const { client, calls, load } = setup();
  try {
    const entry = client.entry("users.all", {}, load);
    await waitFor(
      () => FakeEventSource.instances.length === 1,
      "first connection",
    );

    const first = FakeEventSource.instances[0]!;
    first.emit("error", {});

    await waitFor(
      () => FakeEventSource.instances.length === 2,
      "reconnect attempt",
    );

    assert.equal(first.closed, true);
    assert.ok(
      calls.filter((call) => call.includes("/live/connect")).length >= 2,
    );
    await waitFor(
      () => calls.filter((call) => call.includes("/live/sub")).length >= 2,
      "resubscribe after reconnect",
    );
    assert.ok(entry.refs >= 1);
  } finally {
    client.dispose();
  }
});
