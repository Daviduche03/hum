# hum

Make any SQLite database realtime for React, without adopting a new framework.

Writes from Drizzle, raw SQL, a migration, or `sqlite3` in a terminal all show
up in the browser. The server sends "this query is stale"; your existing loader
refetches.

```
write → trigger → _live_changes → tailer → SSE → useLiveQuery → your fetcher()
```

- **Works with any TS backend** that can serve a `(Request) => Response`.
- **Works with Drizzle out of the box** — no hooks, no codegen, no schema changes
  beyond one outbox table.
- **No client replica.** The browser still asks your API for data, so your auth
  is the only auth.

## Run the demo

```bash
pnpm install
pnpm demo:build
pnpm demo            # http://localhost:4477
```

Then, in another terminal, write outside the app and watch both panels update:

```bash
sqlite3 examples/demo/demo.db \
  "insert into users (name, org_id) values ('from the CLI', 1)"
```

Open the page twice to see both tabs update from one write.

## Quick start

### 1. Install the capture layer

Call this once at boot and after migrations. It creates `_live_changes` and one
trigger per table per operation. Idempotent.

```ts
import { DatabaseSync } from "node:sqlite";
import { syncLive, checkTriggers } from "@hum/core";

const db = new DatabaseSync("./app.db");
const result = syncLive({ db });
// result.tables: [{ name: "users", pk: ["id"], rowid: false }, ...]

// Optional boot check — catches triggers dropped by `drizzle-kit push`.
const drift = checkTriggers(db);
if (drift.missing.length) console.warn("missing triggers", drift.missing);
```

`better-sqlite3` and `bun:sqlite` satisfy the same `LiveDb` interface, so swap
the constructor and everything else is unchanged.

### 2. Register the queries clients may subscribe to

```ts
import { LiveHub, TicketStore, createLiveHandler } from "@hum/server";

const queries = {
  "users.all": {
    build: () => ({
      sql: "select id, name, org_id from users order by id desc limit 50",
      params: [],
    }),
  },
  "users.byOrg": {
    params: (raw) => {
      const orgId = Number(raw?.org_id);
      if (!Number.isInteger(orgId)) throw new Error("org_id");
      return { org_id: orgId };
    },
    // Tables are inferred from EXPLAIN QUERY PLAN. Override with
    // `tables: ["users"]` when you want precision.
    build: (ctx, params) => ({
      sql: "select id, name, org_id from users where org_id = ? order by id desc",
      params: [params.org_id],
    }),
    authorize: (ctx) => ctx.userId != null,
  },
};

const hub = new LiveHub({
  db,
  queries,
  // Put identity in the subscription key. Required for shared multi-tenant DBs.
  scope: (ctx) => ctx.orgId,
  identity: (ctx) => ctx.userId,
});
hub.start();

const tickets = new TicketStore();

export const live = createLiveHandler(hub, tickets, {
  authenticate: async (request) => {
    const user = await verifySession(request); // your code
    return { userId: user.id, orgId: user.orgId };
  },
  origin: ["https://app.example.com"],
});
```

### 3. Mount it

**Node HTTP / Express** (Express `req`/`res` are compatible):

```ts
import { toNodeHandler } from "@hum/server";
app.use(toNodeHandler(live)); // mount at the root so /live/* is preserved
```

**Hono**

```ts
app.all("/live/*", async (c) => (await live(c.req.raw)) ?? c.notFound());
```

**Next.js** (`app/live/[...path]/route.ts`)

```ts
export const GET = (request: Request) => live(request);
export const POST = (request: Request) => live(request);
```

**Bun / Deno** — call `live(request)` directly from the fetch handler.

### 4. React

```tsx
import { LiveClient, useLiveQuery } from "@hum/react";

const client = new LiveClient({ basePath: "/live" });

function Users({ orgId }: { orgId: number }) {
  const { data, loading, error, refetch } = useLiveQuery({
    client,
    name: "users.byOrg",
    params: { org_id: orgId },
    fetcher: async () => {
      const response = await fetch(`/api/users?org_id=${orgId}`);
      return response.json();
    },
  });

  if (loading) return <p>Loading…</p>;
  if (error) return <p>{error.message}</p>;
  return <ul>{data.rows.map((u) => <li key={u.id}>{u.name}</li>)}</ul>;
}
```

Components that pass the same `name` + `params` share one subscription and one
in-flight fetch. Unmounting the last one unsubscribes.

## Write path

The outbox is polled, not pushed: `pollMs` defaults to 250ms, which covers
writes made by *other* processes (a CLI, a migration, a cron job). For writes
that go through the same process, tell the hub instead of waiting:

```ts
const hub = new LiveHub({ db, queries });
hub.start();

// Wrap the handle your app already uses. `run()` and `exec()` flush the
// tailer immediately; reads are untouched.
const live = onWrite(db, () => hub.flush());

live.prepare("insert into users (name, org_id) values (?, ?)").run(name, orgId);
```

In-process latency is then whatever your own query costs — typically under a
millisecond, not 250ms. Forgetting to flush is not a correctness bug, only a
latency one: the poll still finds the change.

## Drizzle

Drizzle emits plain SQL, so capture needs no integration. The helper in
`@hum/core` exists to keep types and dependency inference convenient:

```ts
import { eq } from "drizzle-orm";
import { defineDrizzleQuery } from "@hum/core";

const queries = {
  "users.byOrg": defineDrizzleQuery({
    build: (ctx) => db.select().from(users).where(eq(users.orgId, ctx.orgId)),
  }),
};
```

`defineDrizzleQuery` compiles with `.toSQL()`. The SQL is used for dependency
inference and the params become part of the subscription key.

> **After every `drizzle-kit push`:** tables are recreated, which drops their
> triggers. Call `syncLive({ db })` at boot (or in a `predev`/`predeploy`
> script) and optionally assert `checkTriggers(db).missing` is empty.

## API surface

### `@hum/core`

| Export | Purpose |
|---|---|
| `syncLive({ db, include?, exclude?, capture?, outbox?, meta? })` | Create outbox + triggers. Idempotent, and a no-op when the DDL already matches. |
| `checkTriggers(db, { include?, exclude? })` | `{ missing, orphaned }` for boot-time drift detection. Pass the same filters you gave `syncLive`. |
| `Tailer` | Cursor over `_live_changes`; emits coalesced `{ from, to, tables, count }`. |
| `compact({ db, minCursor?, keep? })` | Prune the outbox and record `truncated_through`. |
| `tablesForSql(db, sql, params, known?)` | Planner-derived table dependencies. |
| `onWrite(db, notify)` | Proxy that calls `notify` after `run()`/`exec()`. Pair with `hub.flush()`. |
| `defineDrizzleQuery({ build, params?, tables?, authorize? })` | Drizzle builder → query definition. Also `compileDrizzle`, `drizzleTables`, `defineRegistry`. |
| `currentSeq(db)`, `stableStringify(value)` | Small helpers. |

`capture` controls what a trigger writes: `"row"` (default) stores the primary
key of each affected row, `"table"` stores `NULL`. Invalidation is table-level
either way, so `"table"` is strictly cheaper on bulk writes and correct for the
delivered pipeline; `"row"` keeps the data needed for future row patches.

### `@hum/server`

| Export | Purpose |
|---|---|
| `LiveHub({ db, queries, scope?, identity?, pollMs?, keep?, connectionTtlMs? })` | Registry, fan-out, tailer, retention. |
| `createLiveHandler(hub, tickets, { authenticate, origin?, bodyLimit? })` | `(Request) => Response` router. |
| `toNodeHandler(handler)` | Node/Express adapter. |
| `TicketStore`, `TicketStoreLike` | Single-use stream tickets (30s default). Implement the interface to share them across processes. |

### `@hum/react`

| Export | Purpose |
|---|---|
| `LiveClient({ basePath?, fetch?, EventSource?, retry? })` | One SSE connection per tab, refcounted subscriptions. |
| `useLiveQuery({ client, name, params, fetcher })` | `{ data, error, loading, seq, updatedAt, refetch }`. |

## Semantics and costs

- **Invalidation, not replication.** The message is `{ seq, ids }`. Nothing else
  crosses the wire, so a client cannot learn rows it is not allowed to read.
- **Whole-result refetch.** A change to `users` refetches every subscribed
  `users` query. Simple and correct; bandwidth is the trade-off. Coalescing
  means a 500-row import produces one message, not 500.
- **Coalesced per commit.** The tailer folds a burst into one batch of table
  names.
- **At-least-once, idempotent.** Duplicates are harmless; `seq` guards ordering.
- **A dropped frame always ends in a resync.** Under backpressure the server
  discards change frames — they carry no data — and queues exactly one `resync`
  for the moment the consumer catches up. The pending resync survives a quiet
  stream (it is flushed from `pull()`, the next send, or the heartbeat), so a
  slow tab can be late but never silently stale.

## Deployment

`/live` must be served by **one process per database** — or by replicas that
share state. The outbox is cross-process by design, so any number of processes
can *write*; what is not shared is the connection registry and `TicketStore`.
A ticket minted by process A cannot be redeemed by process B, and fan-out only
reaches connections held by the process that tails the row.

If you must run more than one instance, either pin `/live` to a single instance
(shuffle sharding, sticky routes, a single live service) or supply a
`TicketStoreLike` backed by shared storage *and* a cross-process transport for
fan-out. Sticky sessions alone are not enough: the tailer that sees a write has
to reach every connection.

Connections are per-tab and in-memory. `connectionTtlMs` (30s) reaps ones that
were created and never opened a stream.

## Security checklist

- [ ] `authenticate` verifies your real session; tickets are only a handshake.
- [ ] Identity is in `scope` if one SQLite file holds multiple tenants.
- [ ] Every query either scopes by `ctx` or has an `authorize` check.
- [ ] `origin` is set for cookie-authenticated deployments.
- [ ] `hub.disconnect(connId)` is wired into logout and role changes. `ctx` is
      captured at connect time, so a stream outlives a revoked session until you
      close it.
- [ ] Prefer one database file per tenant. Then a scoping bug is a physical
      boundary, not a query bug.

## Project layout

```
packages/core     outbox, triggers, tailer, retention, deps, write + drizzle helpers
packages/server   hub, subscriptions, SSE, tickets, node adapter
packages/react    LiveClient + useLiveQuery
examples/demo     runnable end-to-end app
```

## Development

```bash
pnpm test        # 28 tests: triggers, tailer, retention, HTTP/SSE, backpressure, chaos
pnpm typecheck
pnpm demo:build && pnpm demo
```

All tests run against real SQLite files in WAL mode. `chaos.test.ts` drives
concurrent writers, aggressive truncation and reconnects, then asserts no
subscriber was left behind. See `SPEC.md` for the invariants.
