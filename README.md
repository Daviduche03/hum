# hum

**Realtime SQLite for React, without adopting a framework.**

Any write to your SQLite file — Drizzle, raw SQL, a migration, `sqlite3` in a
terminal — makes the React components reading those rows refetch. Nothing is
replicated, no message carries your data, and your auth stays the only auth.

```
write → trigger → _live_changes → tailer → SSE → useLiveQuery → your fetcher()
```

The server sends one thing: *"this query is stale."* Your loader does the rest.

- **Any TS backend** that can serve `(Request) => Response` — Node, Hono, Next,
  Bun, Deno.
- **Any SQLite driver** shaped like `better-sqlite3` — `node:sqlite`,
  `bun:sqlite`, `better-sqlite3`.
- **No client replica, no codegen, no schema changes** beyond one outbox table.

## Where it fits

|  | hum | `setInterval(refetch, 5s)` | sync engines |
|---|---|---|---|
| Update latency | milliseconds | up to the interval | milliseconds |
| Data over the wire | none — invalidation only | none | rows / ops |
| Second auth surface | no | no | yes |
| Client replica | no | no | yes |
| Offline writes, conflicts | no | no | yes |
| Backend | any | any | specific |

It is not a sync engine. It is the honest version of polling: the database
tells the browser *when* to refetch, instead of the browser asking forever.

## Quick start

> Not on npm yet. This repo is the source of truth; the packages are
> `@hum/core`, `@hum/server`, `@hum/react` in a pnpm workspace.

### 1. Capture

Call once at boot and after migrations. Idempotent, and a no-op when the
trigger DDL already matches.

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

### 2. Register what clients may subscribe to

Clients send a `name` and `params`. Never SQL. A query is a name, an optional
param parser, and the tables it reads.

```ts
import { LiveHub, TicketStore, createLiveHandler } from "@hum/server";

const queries = {
  "users.all": {
    tables: ["users"],
  },
  "users.byOrg": {
    params: (raw) => {
      const orgId = Number(raw?.org_id);
      if (!Number.isInteger(orgId)) throw new Error("org_id");
      return { org_id: orgId };
    },
    tables: ["users"],          // a write to `users` invalidates this query
    authorize: (ctx) => ctx.userId != null,
  },
};

const hub = new LiveHub({
  db,
  queries,
  scope: (ctx) => ctx.orgId,    // tenant bucket for subscription dedupe
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

**Node / Express** (Express `req`/`res` are compatible):

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

**Bun / Deno** — call `live(request)` from the fetch handler.

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

Components with the same `name` + `params` share one subscription and one
in-flight fetch. Unmounting the last one unsubscribes.

## Write path

The outbox is polled at 250ms, which covers writes from *other* processes — a
CLI, a migration, a cron job. Writes in the same process can skip the wait:

```ts
const hub = new LiveHub({ db, queries });
hub.start();

// Wrap the handle your app already uses. `run()` and `exec()` flush the
// tailer immediately; reads are untouched.
const live = onWrite(db, () => hub.flush());

live.prepare("insert into users (name, org_id) values (?, ?)").run(name, orgId);
```

In-process latency becomes whatever your query costs — typically a few
milliseconds end to end, not 250ms. Forgetting to flush is a latency bug, not a
correctness one: the poll still finds the change.

## Query definitions

A query is a name, an optional param parser, and the tables it reads — that's
the whole shape.

```ts
interface QueryDef<P = any> {
  params?: (raw: unknown) => P;                            // throw to reject (400)
  tables: string[] | ((ctx: Ctx, params: P) => string[]);  // required
  authorize?: (ctx: Ctx, params: P) => boolean | Promise<boolean>;
}
```

A write to any listed table invalidates every subscription to that query. There
is no SQL in the registry, no parser, and no inference: the tables are yours to
declare, and `LiveHub` throws at construction if one is missing. Use the
function form when dependencies vary by request:

```ts
tables: (ctx, params) => ["messages", params.with_authors ? "users" : null].filter(Boolean)
```

The trade-off is explicit: forget a table and that query will not wake up.
There is nothing at runtime that can catch it — but boot fails loudly, and the
list is three lines from the SQL it describes.

## Semantics

- **Invalidation, not replication.** A frame is `{ seq, ids }`. Nothing else
  crosses the wire, so a client cannot learn rows it is not allowed to read.
- **Whole-result refetch.** A change to `users` refetches every subscribed
  `users` query. Simple and correct; bandwidth is the trade-off.
- **Coalesced.** The tailer folds a burst into one batch of table names, and the
  client's in-flight dedupe turns a burst of frames into one follow-up fetch.
- **At-least-once, idempotent.** Duplicates are harmless; `seq` guards ordering.
- **A dropped frame always ends in a resync.** Under backpressure the server
  discards change frames and queues exactly one `resync` for the moment the
  consumer catches up. A slow tab can be late, never silently stale.
- **Truncation forces a resync.** Retention records `truncated_through`; a
  cursor below it is told to start over instead of silently skipping.

## Examples

Both examples run from this repo:

```bash
pnpm install

pnpm demo:build && pnpm demo      # http://localhost:4477
pnpm chat:build && pnpm chat      # http://localhost:4478
```

`examples/demo` is the smallest end-to-end app: one table, two panels, a
composer. Write from a terminal and watch both panels update:

```bash
sqlite3 examples/demo/demo.db \
  "insert into users (name, org_id) values ('from the CLI', 1)"
```

`examples/chat` is the stress case — many writers, read receipts, and history
pagination. Open `http://localhost:4478/?as=grace` in a second window to be
someone else.

It also ships a benchmark: 30 SSE viewers, one room, four phases.

```bash
pnpm chat:bench
```

```
Phase A  one message at a time     post→all viewers  p50 ~7ms   p95 ~10ms
Phase B  10 messages/sec           post→all viewers  p50 ~5ms   p95 ~12ms
Phase C  500 messages, same proc   ~840 posts/s, converged in ~600ms
Phase D  500 rows, another process 1 frame per viewer, converged in 100–250ms
```

Two things fall out of it:

- **In-process writes buy latency at the cost of coalescing.** `onWrite` flushes
  per request, so 500 posts produce 500 frames per viewer — but the client's
  in-flight dedupe cut 15,000 potential refetches down to ~770.
- **Out-of-process writes coalesce for free.** The same 500 rows written by
  another connection arrive as **one** frame.

## API surface

### `@hum/core`

| Export | Purpose |
|---|---|
| `syncLive({ db, include?, exclude?, capture?, outbox?, meta? })` | Create outbox + triggers. Idempotent. |
| `checkTriggers(db, { include?, exclude? })` | `{ missing, orphaned }` for boot-time drift detection. |
| `Tailer` | Cursor over `_live_changes`; emits coalesced `{ from, to, tables, count }`. |
| `compact({ db, minCursor?, keep? })` | Prune the outbox and record `truncated_through`. |
| `onWrite(db, notify)` | Proxy that calls `notify` after `run()`/`exec()`. |
| `currentSeq(db)`, `stableStringify(value)` | Small helpers. |

`capture` controls what a trigger writes: `"row"` (default) stores the primary
key of each affected row, `"table"` stores `NULL`. Invalidation is table-level
either way, so `"table"` is cheaper on bulk writes; `"row"` keeps data for
future row patches.

### `@hum/server`

| Export | Purpose |
|---|---|
| `LiveHub({ db, queries, scope?, identity?, pollMs?, keep?, connectionTtlMs? })` | Registry, fan-out, tailer, retention. |
| `createLiveHandler(hub, tickets, { authenticate, origin?, bodyLimit? })` | `(Request) => Response` router. |
| `toNodeHandler(handler)` | Node / Express adapter. |
| `TicketStore`, `TicketStoreLike` | Single-use stream tickets (30s). Implement the interface to share them. |

### `@hum/react`

| Export | Purpose |
|---|---|
| `LiveClient({ basePath?, fetch?, EventSource?, retry? })` | One SSE connection per tab, refcounted subscriptions. |
| `useLiveQuery({ client, name, params, fetcher })` | `{ data, error, loading, seq, updatedAt, refetch }`. |

## Limitations

- **Table granularity.** A change to any row of `users` refetches every
  subscribed `users` query. Precision is deliberately unsolved; row patches are
  future work.
- **One process per database** for `/live` (see Deployment).
- **No payloads.** No presence, typing indicators, or optimistic echoes. Add a
  WebSocket beside it if you need those.
- **Not serverless.** SSE needs a long-lived connection; there is no polling
  adapter for D1 or Lambda.
- **SQLite's single writer.** Concurrent writes serialize at the database, so
  set `busy_timeout` on every connection that writes.

## Deployment

`/live` must be served by **one process per database** — or by replicas that
share state. The outbox is cross-process by design, so any number of processes
can *write*; what is not shared is the connection registry and `TicketStore`.

If you must run more than one instance, either pin `/live` to one instance
(shuffle sharding, sticky routes, a single live service) or supply a
`TicketStoreLike` backed by shared storage *and* a cross-process transport for
fan-out. Sticky sessions alone are not enough: the tailer that sees a write has
to reach every connection.

## Security checklist

- [ ] `authenticate` verifies your real session; tickets are only a handshake.
- [ ] Identity is in `scope` if one SQLite file holds multiple tenants.
- [ ] Every query either scopes by `ctx` or has an `authorize` check.
- [ ] `origin` is set for cookie-authenticated deployments.
- [ ] `hub.disconnect(connId)` is wired into logout and role changes — `ctx` is
      captured at connect time, so a stream outlives a revoked session.
- [ ] Prefer one database file per tenant: a scoping bug becomes a physical
      boundary instead of a query bug.

## Project layout

```
packages/core     outbox, triggers, tailer, retention, write helper
packages/server   hub, subscriptions, SSE, tickets, node adapter
packages/react    LiveClient + useLiveQuery
examples/demo     smallest end-to-end app
examples/chat     group chat + benchmark
SPEC.md           invariants and the design of the pipeline
```

## Development

```bash
pnpm test        # 30 tests: triggers, tailer, retention, HTTP/SSE, backpressure, chaos
pnpm typecheck
pnpm demo:build && pnpm demo
pnpm chat:build && pnpm chat
pnpm chat:bench
```

All tests run against real SQLite files in WAL mode. `chaos.test.ts` drives
concurrent writers, aggressive truncation and a mid-stream reconnect, then
asserts no subscriber was left behind.

Requires Node 24+ (`node:sqlite` and built-in type stripping).

## License

MIT
