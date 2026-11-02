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

## Drizzle

Drizzle emits plain SQL, so capture needs no integration. The adapter exists to
keep types and dependency inference convenient:

```ts
import { eq } from "drizzle-orm";
import { defineDrizzleQuery } from "@hum/drizzle";

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
| `syncLive({ db, include?, exclude?, outbox?, meta? })` | Create outbox + triggers. Idempotent. Returns synced tables. |
| `checkTriggers(db)` | `{ missing, orphaned }` for boot-time drift detection. |
| `Tailer` | Cursor over `_live_changes`; emits coalesced `{ from, to, tables, count }`. |
| `compact({ db, minCursor?, keep? })` | Prune the outbox and record `truncated_through`. |
| `tablesForSql(db, sql, params, known?)` | Planner-derived table dependencies. |
| `currentSeq(db)`, `stableStringify(value)` | Small helpers. |

### `@hum/server`

| Export | Purpose |
|---|---|
| `LiveHub({ db, queries, scope?, identity?, pollMs?, keep? })` | Registry, fan-out, tailer, retention. |
| `createLiveHandler(hub, tickets, { authenticate, origin? })` | `(Request) => Response` router. |
| `toNodeHandler(handler)` | Node/Express adapter. |
| `TicketStore` | Single-use stream tickets (30s default). |

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

## Security checklist

- [ ] `authenticate` verifies your real session; tickets are only a handshake.
- [ ] Identity is in `scope` if one SQLite file holds multiple tenants.
- [ ] Every query either scopes by `ctx` or has an `authorize` check.
- [ ] `origin` is set for cookie-authenticated deployments.
- [ ] Call `hub.disconnect(connId)` on logout/role change (epoch revalidation is
      not implemented yet).
- [ ] Prefer one database file per tenant. Then a scoping bug is a physical
      boundary, not a query bug.

## Project layout

```
packages/core     outbox, triggers, tailer, retention, deps
packages/server   hub, subscriptions, SSE, tickets, node adapter
packages/drizzle  defineDrizzleQuery / dependency helpers
packages/react    LiveClient + useLiveQuery
examples/demo     runnable end-to-end app
```

## Development

```bash
pnpm test        # 17 tests: triggers, tailer, retention, HTTP/SSE, client
pnpm typecheck
pnpm demo:build && pnpm demo
```

All tests run against real SQLite files in WAL mode. See `SPEC.md` for the
invariants and the deferred work.
