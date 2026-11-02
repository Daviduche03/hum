# hum — design spec

A drop-in realtime layer for SQLite. Any write, from any process, makes the
React components that read those rows refetch. No new framework, no new
database, no client-side replica.

## Non-goals (v1)

- Not a sync engine. No offline writes, no conflict resolution, no CRDTs.
- Not a new backend. You keep Hono / Express / Next / Bun / Deno.
- Not an ORM. Drizzle keeps compiling the queries; we watch underneath it.
- Not a data API. The live endpoint never returns rows. It returns "this query
  is stale"; your existing loader fetches the data.

## The pipeline

```
write (Drizzle / raw SQL / sqlite3 CLI / another process)
  → AFTER INSERT/UPDATE/DELETE trigger
  → _live_changes (seq, tbl, op, pk, ts)
  → tailer (per-process cursor over seq)
  → hub (coalesce tables → subscriptions → connections)
  → SSE frame { seq, ids: [...] }
  → LiveClient (per-tab EventSource)
  → your fetcher()
```

## Invariants

1. **seq is monotonic and never reused.** The outbox uses `AUTOINCREMENT`.
   Ordering never depends on wall-clock time.
2. **The outbox insert rides the write's transaction.** A rolled-back write
   leaves no change row. A trigger failure fails the write.
3. **The tailer reads from its own connection.** In WAL mode it can never see
   uncommitted change rows, which removes the entire "broadcast before COMMIT"
   class of bugs.
4. **Subscribe before fetch.** `POST /live/sub` returns the current `seq`; the
   client fetches after that. Any change at or after that seq is delivered, so
   a fetch can be redundant but never missed.
5. **Notifications are idempotent.** A message means "this query is dirty", so
   duplicates, coalescing, and reordering are all safe. Refetching is
   idempotent, so delivery is effectively at-least-once.
6. **Truncation forces a resync.** Retention records `truncated_through`. A
   cursor below it must not silently continue; it receives `resync`.
7. **Default deny.** Client sends `{ name, params }`. SQL is server-owned. An
   unknown query name and an unauthorized query produce the same response.
8. **A dropped frame always ends in a resync.** Under backpressure the server
   discards change frames and retains exactly one pending `resync`. It is
   written at the first opportunity — from the stream's `pull()`, the next
   send, or the heartbeat — so a quiet stream cannot swallow it.
9. **Capture is table-level.** The outbox carries `op` and `pk` for debugging
   and future row patches, but the pipeline only ever uses `tbl`. A trigger's
   per-row work is therefore optional: `capture: "table"` writes `NULL` and
   leaves bulk writes cheap. `ts` is wall-clock and diagnostic only; ordering
   never uses it.

## Process model

The outbox is the bus for *changes*: every process can tail it independently,
and writes from any process are seen by every reader. Connections are not
shared: `LiveHub.connections` and `TicketStore` are in-memory.

Consequences:

- `/live` must be served by one process per database, or by replicas that share
  a ticket store and a fan-out transport.
- A ticket minted by one process cannot be redeemed by another.
- Fan-out reaches only the connections held by the process that tailed the row.
- A connection created by `POST /connect` and never streamed is reaped after
  `connectionTtlMs` (default 30s).

## Write path

`pollMs` (default 250ms) exists for out-of-process writers — a CLI, a
migration, another service. In-process writes avoid the poll entirely:

```
write → trigger → outbox → hub.flush() → tailer.poll() → fan-out
```

`flush()` is a latency hint, not a correctness requirement; losing a
notification costs one poll interval, and the outbox still holds the change.
`onWrite(db, () => hub.flush())` wires `run()`/`exec()` to it.

## Subscription model

```
key = scope(ctx) + "\0" + name + "\0" + stableStringify(params)
```

- `scope` is your tenant/user bucket. Without it, two identities in a shared DB
  share one subscription entry (they still never receive data, only timing).
- Identical keys are refcounted: twenty hooks sharing one query produce one
  server subscription, one posture, and one client fetch.
- `byTable` is a reverse index (`table → subscriptions`) so fan-out is
  proportional to affected subscriptions, not to total subscriptions.

### Dependencies

Declared, not inferred. `tables` is the whole dependency model:

```ts
"users.byOrg": {
  params: (raw) => ({ org_id: Number(raw.org_id) }),
  tables: ["users"],
}
```

A write to any listed table invalidates every subscription to that query. A
function is evaluated per subscription, so dependencies can depend on params or
identity:

```ts
tables: (ctx, params) => ["messages", params.include_authors && "users"].filter(Boolean)
```

There is no SQL in the registry and nothing to keep in sync with a query
planner. The trade-off is explicit: forget a table and the query will not wake
up. `LiveHub` refuses a query with no `tables` at construction, so the missing
field fails at boot; a wrong list is yours to catch.

## Transport

- **SSE** for changes. `text/event-stream`, resumable semantics ignored because
  subscriptions live on the connection, not on a stream id. The stream is
  created with a 64-message high-water mark, so a normal burst does not look
  like a stalled consumer.
- **POST** for `connect`, `sub`, `unsub`, `close`. One batched POST for all
  hooks on a page.
- **Ticket handshake.** `EventSource` cannot send headers, so `POST
  /live/connect` mints a single-use ticket bound to one connection; the stream
  URL carries the ticket, never the session.
- Heartbeat comment every 25s. `X-Accel-Buffering: no`.
- Backpressure: if the consumer stops reading, payloads are dropped and a
  single `resync` is retained until the queue drains. Dirty notifications make
  dropping safe; retaining the resync is what makes a *quiet* stream safe.

## Retention

```
target = min(oldest attached cursor - 1, maxSeq - keep)
delete seq <= target in chunks
set _live_meta.truncated_through = target
```

- `keep` defaults to 1000 rows of history for debugging.
- Runs every 5s in the hub, or on demand via `compact()`.
- Deletion is a correctness boundary: anything below `truncated_through` must
  resync.

## Security model

| Layer | Mechanism |
|---|---|
| Stream auth | single-use 30s ticket → connection |
| POST auth | `authenticate(request)` on every call |
| Connection ownership | `identity(ctx)` must match the connection's identity |
| CSRF | `Origin` allowlist when the header is present |
| Query surface | default-deny registry, server-owned SQL |
| Params | per-query parser; identity filters come from `ctx`, not params |
| Dedupe | auth scope is part of the subscription key |
| Data | never in messages; refetch goes through your normal loader |

**Not implemented yet:** continuous re-validation. The connection captures
`ctx` at connect time, so a stream outlives a revoked session. This is a
required integration point, not a nicety: call `hub.disconnect(connId)` from
your logout and role-change paths. A session epoch checked on heartbeat is the
next step.

## Failure modes

| Failure | Behaviour |
|---|---|
| Trigger dropped by `drizzle-kit push` | `syncLive()` recreates at boot; `checkTriggers()` reports drift |
| Tailer crash / restart | starts at `MAX(seq)`; clients that were behind get `resync` |
| Client misses frames | gap detection on `batch.from` → `resync` |
| Outbox truncated under a client | `truncated_through` → `resync` |
| Slow consumer | drops frames, retains one pending `resync`, writes it when the queue drains |
| Process restarts | subscriptions are per-connection; the client resubscribes |
| Connect without a stream | reaped after `connectionTtlMs` (default 30s) |

## Open decisions

1. **Row patches** — the outbox already carries `pk` when `capture: "row"`.
   Would need `row` JSON and delete handling. Deferred; `capture: "table"`
   exists precisely because nothing reads `pk` today.
2. **Precise filter matching** — deliberately not attempted. Pessimistic
   invalidation is correct; precision is an optimization.
3. **WebSocket** — `live.upgrade()` is the intended shape for client acks and
   mid-stream subscription changes.
4. **Serverless / D1** — no long-lived connections without Durable Objects;
   needs a polling adapter.
5. **Client replica** — different product (LiveStore / ElectricSQL territory).
