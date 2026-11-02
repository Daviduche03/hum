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

### Dependency discovery

`EXPLAIN QUERY PLAN` is the source of truth, because the planner already knows
which tables a query touches. Two wrinkles:

- The planner prints **aliases** (`SCAN u`), so FROM/JOIN clauses are scanned
  for real table names and filtered against `sqlite_master`.
- If nothing is inferred, we fall back to **all tables**. That is noisy but
  correct; a silent no-notification subscription is not.

`QueryDef.tables` overrides inference when you want precision (views, virtual
tables, CTEs that the heuristic misses).

## Transport

- **SSE** for changes. `text/event-stream`, resumable semantics ignored because
  subscriptions live on the connection, not on a stream id.
- **POST** for `connect`, `sub`, `unsub`, `close`. One batched POST for all
  hooks on a page.
- **Ticket handshake.** `EventSource` cannot send headers, so `POST
  /live/connect` mints a single-use ticket bound to one connection; the stream
  URL carries the ticket, never the session.
- Heartbeat comment every 25s. `X-Accel-Buffering: no`.
- Backpressure: if the consumer stops reading, payloads are dropped and a
  `resync` is sent when it recovers. Dirty notifications make this safe.

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
`ctx` at connect time. To revoke immediately, call `hub.disconnect(connId)`
from your logout/role-change path. A session epoch checked on heartbeat is the
next step.

## Failure modes

| Failure | Behaviour |
|---|---|
| Trigger dropped by `drizzle-kit push` | `syncLive()` recreates at boot; `checkTriggers()` reports drift |
| Tailer crash / restart | starts at `MAX(seq)`; clients that were behind get `resync` |
| Client misses frames | gap detection on `batch.from` → `resync` |
| Outbox truncated under a client | `truncated_through` → `resync` |
| Slow consumer | drops payloads → `resync` on recovery |
| Process restarts | subscriptions are per-connection; the client resubscribes |

## Open decisions

1. **Row patches** — outbox already carries `pk`; would need `row` JSON and
   delete handling. Deferred.
2. **Precise filter matching** — deliberately not attempted. Pessimistic
   invalidation is correct; precision is an optimization.
3. **WebSocket** — `live.upgrade()` is the intended shape for client acks and
   mid-stream subscription changes.
4. **Serverless / D1** — no long-lived connections without Durable Objects;
   needs a polling adapter.
5. **Client replica** — different product (LiveStore / ElectricSQL territory).
