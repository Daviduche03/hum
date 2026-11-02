import type { LiveDb, PreparedStatement } from "./types.ts";

/**
 * Wrap a database so that every write flushes the live tailer immediately.
 *
 * Without this, in-process writes wait for the poll (250ms by default). The
 * outbox is still the source of truth — `notify` is only a latency hint, so
 * losing a notification is harmless, and calling it too often costs a cheap
 * `MAX(seq)` query.
 *
 * ```ts
 * const hub = new LiveHub({ db, queries });
 * const live = onWrite(db, () => hub.flush()); // pass `live` to your app code
 * ```
 *
 * Every statement goes through the same underlying connection; the proxy only
 * calls `notify` after `run()` and `exec()` succeed. Reads are untouched.
 */
export function onWrite<T extends LiveDb>(db: T, notify: () => void): T {
  return new Proxy(db, {
    get(target, property) {
      if (property === "prepare") {
        return (sql: string) => wrapStatement(target.prepare(sql), notify);
      }
      if (property === "exec") {
        return (sql: string) => {
          const result = target.exec(sql);
          notify();
          return result;
        };
      }
      return readMember(target, property);
    },
  }) as T;
}

function wrapStatement(
  statement: PreparedStatement,
  notify: () => void,
): PreparedStatement {
  return new Proxy(statement, {
    get(target, property) {
      if (property === "run") {
        return (...params: unknown[]) => {
          const result = target.run(...params);
          notify();
          return result;
        };
      }
      return readMember(target, property);
    },
  });
}

/** Read a member with `this` bound to the real object (class private fields). */
function readMember(target: object, property: string | symbol): unknown {
  const value = Reflect.get(target, property, target);
  return typeof value === "function" ? value.bind(target) : value;
}
