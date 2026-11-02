import { useEffect, useRef, useState } from "react";
import type { LiveClient, LiveEntry, LiveSnapshot } from "./client.ts";

export interface UseLiveQueryOptions<T> {
  client: LiveClient;
  /** Name of a query registered on the server. Never SQL. */
  name: string;
  params?: unknown;
  /** Your normal data loader. Called on mount and whenever the query changes. */
  fetcher: () => Promise<T>;
}

export type UseLiveQueryResult<T> = LiveSnapshot<T> & {
  refetch: () => void;
};

/**
 * Subscribe a component to a server-owned query.
 *
 * The value is fetched by `fetcher` and refetched when the server reports that
 * one of the query's tables changed. Components asking for the same name +
 * params share one subscription and one in-flight fetch.
 */
export function useLiveQuery<T>(
  options: UseLiveQueryOptions<T>,
): UseLiveQueryResult<T> {
  const { client, name } = options;
  const key = client.keyFor(name, options.params);

  const fetcherRef = useRef(options.fetcher);
  fetcherRef.current = options.fetcher;
  const paramsRef = useRef(options.params);
  paramsRef.current = options.params;

  const [snapshot, setSnapshot] = useState<LiveSnapshot<T>>(() => ({
    data: undefined,
    error: undefined,
    loading: true,
    seq: 0,
    updatedAt: undefined,
  }));

  const entryRef = useRef<LiveEntry<T> | null>(null);

  useEffect(() => {
    const entry = client.entry<T>(name, paramsRef.current, () =>
      fetcherRef.current(),
    );
    entryRef.current = entry;

    const listener = () => setSnapshot({ ...entry.snapshot });
    entry.listeners.add(listener);
    listener();

    return () => {
      entry.listeners.delete(listener);
      entryRef.current = null;
      client.release(entry);
    };
  }, [client, key, name]);

  return {
    ...snapshot,
    refetch: () => {
      const entry = entryRef.current;
      if (entry) void client.refetchEntry(entry);
    },
  };
}
