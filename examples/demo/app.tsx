import { useEffect, useRef, useState, type FormEvent } from "react";
import { createRoot } from "react-dom/client";
import { LiveClient, useLiveQuery, type LiveStatus } from "@livelite/react";

// One client per tab. Every hook below multiplexes over its single SSE stream.
const client = new LiveClient({ basePath: "/live" });

interface User {
  id: number;
  name: string;
  org_id: number;
  created_at: number;
}

interface Payload {
  rows: User[];
  at: number;
}

async function loadUsers(orgId?: number): Promise<Payload> {
  const query = orgId ? `?org_id=${orgId}` : "";
  const response = await fetch(`/api/users${query}`, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

function StatusPill() {
  const [status, setStatus] = useState<LiveStatus>(client.getStatus());

  useEffect(() => client.onStatus(setStatus), []);

  const label =
    status === "live"
      ? "live"
      : status === "connecting"
        ? "connecting"
        : status === "reconnecting"
          ? "reconnecting"
          : status;

  return (
    <span className={`pill ${status === "live" ? "live" : ""}`}>
      <span className="dot" />
      {label}
    </span>
  );
}

function UserList({
  title,
  orgId,
}: {
  title: string;
  orgId?: number;
}) {
  const live = useLiveQuery<Payload>({
    client,
    name: orgId ? "users.byOrg" : "users.all",
    params: orgId ? { org_id: orgId } : {},
    fetcher: () => loadUsers(orgId),
  });

  // Flash the panel when a live update lands.
  const [pulse, setPulse] = useState(false);
  const seen = useRef<number | undefined>(undefined);

  useEffect(() => {
    if (!live.updatedAt || live.updatedAt === seen.current) return;
    seen.current = live.updatedAt;
    setPulse(true);
    const timer = setTimeout(() => setPulse(false), 500);
    return () => clearTimeout(timer);
  }, [live.updatedAt]);

  const rows = live.data?.rows ?? [];

  return (
    <section className={`panel ${pulse ? "pulse" : ""}`}>
      <header>
        <h2>{title}</h2>
        <span className="meta">
          {live.loading ? "loading…" : `${rows.length} row${rows.length === 1 ? "" : "s"}`}
        </span>
      </header>

      {live.error && <p className="error">{live.error.message}</p>}
      {!live.error && rows.length === 0 && <p className="empty">No rows yet.</p>}

      <ul>
        {rows.map((user) => (
          <li key={user.id}>
            <span className="id">#{user.id}</span>
            <span className="name">{user.name}</span>
            <span className="org">org {user.org_id}</span>
          </li>
        ))}
      </ul>
    </section>
  );
}

function Composer() {
  const [name, setName] = useState("");
  const [orgId, setOrgId] = useState(1);
  const [busy, setBusy] = useState(false);

  async function add(event: FormEvent) {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed || busy) return;
    setBusy(true);
    try {
      await fetch("/api/seed", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: trimmed, org_id: orgId }),
      });
      setName("");
    } finally {
      setBusy(false);
    }
  }

  async function clear() {
    setBusy(true);
    try {
      await fetch("/api/reset", { method: "POST" });
    } finally {
      setBusy(false);
    }
  }

  return (
    <form className="composer" onSubmit={add}>
      <input
        value={name}
        onChange={(event) => setName(event.target.value)}
        placeholder="New user name"
        aria-label="New user name"
      />
      <select
        value={orgId}
        onChange={(event) => setOrgId(Number(event.target.value))}
        aria-label="Organisation"
      >
        <option value={1}>org 1</option>
        <option value={2}>org 2</option>
      </select>
      <button type="submit" disabled={busy || !name.trim()}>
        Insert row
      </button>
      <button type="button" className="ghost" onClick={clear} disabled={busy}>
        Clear
      </button>
    </form>
  );
}

function App() {
  return (
    <main className="wrap">
      <header className="top">
        <div>
          <h1>livelite</h1>
          <p className="sub">
            Any SQLite row change → a trigger → an outbox row → one SSE frame →
            the React component refetches.
          </p>
        </div>
        <StatusPill />
      </header>

      <Composer />

      <div className="grid">
        <UserList title="All users" />
        <UserList title="Org 2 only" orgId={2} />
      </div>

      <footer>
        Open a second tab, or write from outside the app:
        <code>
          sqlite3 examples/demo/demo.db "insert into users (name, org_id) values
          ('from the CLI', 1)"
        </code>
      </footer>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
