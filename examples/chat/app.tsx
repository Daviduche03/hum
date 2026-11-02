import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { createRoot } from "react-dom/client";
import { LiveClient, useLiveQuery, type LiveStatus } from "@hum/react";

// One client per tab. Every hook below multiplexes over its single SSE stream.
const client = new LiveClient({ basePath: "/live" });

interface Me {
  userId: number;
  name: string;
}

interface User {
  id: number;
  name: string;
}

interface Room {
  id: number;
  name: string;
  unread: number;
  last_id: number | null;
}

interface Message {
  id: number;
  room_id: number;
  body: string;
  created_at: number;
  author_id: number;
  author: string;
}

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json() as Promise<T>;
}

async function postJson(url: string, body: unknown): Promise<unknown> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

function initials(name: string): string {
  return name.slice(0, 2).toUpperCase();
}

function clock(seconds: number): string {
  return new Date(seconds * 1000).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function StatusPill() {
  const [status, setStatus] = useState<LiveStatus>(client.getStatus());

  useEffect(() => client.onStatus(setStatus), []);

  return (
    <span className={`pill ${status === "live" ? "live" : ""}`}>
      <span className="dot" />
      {status}
    </span>
  );
}

function Thread({ room, me }: { room: Room; me: Me }) {
  const [older, setOlder] = useState<Message[]>([]);
  const [hasMore, setHasMore] = useState(true);
  const [text, setText] = useState("");
  const [sending, setSending] = useState(false);
  const listRef = useRef<HTMLDivElement>(null);
  const stick = useRef(true);

  // Older pages are local state. The live query replaces only the newest
  // window, so the two are merged by id below.
  useEffect(() => {
    setOlder([]);
    setHasMore(true);
  }, [room.id]);

  const live = useLiveQuery<{ rows: Message[] }>({
    client,
    name: "messages.latest",
    params: { room_id: room.id },
    fetcher: () => getJson(`/api/messages?room_id=${room.id}`),
  });

  const latest = live.data?.rows;

  const messages = useMemo(() => {
    const seen = new Map<number, Message>();
    for (const message of latest ?? []) seen.set(message.id, message);
    for (const message of older) {
      if (!seen.has(message.id)) seen.set(message.id, message);
    }
    return [...seen.values()].sort((a, b) => a.id - b.id);
  }, [latest, older]);

  const newest = latest?.[0]?.id;

  // Mark the room read up to the newest message we can see. This writes to
  // `reads`, which invalidates the room list and clears the badge.
  useEffect(() => {
    if (!newest) return;
    const timer = setTimeout(() => {
      void postJson("/api/read", {
        room_id: room.id,
        last_seen_id: newest,
      }).catch(() => {});
    }, 250);
    return () => clearTimeout(timer);
  }, [room.id, newest]);

  // Follow new messages only when the reader is already near the bottom.
  useEffect(() => {
    const element = listRef.current;
    if (!element || !stick.current) return;
    element.scrollTop = element.scrollHeight;
  }, [room.id, messages.length]);

  function onScroll() {
    const element = listRef.current;
    if (!element) return;
    stick.current = element.scrollHeight - element.scrollTop - element.clientHeight < 120;
  }

  async function loadOlder() {
    const oldest = messages[0]?.id;
    if (!oldest) return;
    const page = await getJson<{ rows: Message[] }>(
      `/api/messages?room_id=${room.id}&before=${oldest}`,
    );
    if (page.rows.length === 0) {
      setHasMore(false);
      return;
    }
    setOlder((previous) => {
      const seen = new Set(previous.map((message) => message.id));
      return [...previous, ...page.rows.filter((row) => !seen.has(row.id))];
    });
  }

  async function send(event: FormEvent) {
    event.preventDefault();
    const body = text.trim();
    if (!body || sending) return;
    setSending(true);
    setText("");
    try {
      await postJson("/api/messages", { room_id: room.id, body });
    } catch {
      setText(body);
    } finally {
      setSending(false);
    }
  }

  return (
    <section className="thread">
      <header className="thread-head">
        <div>
          <h2>#{room.name}</h2>
          <p className="hint">
            {live.loading
              ? "loading…"
              : `${messages.length} message${messages.length === 1 ? "" : "s"} in view`}
          </p>
        </div>
        <StatusPill />
      </header>

      <div className="scroll" ref={listRef} onScroll={onScroll}>
        {live.error && <p className="error">{live.error.message}</p>}
        {live.loading && messages.length === 0 && <p className="empty">Loading…</p>}
        {!live.loading && messages.length === 0 && (
          <p className="empty">No messages yet. Say something.</p>
        )}

        {messages.length > 0 && hasMore && (
          <button type="button" className="more" onClick={loadOlder}>
            Load older
          </button>
        )}

        {messages.map((message) => (
          <article
            key={message.id}
            className={`msg ${message.author_id === me.userId ? "mine" : ""}`}
          >
            <div className="avatar">{initials(message.author)}</div>
            <div>
              <div className="meta">
                <strong>{message.author}</strong>
                <time>{clock(message.created_at)}</time>
              </div>
              <div className="body">{message.body}</div>
            </div>
          </article>
        ))}
      </div>

      <form className="composer" onSubmit={send}>
        <input
          value={text}
          onChange={(event) => setText(event.target.value)}
          placeholder={`Message #${room.name}`}
          aria-label={`Message #${room.name}`}
          maxLength={2000}
        />
        <button type="submit" disabled={sending || !text.trim()}>
          Send
        </button>
      </form>

      <p className="foot">
        Posting writes one row to SQLite. Everyone in the room is told it changed
        and refetches — no message payloads cross the socket.
      </p>
    </section>
  );
}

function Sidebar({
  me,
  users,
  rooms,
  roomId,
  onSelect,
}: {
  me: Me;
  users: User[];
  rooms: Room[];
  roomId: number | null;
  onSelect: (id: number) => void;
}) {
  return (
    <aside className="sidebar">
      <div className="brand">
        <h1>hum chat</h1>
        <p>SQLite invalidation, in a room</p>
      </div>

      <ul className="rooms">
        {rooms.map((room) => (
          <li key={room.id}>
            <button
              type="button"
              aria-current={room.id === roomId}
              onClick={() => onSelect(room.id)}
            >
              <span>#{room.name}</span>
              {room.unread > 0 && room.id !== roomId && (
                <span className="badge">{room.unread > 99 ? "99+" : room.unread}</span>
              )}
            </button>
          </li>
        ))}
      </ul>

      <div className="who">
        <span>
          You are <strong>{me.name}</strong>
        </span>
        <select
          value={me.name}
          aria-label="Switch user"
          onChange={(event) => {
            window.location.href = `/?as=${encodeURIComponent(event.target.value)}`;
          }}
        >
          {users.map((user) => (
            <option key={user.id} value={user.name}>
              {user.name}
            </option>
          ))}
          {!users.some((user) => user.name === me.name) && (
            <option value={me.name}>{me.name}</option>
          )}
        </select>
      </div>
    </aside>
  );
}

function App({ me }: { me: Me }) {
  const [users, setUsers] = useState<User[]>([]);
  const [roomId, setRoomId] = useState<number | null>(null);

  useEffect(() => {
    void getJson<{ rows: User[] }>("/api/users").then((body) => setUsers(body.rows));
  }, []);

  const rooms = useLiveQuery<{ rows: Room[] }>({
    client,
    name: "rooms.forUser",
    params: {},
    fetcher: () => getJson("/api/rooms"),
  });

  const rows = rooms.data?.rows ?? [];
  const active = rows.find((room) => room.id === roomId) ?? rows[0];

  return (
    <div className="app">
      <Sidebar
        me={me}
        users={users}
        rooms={rows}
        roomId={active?.id ?? null}
        onSelect={setRoomId}
      />
      {active ? (
        <Thread key={active.id} room={active} me={me} />
      ) : (
        <section className="thread">
          <p className="empty">Loading rooms…</p>
        </section>
      )}
    </div>
  );
}

const me = JSON.parse(
  document.getElementById("me")!.textContent || "null",
) as Me;

createRoot(document.getElementById("root")!).render(<App me={me} />);
