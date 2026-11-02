import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

const local = (name) => fileURLToPath(new URL(name, import.meta.url));

const VIEWERS = Number(process.env.VIEWERS ?? 30);
const SEQUENTIAL = Number(process.env.SEQUENTIAL ?? 20);
const PACED = Number(process.env.PACED ?? 30);
const PACED_MS = Number(process.env.PACED_MS ?? 100);
const BURST = Number(process.env.BURST ?? 500);
const PORT = Number(process.env.PORT ?? 4479);
const BASE = `http://127.0.0.1:${PORT}`;
const ROOM = 1;

const dir = mkdtempSync(join(tmpdir(), "hum-chat-bench-"));
let serverLog = "";

const child = spawn(process.execPath, [local("./server.mjs")], {
  env: { ...process.env, PORT: String(PORT), CHAT_DB: join(dir, "bench.db") },
  stdio: ["ignore", "pipe", "pipe"],
});
child.stdout.on("data", (chunk) => (serverLog += chunk));
child.stderr.on("data", (chunk) => (serverLog += chunk));

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const tick = () => new Promise((resolve) => setImmediate(resolve));

async function waitFor(predicate, label, timeout = 15_000) {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeout) throw new Error(`timeout: ${label}`);
    await tick();
  }
  return Date.now() - start;
}

function percentile(values, p) {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[index];
}

const ms = (value) => `${value.toFixed(1)}ms`;
const rate = (count, elapsed) => `${Math.round((count / elapsed) * 1000)}/s`;

const viewers = [];

function cleanup(code = 0) {
  for (const viewer of viewers) viewer.controller.abort();
  child.kill("SIGTERM");
  rmSync(dir, { recursive: true, force: true });
  process.exit(code);
}

process.on("SIGINT", () => cleanup(130));

/**
 * Mirrors LiveClient.refetchEntry: a fetch in flight absorbs every frame that
 * arrives meanwhile into one follow-up fetch. Fetching once per frame instead
 * would overstate server load and understate latency.
 */
function attachRefetch(viewer) {
  let fetching = false;
  let dirty = false;

  viewer.refetches = 0;
  viewer.refetchMs = 0;
  viewer.maxSeq = 0;
  viewer.frames = 0;

  viewer.refetch = () => {
    if (fetching) {
      dirty = true;
      return;
    }
    fetching = true;
    void (async () => {
      do {
        dirty = false;
        const started = performance.now();
        try {
          await fetch(`${BASE}/api/messages?room_id=${ROOM}`, {
            headers: { cookie: viewer.cookie },
          }).then((response) => response.arrayBuffer());
        } catch {
          // ignore
        }
        viewer.refetchMs += performance.now() - started;
        viewer.refetches += 1;
      } while (dirty);
      fetching = false;
    })();
  };
}

async function connectViewer(index) {
  const cookie = `uid=bench-${String(index).padStart(2, "0")}`;
  const controller = new AbortController();

  const response = await fetch(`${BASE}/live/connect`, {
    method: "POST",
    headers: { cookie },
  });
  if (!response.ok) throw new Error(`connect ${response.status}`);
  const { connId, ticket } = await response.json();

  return { cookie, connId, ticket, controller };
}

async function streamViewer(viewer) {
  const stream = await fetch(`${BASE}/live/stream?ticket=${viewer.ticket}`, {
    signal: viewer.controller.signal,
  });

  attachRefetch(viewer);

  void (async () => {
    const reader = stream.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        buffer += decoder.decode(value, { stream: true });

        let boundary = buffer.indexOf("\n\n");
        while (boundary >= 0) {
          const block = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);

          const event = block
            .split("\n")
            .find((line) => line.startsWith("event: "))
            ?.slice(7);
          const data = block
            .split("\n")
            .find((line) => line.startsWith("data: "))
            ?.slice(6);

          if (event === "change" || event === "resync") {
            const seq = Number(JSON.parse(data ?? "null")?.seq ?? 0);
            if (seq > viewer.maxSeq) viewer.maxSeq = seq;
            viewer.frames += 1;
            viewer.refetch();
          }

          boundary = buffer.indexOf("\n\n");
        }
      }
    } catch {
      // aborted
    }
  })();
}

async function subscribeViewer(viewer) {
  const response = await fetch(`${BASE}/live/sub`, {
    method: "POST",
    headers: { "content-type": "application/json", cookie: viewer.cookie },
    body: JSON.stringify({
      connId: viewer.connId,
      id: "q1",
      name: "messages.latest",
      params: { room_id: ROOM },
    }),
  });
  if (!response.ok) throw new Error(`sub ${response.status}`);
}

async function post(cookie, body) {
  const response = await fetch(`${BASE}/api/messages`, {
    method: "POST",
    headers: { cookie, "content-type": "application/json" },
    body: JSON.stringify({ room_id: ROOM, body }),
  });
  if (!response.ok) throw new Error(`post ${response.status}`);
  return response.json();
}

async function converge(seq) {
  await waitFor(() => viewers.every((viewer) => viewer.maxSeq >= seq), `seq ${seq}`);
}

try {
  await waitFor(async () => {
    try {
      return (await fetch(`${BASE}/live/health`)).ok;
    } catch {
      return false;
    }
  }, "server up");

  // Two phases: create every identity first, then subscribe. Otherwise the
  // user-insert triggers invalidate subscriptions that already exist and the
  // measurement picks up setup noise.
  for (let index = 0; index < VIEWERS; index += 1) {
    viewers.push(await connectViewer(index));
  }
  for (const viewer of viewers) {
    await subscribeViewer(viewer);
    await streamViewer(viewer);
  }

  await sleep(200);

  const writer = viewers[0].cookie;

  // Warm the JIT and the connection before measuring.
  for (let i = 0; i < 5; i += 1) {
    const { seq } = await post(writer, `warmup ${i}`);
    await converge(seq);
  }

  // -------------------------------------------------- phase A: sequential

  const refetchBeforeA = viewers.reduce((sum, viewer) => sum + viewer.refetches, 0);
  const latencies = [];
  for (let i = 1; i <= SEQUENTIAL; i += 1) {
    const started = performance.now();
    const { seq } = await post(writer, `sequential message ${i}`);
    await converge(seq);
    latencies.push(performance.now() - started);
  }

  const refetchA = viewers.reduce((sum, viewer) => sum + viewer.refetches, 0) - refetchBeforeA;
  const refetchMsA = viewers.reduce((sum, viewer) => sum + viewer.refetchMs, 0);

  // ------------------------------------------------ phase B: human pace

  const pacedLatencies = [];
  const pacedStart = performance.now();
  for (let i = 1; i <= PACED; i += 1) {
    const started = performance.now();
    const { seq } = await post(writer, `paced message ${i}`);
    await converge(seq);
    pacedLatencies.push(performance.now() - started);
    await sleep(PACED_MS);
  }
  const pacedElapsed = performance.now() - pacedStart;
  const refetchB = viewers.reduce((sum, viewer) => sum + viewer.refetches, 0) - refetchA;

  // -------------------------------------------------------- phase C: burst

  const framesBefore = viewers.map((viewer) => viewer.frames);
  const refetchBefore = viewers.reduce((sum, viewer) => sum + viewer.refetches, 0);

  const burstStart = performance.now();
  let lastSeq = 0;
  for (let i = 0; i < BURST; i += 20) {
    await Promise.all(
      Array.from({ length: Math.min(20, BURST - i) }, (_, j) =>
        post(writer, `burst message ${i + j}`).then((body) => {
          lastSeq = Math.max(lastSeq, body.seq);
        }),
      ),
    );
  }
  const posted = performance.now() - burstStart;

  await converge(lastSeq);
  const converged = performance.now() - burstStart;

  const framesC = viewers.map((viewer, index) => viewer.frames - framesBefore[index]);
  const framesTotal = framesC.reduce((sum, value) => sum + value, 0);
  const refetchC = viewers.reduce((sum, viewer) => sum + viewer.refetches, 0) - refetchBefore;

  // ------------------------------------ phase D: another process, no flush

  const framesBeforeD = viewers.map((viewer) => viewer.frames);
  const refetchBeforeD = viewers.reduce((sum, viewer) => sum + viewer.refetches, 0);

  const external = new DatabaseSync(join(dir, "bench.db"));
  external.exec("PRAGMA busy_timeout = 3000");
  const insert = external.prepare(
    "INSERT INTO messages (room_id, author_id, body) VALUES (?, ?, ?)",
  );

  const externalStart = performance.now();
  external.exec("BEGIN");
  for (let i = 0; i < BURST; i += 1) insert.run(ROOM, 1, `external ${i}`);
  external.exec("COMMIT");
  const externalWritten = performance.now() - externalStart;

  const externalSeq = Number(
    external.prepare("SELECT MAX(seq) AS seq FROM _live_changes").get().seq,
  );
  await converge(externalSeq);
  const externalConverged = performance.now() - externalStart;

  const framesD = viewers.map((viewer, index) => viewer.frames - framesBeforeD[index]);
  const framesTotalD = framesD.reduce((sum, value) => sum + value, 0);
  const refetchD =
    viewers.reduce((sum, viewer) => sum + viewer.refetches, 0) - refetchBeforeD;

  external.close();

  const health = await (await fetch(`${BASE}/live/health`)).json();

  // ------------------------------------------------------------- reporting

  console.log(`\nhum chat benchmark`);
  console.log(`  ${VIEWERS} viewers · room #general · 1 writer\n`);

  console.log(`Phase A — one message at a time (${SEQUENTIAL})`);
  console.log(
    `  post→all viewers  p50 ${ms(percentile(latencies, 50))}  p95 ${ms(
      percentile(latencies, 95),
    )}  max ${ms(Math.max(...latencies))}`,
  );
  console.log(
    `  refetches         ${refetchA} (${(refetchA / SEQUENTIAL / VIEWERS).toFixed(
      2,
    )} per viewer per message)`,
  );

  console.log(`\nPhase B — human pace (${PACED} messages, one every ${PACED_MS}ms)`);
  console.log(
    `  post→all viewers  p50 ${ms(percentile(pacedLatencies, 50))}  p95 ${ms(
      percentile(pacedLatencies, 95),
    )}  max ${ms(Math.max(...pacedLatencies))}`,
  );
  console.log(
    `  refetches         ${refetchB} over ${ms(pacedElapsed)} (${rate(
      refetchB,
      pacedElapsed,
    )})`,
  );

  console.log(`\nPhase C — burst (${BURST} as fast as possible)`);
  console.log(`  posting           ${ms(posted)} → ${rate(BURST, posted)}`);
  console.log(`  converged         ${ms(converged)} after the first post`);
  console.log(
    `  frames            ${(framesTotal / VIEWERS).toFixed(0)}/viewer → ${(
      BURST /
      (framesTotal / VIEWERS)
    ).toFixed(1)} messages per frame`,
  );
  console.log(
    `  refetches         ${refetchC} (${(refetchC / VIEWERS).toFixed(0)}/viewer, ${(
      refetchC / BURST
    ).toFixed(2)} per message)`,
  );

  console.log(`\nPhase D — ${BURST} rows from another process (no flush, poll only)`);
  console.log(`  writing           ${ms(externalWritten)}`);
  console.log(`  converged         ${ms(externalConverged)} after the first write`);
  console.log(
    `  frames            ${(framesTotalD / VIEWERS).toFixed(1)}/viewer → ${(
      BURST /
      (framesTotalD / VIEWERS)
    ).toFixed(0)} messages per frame`,
  );
  console.log(`  refetches         ${refetchD}`);

  console.log(`\nserver`);
  console.log(
    `  ${health.connections} connections · ${health.subscriptions} subscriptions · tables ${health.tables.join(", ")}`,
  );
} catch (error) {
  console.error(`\nbench failed: ${error.message}`);
  if (serverLog.trim()) console.error(serverLog);
  cleanup(1);
}

cleanup(0);
