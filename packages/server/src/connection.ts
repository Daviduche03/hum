import type { Ctx } from "./types.ts";

const encoder = new TextEncoder();
const MAX_BUFFERED = 256;

/**
 * One browser tab (or one SDK client).
 *
 * A connection can exist before the SSE stream attaches, so it buffers a small
 * number of messages. If more than that pile up we mark it for resync instead
 * of growing without bound.
 *
 * Backpressure is the subtle part. When the consumer stops reading, payloads
 * are dropped — that is safe because a change frame only says "this query is
 * dirty". But the compensating `resync` must not be dropped along with them,
 * or a quiet stream leaves the client silently stale. So the resync is kept as
 * pending state and written at the first moment there is room: from `pull()`,
 * from the heartbeat, or at the start of the next send.
 */
export class Connection {
  readonly id: string;
  readonly ctx: Ctx;
  readonly identity: string;
  readonly createdAt: number;
  lastSeq: number;
  closed = false;

  private controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  private buffer: string[] = [];
  /** Number of change frames dropped since the last resync was delivered. */
  private dropped = 0;
  private resyncScheduled = false;
  private heartbeat: ReturnType<typeof setInterval> | null = null;

  constructor(id: string, ctx: Ctx, identity: string, lastSeq: number) {
    this.id = id;
    this.ctx = ctx;
    this.identity = identity;
    this.lastSeq = lastSeq;
    this.createdAt = Date.now();
  }

  attach(controller: ReadableStreamDefaultController<Uint8Array>): void {
    if (this.closed) {
      try {
        controller.close();
      } catch {
        // ignore
      }
      return;
    }
    this.controller = controller;
    // Write the handshake directly: going through `send()` could flush an owed
    // resync first and put it ahead of `hello`.
    this.writeRaw(this.frame("hello", { connId: this.id, seq: this.lastSeq }));

    for (const payload of this.buffer.splice(0)) {
      this.writeRaw(payload);
    }
    // A resync may have been requested while buffering, or dropped just now.
    this.flushResync();
  }

  send(event: string, data: unknown): void {
    if (this.closed) return;
    const payload = this.frame(event, data);

    if (!this.controller) {
      if (this.buffer.length >= MAX_BUFFERED) {
        this.drop();
        return;
      }
      this.buffer.push(payload);
      return;
    }

    if (this.full()) {
      this.drop();
      return;
    }

    // Deliver any owed resync before newer notifications.
    this.flushResync();
    this.writeRaw(payload);
  }

  private frame(event: string, data: unknown): string {
    return `id: ${this.lastSeq}\nevent: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  }

  ping(): void {
    this.flushResync();
    this.writeRaw(": ping\n\n");
  }

  /**
   * Write the owed resync when the consumer has room. Call this from the
   * stream's `pull()` callback: it is the exact moment the queue drained.
   */
  flushResync(): void {
    if (!this.controller || this.dropped === 0) return;
    if (this.full()) return;

    const dropped = this.dropped;
    this.dropped = 0;
    this.writeRaw(
      `event: resync\ndata: ${JSON.stringify({
        seq: this.lastSeq,
        reason: "backpressure",
        dropped,
      })}\n\n`,
    );
  }

  startHeartbeat(intervalMs: number): void {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = setInterval(() => this.ping(), intervalMs);
    (this.heartbeat as { unref?: () => void }).unref?.();
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = null;
    try {
      this.controller?.close();
    } catch {
      // already closed
    }
    this.controller = null;
    this.buffer = [];
    this.dropped = 0;
  }

  get attached(): boolean {
    return this.controller !== null;
  }

  /** True when the consumer is not keeping up. */
  private full(): boolean {
    const desired = this.controller?.desiredSize;
    return desired !== null && desired !== undefined && desired <= 0;
  }

  private drop(): void {
    this.dropped += 1;
    // One retry on the microtask queue covers a consumer that is merely a tick
    // behind. Coalesce the burst into a single attempt, then wait for `pull()`
    // or the heartbeat — never spin.
    if (!this.controller || this.resyncScheduled) return;
    this.resyncScheduled = true;
    queueMicrotask(() => {
      this.resyncScheduled = false;
      this.flushResync();
    });
  }

  private writeRaw(payload: string): void {
    if (!this.controller) return;
    try {
      this.controller.enqueue(encoder.encode(payload));
    } catch {
      this.close();
    }
  }
}
