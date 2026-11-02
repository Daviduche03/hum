import type { Ctx } from "./types.ts";

const encoder = new TextEncoder();
const MAX_BUFFERED = 256;

/**
 * One browser tab (or one SDK client).
 *
 * A connection can exist before the SSE stream attaches, so it buffers a small
 * number of messages. If more than that pile up we mark it for resync instead
 * of growing without bound.
 */
export class Connection {
  readonly id: string;
  readonly ctx: Ctx;
  readonly identity: string;
  lastSeq: number;
  closed = false;

  private controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  private buffer: string[] = [];
  private dropped = 0;
  private resyncPending = false;
  private heartbeat: ReturnType<typeof setInterval> | null = null;

  constructor(id: string, ctx: Ctx, identity: string, lastSeq: number) {
    this.id = id;
    this.ctx = ctx;
    this.identity = identity;
    this.lastSeq = lastSeq;
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
    this.send("hello", { connId: this.id, seq: this.lastSeq });

    for (const payload of this.buffer.splice(0)) {
      this.writeRaw(payload);
    }
    if (this.resyncPending) {
      this.resyncPending = false;
      this.send("resync", { seq: this.lastSeq, reason: "buffered" });
    }
  }

  send(event: string, data: unknown): void {
    if (this.closed) return;
    const payload = `id: ${this.lastSeq}\nevent: ${event}\ndata: ${JSON.stringify(
      data,
    )}\n\n`;

    if (!this.controller) {
      if (this.buffer.length >= MAX_BUFFERED) {
        this.resyncPending = true;
        return;
      }
      this.buffer.push(payload);
      return;
    }

    const desired = this.controller.desiredSize;
    if (desired !== null && desired <= 0) {
      // The client is not reading fast enough. Drop the payload: it is only a
      // dirty notification, so a later resync restores correctness.
      this.dropped += 1;
      return;
    }

    if (this.dropped > 0) {
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

    this.writeRaw(payload);
  }

  ping(): void {
    this.writeRaw(": ping\n\n");
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
  }

  get attached(): boolean {
    return this.controller !== null;
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
