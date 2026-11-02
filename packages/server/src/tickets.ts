function randomToken(): string {
  const crypto = globalThis.crypto;
  if (crypto?.randomUUID) return crypto.randomUUID().replace(/-/g, "");
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

interface TicketRecord {
  connId: string;
  expiresAt: number;
}

/**
 * The handshake contract, kept structural so it can be backed by a shared
 * store (Redis, a table) when `/live` runs on more than one process.
 *
 * `TicketStore` below is in-process, which means a ticket minted on one
 * process cannot be redeemed on another. See the deployment note in the
 * README.
 */
export interface TicketStoreLike {
  issue(connId: string): { ticket: string; expiresAt: number };
  redeem(ticket: string): string | null;
}

/**
 * Single-use, short-lived tickets for the SSE handshake.
 *
 * `EventSource` cannot send headers, so the stream URL has to carry something.
 * It must not carry the session token: URLs end up in logs, referrers and proxy
 * history. A ticket that is unguessable, scoped to one connection, and dead
 * after 30 seconds is the safe version of that.
 */
export class TicketStore implements TicketStoreLike {
  private readonly tickets = new Map<string, TicketRecord>();
  private readonly ttlMs: number;

  constructor(options: { ttlMs?: number } = {}) {
    this.ttlMs = options.ttlMs ?? 30_000;
  }

  issue(connId: string): { ticket: string; expiresAt: number } {
    this.sweep();
    const ticket = randomToken();
    const expiresAt = Date.now() + this.ttlMs;
    this.tickets.set(ticket, { connId, expiresAt });
    return { ticket, expiresAt };
  }

  redeem(ticket: string): string | null {
    const record = this.tickets.get(ticket);
    if (!record) return null;
    this.tickets.delete(ticket);
    if (record.expiresAt < Date.now()) return null;
    return record.connId;
  }

  sweep(): void {
    const now = Date.now();
    for (const [ticket, record] of this.tickets) {
      if (record.expiresAt <= now) this.tickets.delete(ticket);
    }
  }
}
