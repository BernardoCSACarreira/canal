export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [k: string]: JsonValue };

/** Payload shape accepted at the ingestion edge (matches OpenAPI IngestEvent). */
export type BufferedIngestEvent = {
  id: string;
  type: string;
  occurredAt: string;
  payload?: JsonValue;
};

/** One durable row in the P1-local primary Event Buffer (RFC §5.1). */
export type BufferedIngestRecord = {
  sequence: number;
  source: string;
  enqueuedAtMs: number;
  event: BufferedIngestEvent;
};

export type EventBufferMetrics = {
  depth: number;
  /** Milliseconds since the oldest still-buffered record was appended; `null` when empty. */
  oldestAgeMs: number | null;
};

/**
 * Phase-1 local in-memory Event Buffer: append, monotonic sequence, read cursor,
 * trim-after-commit, and depth / oldest-age metrics (see RFC buffer matrix P1-local row).
 */
export class P1LocalEventBuffer {
  private nextSequence = 1;
  private readonly queue: BufferedIngestRecord[] = [];

  /**
   * Append accepted events in order; each receives the next monotonic `sequence`.
   * @returns Assigned sequence numbers (same order as `events`).
   */
  append(source: string, events: readonly BufferedIngestEvent[]): number[] {
    const enqueuedAtMs = Date.now();
    const assigned: number[] = [];
    for (const event of events) {
      const sequence = this.nextSequence++;
      this.queue.push({ sequence, source, enqueuedAtMs, event });
      assigned.push(sequence);
    }
    return assigned;
  }

  /**
   * Read records strictly after `cursorSequence` (0 means "from start"), FIFO, at most `limit`.
   */
  readAfter(cursorSequence: number, limit: number): BufferedIngestRecord[] {
    if (limit < 1) return [];
    const out: BufferedIngestRecord[] = [];
    for (const rec of this.queue) {
      if (rec.sequence <= cursorSequence) continue;
      out.push(rec);
      if (out.length >= limit) break;
    }
    return out;
  }

  /** Drop all records with `sequence <= commitThroughSequence` (trim-after-commit watermark). */
  trimAfterCommit(commitThroughSequence: number): void {
    while (this.queue.length > 0 && this.queue[0]!.sequence <= commitThroughSequence) {
      this.queue.shift();
    }
  }

  getMetrics(nowMs = Date.now()): EventBufferMetrics {
    if (this.queue.length === 0) {
      return { depth: 0, oldestAgeMs: null };
    }
    let oldest = this.queue[0]!.enqueuedAtMs;
    for (const r of this.queue) {
      if (r.enqueuedAtMs < oldest) oldest = r.enqueuedAtMs;
    }
    return { depth: this.queue.length, oldestAgeMs: Math.max(0, nowMs - oldest) };
  }
}
