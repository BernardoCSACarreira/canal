/**
 * Phase-1 local checkpoint store (RFC `rfc-phase1-buffer-checkpoint-provider-matrix.md` §5.2).
 * In-process persistence of consumer cursors; `fence` is a deliberate no-op until remote drivers add epochs.
 */

/** Cursor persisted between restarts; `committedSequence` aligns with {@link P1LocalEventBuffer} watermarks. */
export type CheckpointCursor = {
  committedSequence: number;
  updatedAtMs: number;
};

export interface CheckpointStore {
  /** Last successful checkpoint for `consumerId`, or `null` if none (start from beginning). */
  load(consumerId: string): CheckpointCursor | null;
  /**
   * Record progress after durable processing. Per P1-local matrix, sequence is monotonic per consumer:
   * the stored `committedSequence` is the maximum of any prior value and the incoming cursor.
   */
  save(consumerId: string, committedSequence: number, nowMs?: number): void;
  /**
   * Optional fencing for leader/epoch handoff (matrix §4). P1-local implementation does nothing.
   */
  fence(_consumerId: string, _epochToken: string): void;
}

export class P1LocalCheckpointStore implements CheckpointStore {
  private readonly byConsumer = new Map<string, CheckpointCursor>();

  load(consumerId: string): CheckpointCursor | null {
    const cur = this.byConsumer.get(consumerId);
    return cur ? { ...cur } : null;
  }

  save(consumerId: string, committedSequence: number, nowMs = Date.now()): void {
    const prev = this.byConsumer.get(consumerId)?.committedSequence ?? 0;
    const nextSeq = Math.max(prev, committedSequence);
    this.byConsumer.set(consumerId, { committedSequence: nextSeq, updatedAtMs: nowMs });
  }

  fence(_consumerId: string, _epochToken: string): void {
    // P1-local: noop (reserved for fenced remote checkpoint drivers).
  }
}
