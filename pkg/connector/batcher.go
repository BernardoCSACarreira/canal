package connector

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Batcher is pure policy with NO GOROUTINE.
//
// It is inverted control that drops into any select loop, so a source owns its own loop and
// the batcher never fights it. Four orthogonal triggers — count, byte size, period, and a
// declarative predicate over the record — and the first to fire wins.
//
// A Batcher is not safe for concurrent use; it is owned by the one goroutine whose loop it
// sits in.
type Batcher struct {
	policy config.BatchPolicy

	pending  []*record.Record
	bytes    int64
	openedAt time.Time
	now      func() time.Time
}

// NewBatcher returns a batcher enforcing p. A policy with no trigger at all would never
// flush, so the caller should have obtained p from config.Config.Batching, which refuses it.
func NewBatcher(p config.BatchPolicy) *Batcher {
	return &Batcher{policy: p, now: time.Now}
}

// Add appends r to the open batch and reports whether the batch should be flushed NOW.
//
// The record is added first and the triggers are evaluated afterwards, so a max-records
// trigger of one flushes on every record rather than one record late.
func (b *Batcher) Add(r *record.Record) bool {
	if r == nil {
		return false
	}
	if len(b.pending) == 0 {
		b.openedAt = b.now()
	}
	b.pending = append(b.pending, r)
	if n := r.Payload.Len(); n > 0 {
		b.bytes += int64(n)
	}

	p := b.policy
	if p.MaxRecords > 0 && len(b.pending) >= p.MaxRecords {
		return true
	}
	if p.MaxBytes > 0 && b.bytes >= p.MaxBytes {
		return true
	}
	if p.FlushOn != nil && p.FlushOn.EvalRecord(r) {
		return true
	}
	return false
}

// UntilNext reports how long until the age trigger fires. ok is false when there is no timed
// component to wait on, either because no age is configured or because the batch is empty —
// and an empty batch has no deadline, so a select must not wake for one.
func (b *Batcher) UntilNext() (time.Duration, bool) {
	if b.policy.MaxAge <= 0 || len(b.pending) == 0 {
		return 0, false
	}
	d := b.policy.MaxAge - b.now().Sub(b.openedAt)
	if d < 0 {
		d = 0
	}
	return d, true
}

// Expired reports whether the age trigger has already fired. A loop that woke for another
// reason checks this rather than recomputing the deadline.
func (b *Batcher) Expired() bool {
	d, ok := b.UntilNext()
	return ok && d == 0
}

// Flush moves every pending record into dst and empties the batcher.
//
// It MOVES already-stamped records; it never mints identity, so it is not an alternative to
// record.Batch.Add and cannot forge provenance. dst keeps whatever position and lane the
// caller set.
func (b *Batcher) Flush(dst *record.Batch) {
	if dst == nil {
		return
	}
	dst.Records = append(dst.Records, b.pending...)
	b.pending = b.pending[:0]
	b.bytes = 0
	b.openedAt = time.Time{}
}

// Len reports how many records are pending.
func (b *Batcher) Len() int { return len(b.pending) }

// Bytes reports the pending encoded size, counting only records whose byte view is
// materialised.
func (b *Batcher) Bytes() int64 { return b.bytes }
