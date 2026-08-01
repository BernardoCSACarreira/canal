package ledger

import (
	"context"
	"sync"
	"time"
)

// node is one tracked admission. It is deliberately NOT generic even though [Tracker] is: keeping the
// payload as an any lets [Ticket] stay a plain comparable struct rather than a generic one, which is
// what makes a ticket usable as a map key and comparable with ==.
//
// The cost is one interface box per admitted group — per group, not per record — which is the right
// trade for a type that must be a map key.
type node struct {
	payload   any
	weight    uint64
	refs      uint32
	at        time.Time
	resolved  bool
	abandoned bool

	// done is set once the node has left the list, so a duplicate release against a stale ticket is a
	// no-op rather than a corruption of the prefix.
	done bool

	// SINGLY LINKED, deliberately. This was a doubly-linked list whose back pointer was assigned in
	// three places and followed in none: the only traversal is advanceLocked walking the contiguous
	// resolved prefix forward from the head, and retirement is always head-first. The pointer cost a
	// write per admission and bought nothing.
	next *node
}

// Ticket identifies one tracked node.
//
// It is a comparable STRUCT holding an unexported pointer, not a func value: funcs are neither
// comparable nor usable as map keys, so a design that identified a pending node by a returned closure
// could not implement the poison-record escape that is its own answer to head-of-line blocking.
type Ticket struct{ n *node }

// IsZero reports whether t identifies nothing.
func (t Ticket) IsZero() bool { return t.n == nil }

// Tracker receives an ordered feed of tracked payloads and an unordered feed of resolutions, and
// reports the highest payload that may safely be committed: the last payload in the contiguous
// resolved prefix.
//
// This is one of only two places generics genuinely help in canal. There is exactly one type
// parameter, it is used with record.Position in production and with ints in its own tests, and
// nothing erases it at a registry boundary.
//
// Safe for concurrent use.
type Tracker[P any] struct {
	mu sync.Mutex

	head, tail *node

	pending uint64 // outstanding weight
	nodes   int
	budget  uint64

	// wake is closed and replaced on every resolution, so a blocked Track is woken by a channel it can
	// select on together with its ctx.
	//
	// NOT a sync.Cond: a Cond cannot be woken by the ctx in Track's own signature, so a graceful
	// shutdown hangs on a full tracker. That is a two-line difference and a whole class of hang.
	wake chan struct{}

	resolved   P
	resolvedOK bool

	admitted  uint64
	settled   uint64
	abandoned uint64

	closed bool
}

// NewTracker creates a tracker whose pending weight is capped at budget.
//
// Track BLOCKS when admitting more would exceed it, and THAT BLOCKING IS canal's backpressure at the
// source edge — one mechanism, not five overlapping knobs.
//
// A budget of zero is treated as one, so a misconfigured budget stalls visibly at one record rather
// than deadlocking on admission.
func NewTracker[P any](budget uint64) *Tracker[P] {
	if budget == 0 {
		budget = 1
	}
	return &Tracker[P]{budget: budget, wake: make(chan struct{})}
}

// Track admits a payload with a logical weight (a record count) and a reference count (how many
// settlements must arrive).
//
// It blocks while pending+weight would exceed the budget, returning ctx.Err() if the wait is
// cancelled. An admission whose weight alone exceeds the budget is admitted when the tracker is
// otherwise empty, because refusing it forever would be a deadlock rather than backpressure.
//
// refs of zero is treated as one: a payload that discharges no references could never resolve. A
// payload that genuinely discharges nothing — a position carried by a batch with no records — is
// admitted through [Tracker.TrackResolved] instead.
func (t *Tracker[P]) Track(ctx context.Context, payload P, weight uint64, refs uint32) (Ticket, error) {
	if weight == 0 {
		weight = 1
	}
	if refs == 0 {
		refs = 1
	}
	for {
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return Ticket{}, ErrTrackerClosed
		}
		if t.nodes == 0 || t.pending+weight <= t.budget {
			n := t.appendLocked(&node{payload: payload, weight: weight, refs: refs, at: time.Now()})
			t.mu.Unlock()
			return Ticket{n: n}, nil
		}
		wake := t.wake
		t.mu.Unlock()

		select {
		case <-wake:
		case <-ctx.Done():
			return Ticket{}, ctx.Err()
		}
	}
}

// TryTrack is Track without the wait: it admits if there is room right now and reports (zero, false)
// if there is not.
//
// IT EXISTS SO THAT "BLOCK" IS A CHOICE RATHER THAN THE ONLY BEHAVIOUR. connector.WhenFull offers
// block, drop_newest, reject and overflow; blocking is the default and the only one that never loses
// data, but a source feeding a queue that must not stall needs the others, and until this existed the
// engine had no way to ask "is there room" without committing to waiting for it.
//
// Same admission rule as Track, including the one that matters: a payload heavier than the whole
// budget is admitted when the tracker is otherwise empty, because refusing it forever is a deadlock
// rather than backpressure.
func (t *Tracker[P]) TryTrack(payload P, weight uint64, refs uint32) (Ticket, bool) {
	if weight == 0 {
		weight = 1
	}
	if refs == 0 {
		refs = 1
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return Ticket{}, false
	}
	if t.nodes == 0 || t.pending+weight <= t.budget {
		return Ticket{n: t.appendLocked(&node{payload: payload, weight: weight, refs: refs, at: time.Now()})}, true
	}
	return Ticket{}, false
}

// TrackResolved admits a payload that discharges NO references and is complete the moment it arrives:
// a lane position carried by a batch with zero records.
//
// It returns the prefix advance exactly as [Tracker.Release] does — (payload, true) when the
// contiguous resolved prefix moved, (zero, false) when it did not — which is what makes "advanced to
// here" and "queued behind something unsettled" the same typed answer on both paths.
//
// It NEVER BLOCKS and never takes a Ticket, because there is nothing to release later. It still enters
// the ORDERED list, so the prefix reaches it only once everything admitted before it has resolved;
// committing such a position directly would commit past unsettled records, which is the one thing this
// package exists to prevent. It contributes zero weight, so an idle lane emitting a position every
// second costs no budget at all — which is exactly what a thousand quiet streams need.
func (t *Tracker[P]) TrackResolved(payload P) (P, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var zero P
	if t.closed {
		return zero, false
	}

	// COALESCE a run of already-resolved positions at the tail instead of growing the list.
	//
	// This is the fix for the unbounded path the compliance audit measured at 419.6 MiB for five
	// million nodes. TrackResolved contributes zero weight by design — that is what lets a thousand
	// quiet streams emit a position every second without consuming budget — but the budget is
	// weight-based, so nothing bounded the NODE COUNT. A lane whose prefix is stuck behind one
	// unsettled record accumulates a node per idle poll, forever, and the memory is invisible to
	// every backpressure signal the package has.
	//
	// Merging is sound because the two nodes are adjacent and both resolved: advanceLocked walks
	// them in one pass and stops at the later payload either way, so the earlier position is never
	// observable as the prefix maximum. Overwriting the tail's payload gives the identical answer in
	// O(1) space.
	//
	// Only a zero-weight resolved tail is merged. A released TRACKED node is also resolved, but it
	// carries weight and reference bookkeeping that other callers still reason about, so it is left
	// alone.
	if n := t.tail; n != nil && n.resolved && n.weight == 0 && n.refs == 0 {
		n.payload = payload
		n.at = time.Now()
		return t.advanceLocked()
	}

	t.appendLocked(&node{payload: payload, at: time.Now(), resolved: true})
	return t.advanceLocked()
}

// appendLocked links n at the tail and accounts for it. The caller holds the mutex.
func (t *Tracker[P]) appendLocked(n *node) *node {
	if t.tail == nil {
		t.head, t.tail = n, n
	} else {
		t.tail.next = n
		t.tail = n
	}
	t.pending += n.weight
	t.nodes++
	t.admitted += n.weight
	return n
}

// Release discharges n references from a ticket. When the count reaches zero the node resolves.
//
// The return reports whether the contiguous prefix ADVANCED and, if so, to which payload — so
// "resolved, but commit nothing" is a typed answer rather than a silent zero.
func (t *Tracker[P]) Release(k Ticket, n uint32) (P, bool) {
	return t.discharge(k, n, false)
}

// Abandon resolves a node as terminally not-delivered.
//
// It advances the prefix exactly as a release does, but records the abandonment so the acknowledgement
// carries a non-zero abandoned count. This is what makes a poison record unable to livelock the
// pipeline: the terminal disposition abandons, the prefix moves, the source unblocks.
func (t *Tracker[P]) Abandon(k Ticket) (P, bool) {
	return t.discharge(k, 0, true)
}

func (t *Tracker[P]) discharge(k Ticket, n uint32, abandon bool) (P, bool) {
	var zero P
	if k.n == nil {
		return zero, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if k.n.done {
		// A stale ticket. Silently ignored rather than double-counted: a double release would advance
		// the prefix past unsettled work, which is the one thing this package exists to prevent.
		return zero, false
	}
	switch {
	case abandon:
		k.n.refs = 0
		k.n.abandoned = true
	case n >= k.n.refs:
		k.n.refs = 0
	default:
		k.n.refs -= n
	}
	if k.n.refs > 0 {
		return zero, false
	}
	k.n.resolved = true
	return t.advanceLocked()
}

// advanceLocked walks the contiguous resolved prefix from the head, retiring nodes as it goes. The
// caller holds the mutex.
func (t *Tracker[P]) advanceLocked() (P, bool) {
	var last P
	moved := false
	for t.head != nil && t.head.resolved {
		n := t.head
		last, _ = n.payload.(P)
		moved = true

		t.head = n.next
		if t.head == nil {
			t.tail = nil
		}
		n.done, n.next = true, nil

		t.pending -= n.weight
		t.nodes--
		if n.abandoned {
			t.abandoned += n.weight
		} else {
			t.settled += n.weight
		}
	}
	if moved {
		t.resolved, t.resolvedOK = last, true
		close(t.wake)
		t.wake = make(chan struct{})
	}
	return last, moved
}

// Resolved returns the current contiguous-prefix payload, and false when nothing has resolved yet.
func (t *Tracker[P]) Resolved() (P, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.resolved, t.resolvedOK
}

// Pending reports outstanding weight, node count, and the age and payload of the oldest outstanding
// node.
//
// These four numbers are the whole diagnostic story for "why is progress not advancing", and no
// acknowledgement-based system in the surveyed field exposes them.
func (t *Tracker[P]) Pending() (weight uint64, nodes int, oldest time.Duration, oldestPayload P) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.head != nil {
		oldest = time.Since(t.head.at)
		oldestPayload, _ = t.head.payload.(P)
	}
	return t.pending, t.nodes, oldest, oldestPayload
}

// Counts reports admitted, settled and abandoned WEIGHT — that is, record counts, since weight is a
// record count. Settled and abandoned are counted only as the prefix retires a node, so they can never
// exceed what has actually been accounted for.
func (t *Tracker[P]) Counts() (admitted, settled, abandoned uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.admitted, t.settled, t.abandoned
}

// Budget reports the configured in-flight bound.
func (t *Tracker[P]) Budget() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.budget
}

// Close releases every blocked Track. Outstanding nodes are left alone: closing a tracker is a
// teardown, not a settlement, and pretending otherwise would acknowledge undelivered work.
func (t *Tracker[P]) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	close(t.wake)
	t.wake = make(chan struct{})
}
