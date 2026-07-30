package record

import (
	"iter"
	"time"
)

// Allocator stamps identity. It is the ONLY way an [Origin] comes into existence,
// it lives in this package so there is no cross-package write to an unexported
// field, and a connector never holds one.
//
// The engine creates one Allocator per (lane, generation) and hands the source a
// [Batch] that already carries it. A source therefore cannot forge provenance, and
// there is no second Record constructor — the hole every "lend a slot" design leaves
// open by also exporting a raw emitter.
//
// An Allocator is not safe for concurrent use. Exactly one goroutine — the source
// node's read goroutine — ever holds one.
type Allocator struct {
	tenant   TenantID
	pipeline PipelineID
	node     NodeID
	lane     LaneID
	stream   StreamName

	nextID    RecordID
	nextGroup GroupID
}

// NewAllocator returns an allocator stamping the given constant provenance.
//
// firstID and firstGroup are the first values [Batch.Add] and
// [Allocator.NextGroup] will hand out. The engine derives them from the
// generation so that ids do not repeat within a generation; they are not durable
// and must never be used as a persisted key.
func NewAllocator(t TenantID, p PipelineID, n NodeID, l LaneID, stream StreamName, firstID RecordID, firstGroup GroupID) *Allocator {
	return &Allocator{
		tenant:    t,
		pipeline:  p,
		node:      n,
		lane:      l,
		stream:    stream,
		nextID:    firstID,
		nextGroup: firstGroup,
	}
}

// NextGroup begins a new settlement group and returns its id.
func (a *Allocator) NextGroup() GroupID {
	g := a.nextGroup
	a.nextGroup++
	return g
}

// nextRecordID hands out the next generation-local record identity.
func (a *Allocator) nextRecordID() RecordID {
	id := a.nextID
	a.nextID++
	return id
}

// Lane reports the lane this allocator stamps for.
func (a *Allocator) Lane() LaneID { return a.lane }

// Batch is a caller-owned, reusable buffer. The engine allocates one per node and
// passes the same pointer every iteration, following the driver.Rows.Next idiom.
//
// A batch — not a record — is what a [Position] attaches to for a prefix lane. This
// matches what real connectors do and it means commit points align with
// source-meaningful boundaries rather than with a clock.
type Batch struct {
	// Records is the batch contents. Pointers, not values: a several-hundred-byte
	// value record copied per range iteration is measurable, and a value type
	// containing a reference field is how fan-out branches end up sharing state.
	Records []*Record

	// Lane is the lane these records belong to. A source sets it on every Read.
	//
	// It MUST equal the lane the batch's allocator stamps, which is what NewBatch
	// already set it to. Setting it to any other lane used to mislabel every record
	// silently; the ledger now REFUSES such a batch with fault.PermanentContract
	// naming both lanes. A source serving several lanes does not retarget one batch —
	// it implements connector.LaneReader and is handed one batch per lane.
	Lane LaneID

	// Position is the position AFTER the last record, for an OrderingPrefix lane.
	// Unset for OrderingDiscrete lanes, whose per-record handles carry progress.
	//
	// A batch with a Position and ZERO RECORDS is legal and meaningful: it says "the
	// lane advanced to here and produced nothing you need to deliver". A filtered
	// tail, a paging cursor that turned up only already-seen rows, a chunk planner
	// that only planned, an idle poll that only proved idleness — all of them advance
	// a cursor without a record. The ledger admits such a batch with ZERO references
	// and resolves it AT ADMISSION, so the position enters the prefix immediately and
	// in order. Refusing it is what wedges those lanes forever.
	Position Position

	// EndOfLane is set by the source on the final batch of a lane it is retiring. It
	// may be set with zero records. The engine finishes the lane only once every group
	// admitted for it has settled.
	//
	// It is not restricted to a BOUNDED lane: a source retiring an unbounded lane —
	// a partition that was revoked upstream, a stream that was dropped, a gated tail
	// that is no longer wanted — sets it too, and the resulting Ack carries
	// LaneFinished so the source learns the retirement became durable.
	EndOfLane bool

	alloc *Allocator
	group GroupID
	cap   int
	bytes int64
}

// NewBatch returns a batch bound to an allocator, with a hard capacity of capHint
// records. Called by the engine, never by a connector.
//
// capHint is a HARD cap, not a hint to grow past: [Batch.Add] returns nil at the cap
// rather than reallocating under live pointers. A capHint below one is raised to
// one.
func NewBatch(a *Allocator, capHint int) *Batch {
	if capHint < 1 {
		capHint = 1
	}
	b := &Batch{
		Records: make([]*Record, 0, capHint),
		alloc:   a,
		cap:     capHint,
	}
	if a != nil {
		b.group = a.NextGroup()
		b.Lane = a.lane
	}
	return b
}

// NewBatchLike returns an empty batch sharing src's allocator, lane and settlement group.
//
// It exists for the engine's reframing nodes — the splitter, the transform output, the
// per-partition batcher — where a change of framing must not be a change of identity. Because
// the group is shared, no record gets a new id and no group can resolve early.
func NewBatchLike(src *Batch, capHint int) *Batch {
	if capHint < 1 {
		capHint = 1
	}
	b := &Batch{Records: make([]*Record, 0, capHint), cap: capHint}
	if src != nil {
		b.alloc = src.alloc
		b.group = src.group
		b.Lane = src.Lane
	}
	return b
}

// Add lends the source a zero-valued record slot with its identity and provenance
// ALREADY STAMPED, and appends it to the batch. This is the only way a source
// produces a record.
//
// The record's Origin.Stream is the allocator's stream — the lane's declared stream. A
// source whose one lane carries several streams uses [Batch.AddFor].
//
// It returns nil when the batch is at its hard cap; a source that ignores the nil
// and dereferences it gets a panic in its own code on its first test run, which is
// the correct place for that mistake to surface.
func (b *Batch) Add() *Record {
	if b.alloc == nil {
		return nil
	}
	return b.AddFor(b.alloc.stream)
}

// AddFor is [Batch.Add] for a record whose logical stream is not the lane's declared
// one. It stamps Origin.Stream — and therefore Dest — with the given name and is
// otherwise identical.
//
// It exists because one lane serving several streams is the NORMAL shape of a shared
// log: one MySQL binlog coordinate, one Postgres LSN, one Mongo change-stream resume
// token, one Kafka partition offset, each interleaving many tables or collections
// under ONE cursor. Without it such a source must either collapse every table into one
// dedupe scope — reintroducing design rule R5's cross-stream key collision — or
// announce one lane per table and invent a cursor per table that its upstream does not
// have.
//
// Origin.Stream stays allocator-stamped and unwritable AFTER the fact: this is a
// parameter at the one moment identity comes into existence, not a setter, so a
// transform still cannot retarget settlement identity. Rewriting the DESTINATION
// remains [Record].Dest's job.
func (b *Batch) AddFor(stream StreamName) *Record {
	if len(b.Records) >= b.cap || b.alloc == nil {
		return nil
	}
	a := b.alloc
	id := a.nextRecordID()
	r := &Record{
		origin: Origin{
			Tenant:   a.tenant,
			Pipeline: a.pipeline,
			Node:     a.node,
			Lane:     a.lane,
			Stream:   stream,
			Group:    b.group,
			ID:       id,
			Root:     id,
			ReadAt:   time.Now(),
			refs:     1,
		},
		Dest: stream,
	}
	b.Records = append(b.Records, r)
	return r
}

// Derive appends a new record in the SAME settlement group as in, with a fresh
// RecordID, Parent and Root set, and refs 1.
//
// The caller must also perform the engine's expansion accounting; the engine does
// this automatically for every transform, decoder and fan-out edge, so no connector
// ever calls it.
//
// There is deliberately NO Copy that preserves a RecordID. Two branches of a
// fan-out sharing one RecordID makes "both branches settled" indistinguishable from
// "one branch double-settled", which is a group resolving early and a position
// committed past unwritten data. Every materialisation gets a fresh id.
//
// It returns nil at the batch's hard cap, exactly as [Batch.Add] does.
func (b *Batch) Derive(in *Record) *Record {
	if len(b.Records) >= b.cap || b.alloc == nil || in == nil {
		return nil
	}
	o := in.origin
	o.ID = b.alloc.nextRecordID()
	o.Parent = in.origin.ID
	o.Parents = nil
	if in.origin.Root != 0 {
		o.Root = in.origin.Root
	} else {
		o.Root = in.origin.ID
	}
	o.refs = 1

	r := &Record{
		origin:    o,
		Dest:      in.Dest,
		EventTime: in.EventTime,
		Payload:   in.Payload.Clone(),
		Meta:      in.Meta.Clone(),
		Change:    in.Change.Clone(),
		Schema:    in.Schema,
	}
	if in.handle != nil {
		r.handle = append([]byte(nil), in.handle...)
	}
	b.Records = append(b.Records, r)
	return r
}

// Merge appends one record whose settlement discharges every parent's references.
//
// This is the N-to-1 shape — windowing, aggregation, regrouping — and without it an
// aggregating transform can never settle its extra inputs and the lane's prefix
// stalls forever.
//
// The merged record joins the FIRST parent's settlement group. Merging across groups
// is legal and is precisely how a regrouping transform discharges references it did
// not open.
//
// It returns nil at the batch's hard cap or when no parents are given.
func (b *Batch) Merge(parents ...*Record) *Record {
	if len(b.Records) >= b.cap || b.alloc == nil || len(parents) == 0 {
		return nil
	}
	first := parents[0]
	o := first.origin
	o.ID = b.alloc.nextRecordID()
	o.Parent = 0
	o.Parents = make([]RecordID, 0, len(parents))
	var refs uint32
	for _, p := range parents {
		if p == nil {
			continue
		}
		o.Parents = append(o.Parents, p.origin.ID)
		refs += p.origin.refs
	}
	o.refs = refs
	if first.origin.Root != 0 {
		o.Root = first.origin.Root
	} else {
		o.Root = first.origin.ID
	}

	r := &Record{
		origin:    o,
		Dest:      first.Dest,
		EventTime: first.EventTime,
		Meta:      first.Meta.Clone(),
		Schema:    first.Schema,
	}
	b.Records = append(b.Records, r)
	return r
}

// Reset empties the batch and opens a NEW settlement group, because one source
// batch becomes one group. It keeps the underlying slice's capacity so the engine's
// reuse is allocation-free.
//
// It deliberately does not clear Lane: a source reading one lane per call sets Lane
// once and Reset must not undo it.
func (b *Batch) Reset() {
	b.Records = b.Records[:0]
	b.Position = Position{}
	b.EndOfLane = false
	b.bytes = 0
	if b.alloc != nil {
		b.group = b.alloc.NextGroup()
	}
}

// Len reports how many records the batch holds.
func (b *Batch) Len() int { return len(b.Records) }

// Cap reports the batch's hard capacity.
func (b *Batch) Cap() int { return b.cap }

// Group reports the settlement group source-produced records in this batch belong
// to.
func (b *Batch) Group() GroupID { return b.group }

// Bytes reports the batch's accumulated encoded size, counting only records whose
// byte view is materialised. It is a measurement, not a guess: a record with no
// encoded form contributes nothing rather than an estimate.
func (b *Batch) Bytes() int64 {
	var n int64
	for _, r := range b.Records {
		if l := r.Payload.Len(); l > 0 {
			n += int64(l)
		}
	}
	return n
}

// All is an ergonomic adapter for range-over-func. It is never the primary API:
// iter.Seq on a plugin surface inverts control away from the runtime, cannot carry a
// per-batch position, and cannot express "nothing available, come back when this
// channel closes".
func (b *Batch) All() iter.Seq[*Record] {
	return func(yield func(*Record) bool) {
		for _, r := range b.Records {
			if !yield(r) {
				return
			}
		}
	}
}

// SetSeq is how the ledger stamps the core-assigned sequence onto the batch's
// position at admission. It exists as a named method rather than a direct field
// write so that the "the connector must not set Seq; the core overwrites it" rule
// has exactly one enforcement site.
func (b *Batch) SetSeq(seq uint64) { b.Position.Seq = seq }

// SetGroup reassigns the batch's settlement group. It exists for the engine's
// regrouping nodes; a connector never calls it.
func (b *Batch) SetGroup(g GroupID) { b.group = g }
