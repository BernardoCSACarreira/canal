package connector

import (
	"context"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// LaneReader upgrades a source from "one lane per Read" to "many lanes per call".
//
// THE PROBLEM IT SOLVES, which four of the eight hostile connectors hit independently
// and all four rated FATAL: [Source.Read] is handed ONE [record.Batch], and a batch is
// bound to one [record.Allocator], which stamps one lane. A worker holding 32 chunk
// lanes, a source with 900 discovered streams, and a 32-way parallel scan running
// concurrently with a CDC tail therefore have no way to emit for more than one lane per
// call. Setting record.Batch.Lane looked like the escape and was worse than no escape:
// it mislabelled every record's Origin.Lane silently, measured at 33350 of 33500
// records attributed to a lane they did not come from.
//
// THE SHAPE. The engine owns one Allocator and one Batch per lane this instance holds,
// hands the source a slice of them, and the source fills whichever it has data for. No
// Allocator is ever retargeted, so provenance stays unforgeable and generation-local ids
// stay unique per lane; no batch is touched by two goroutines, so an Allocator's
// documented single-goroutine rule survives; and each batch carries its own Position, so
// per-lane cursors need no second vocabulary.
//
// ONE INTERFACE, NOT TWO, and the reason is worth stating because two shapes were
// proposed. A per-lane ReadLane(ctx, lane, dst) suits the 32 independent chunk readers
// and is unusable for the 900 multiplexed streams, where one upstream connection decides
// which lane the next record belongs to and 900 blocked goroutines is not an
// implementation. A batch-of-batches ReadLanes suits the multiplexed source and, with
// [SourceCaps.ReadConcurrency], also suits the independent one: the engine partitions
// the held lanes into at most ReadConcurrency DISJOINT groups and calls ReadLanes once
// per group on its own goroutine, so declaring ReadConcurrency 32 with 32 lanes is
// exactly the per-lane shape and declaring 1 is exactly the multiplexed one.
//
// [Source.Read] stays frozen and stays the interface the ninety-percent source
// implements. A source declaring ReadsLanes still implements Read — the required
// interface is required — and the core never calls it. Returning
// fault.PermanentInternal from it is the correct body.
type LaneReader interface {
	// ReadLanes fills any subset of dst and returns.
	//
	// Every batch in dst is pre-bound: dst[i].Lane is set, its allocator stamps that
	// lane, and it has been Reset. The source sets dst[i].Position for a prefix lane and
	// produces records only through dst[i].Add or dst[i].AddFor. It must not retain dst
	// or any batch in it past the return, and must not reorder or re-slice dst.
	//
	// It BLOCKS until at least one record is available on at least one lane, a position
	// advanced on at least one lane, the connection drops, or ctx is done. Filling
	// nothing and returning nil is legal ONLY when at least one batch carries an
	// advanced Position — a return with no records and no position is a spin, and the
	// core raises fault.PermanentContract on the second consecutive one rather than
	// burning a core.
	//
	// CANCELLATION MEANS DRAIN, exactly as in [Source.Read]: stop retrieving, return
	// what is already in dst, and return ctx.Err() only once nothing is left. The
	// engine admits what is in every batch BEFORE handling the error, so a source must
	// never discard records it has already produced.
	//
	// It returns fault.ErrEndOfInput only when EVERY lane it holds is finished and no
	// more will be announced. A single finished lane is signalled by that batch's
	// EndOfLane, not by an error.
	//
	// CONCURRENCY: the core makes up to SourceCaps.ReadConcurrency ReadLanes calls at
	// once, each with a DISJOINT set of lanes, each on its own goroutine, and no lane
	// appears in two live calls. A source declaring 1 needs no locking between calls; a
	// source declaring more needs whatever locking its own upstream client needs and
	// nothing more, because the batches themselves never overlap. The control goroutine
	// — Commit, Heartbeat, Backlog, Nack — still runs concurrently with all of them.
	ReadLanes(ctx context.Context, dst []*record.Batch) error
}

// Discoverer enumerates what a source can read, before a pipeline runs. This is what
// populates a stream picker with zero frontend code and makes drift a diff against a
// stored catalog.
//
// A source with no catalog — a webhook, a socket, a metrics scrape — simply does not
// implement it, and the UI shows "streams known only at runtime" rather than an empty
// table. That is the answer to "what is the minimum viable Discover": the minimum is not
// implementing it. Making Discover required would tax exactly the sources that can least
// afford it.
type Discoverer interface {
	Discover(ctx context.Context) (Catalog, error)
}

// Catalog is what a [Discoverer] found.
type Catalog struct {
	Streams []StreamDesc `json:"streams"`

	// Truncated means the source stopped early and there are more streams. It exists so
	// that a partial catalog says "partial" rather than quietly under-reporting.
	Truncated bool      `json:"truncated"`
	At        time.Time `json:"at"`
}

// StreamDesc describes one discoverable stream.
type StreamDesc struct {
	Name record.StreamName `json:"name"`

	// Schema is nil when not knowable without reading.
	Schema *schema.Schema `json:"schema,omitempty"`

	// Keys are candidate identity field paths, in preference order.
	Keys [][]string `json:"keys,omitempty"`

	// KeysFixed means the source dictates the key and the operator may not choose
	// another. This is a source-defined primary key as DATA, so a connector constrains
	// operator choices without any code in the core or the UI.
	KeysFixed bool `json:"keys_fixed"`

	// Supports declares which lane kinds this stream can produce, so the UI greys out
	// "initial scan" for a stream that cannot be scanned.
	Supports []LaneKind `json:"supports"`

	// EstimatedRecords and EstimatedBytes are zero when unknown, and the core reports
	// unknown rather than zero.
	EstimatedRecords uint64 `json:"estimated_records,omitempty"`
	EstimatedBytes   uint64 `json:"estimated_bytes,omitempty"`

	Label string `json:"label,omitempty"`
}

// Nackable lets a source observe terminal failures. Most sources do not want it — the
// core owns retry and dead-lettering — but a source that must park a message or notify
// upstream implements it.
//
// It is keyed on the source's OWN handle or position, never on a record.RecordID: a
// RecordID is assigned by the core after Read returned, and the source has never seen it
// and cannot map it to a delivery.
type Nackable interface {
	Nack(ctx context.Context, lane record.LaneID, ns []Nack) error
}

// Nack is one terminal failure, described in the source's own vocabulary.
type Nack struct {
	// Handle identifies the delivery for a discrete lane.
	Handle []byte `json:"handle,omitempty"`
	// Position identifies the point for a prefix lane.
	Position record.Position `json:"position,omitempty"`

	Class    fault.Class `json:"class"`
	Reason   string      `json:"reason"`
	Attempts int         `json:"attempts"`
}

// BacklogReporter answers "how much is left". Optional because for many sources it is
// unanswerable or expensive, and an unanswerable question must be answerable as unknown.
type BacklogReporter interface {
	Backlog(ctx context.Context, lane record.LaneID) (Backlog, error)
}

// Backlog is how much a lane has left.
//
// EVERY QUANTITY IS A POINTER, for the reason EventTimeLag already documented on this
// same struct: nil means "I cannot answer", zero means "caught up", and a source that
// conflates them publishes a lie the UI cannot detect. A feed that knows its event-time
// lag but not its record count — a paged REST endpoint with no total, a change stream
// with no queue depth — used to have to declare 0 records, which renders as "caught up"
// on a lane that is hours behind, or decline the whole capability and lose the lag it
// did know.
type Backlog struct {
	Records *uint64 `json:"records,omitempty"`
	Bytes   *uint64 `json:"bytes,omitempty"`

	// Exact distinguishes a count from an estimate, and it is reported as its own GAUGE,
	// never as a label: a label would split the series whenever the source changed
	// strategy and break every graph. An exact count and a statistical estimate must not
	// render identically.
	Exact bool `json:"exact"`

	// AsOf is when this was measured. A polled backlog with no AsOf implies a liveness it
	// does not have.
	AsOf time.Time `json:"as_of"`

	// EventTimeLag is how far behind the newest available record this lane is. Nil when
	// the source has no event time — never zero, which would read as "caught up".
	EventTimeLag *time.Duration `json:"event_time_lag,omitempty"`
}

// Count is the one-line constructor for a known [Backlog] quantity, so a source that
// knows its numbers does not pay a named local per field.
func Count(n uint64) *uint64 { return &n }

// Heartbeater lets a source be told "nothing has arrived for a while", so it can keep an
// upstream from pinning its own retention. Logical replication needs exactly this: with
// no messages to acknowledge, the write-ahead log is never reclaimed and the disk fills.
//
// It is also what makes a GATED stream lane safe: a tail lane waiting behind a long scan
// is not being read, so only a heartbeat can hold its slot. Build refuses the combination
// of a pruning upstream, a gated lane and no heartbeat.
//
// Heartbeat is a method on the control goroutine, NOT a batch. IT STILL NEVER CARRIES A
// POSITION, and that is a decision rather than an omission: it runs concurrently with
// the read path, so a position arriving here has no defined order against records the
// read path has already produced, and committing it would advance the cursor past
// unsettled records. The way a source advances a cursor without producing a record is a
// ZERO-RECORD POSITIONED BATCH from Read or ReadLanes, which is ordered by construction
// — see record.Batch.Position.
//
// A heartbeat DOES mark the lane idle. The core stamps LaneStatus.Idle and IdleSince, so
// hundreds of healthy quiet streams stop reporting a forever-rising CheckpointAge — the
// design's primary alert signal — merely for having nothing to say.
type Heartbeater interface {
	Heartbeat(ctx context.Context, lane record.LaneID, idle time.Duration) error
}

// StateAdopter lets a connector declare which other connectors' persisted lane state it
// can read, so a rewrite or a rename is a declaration rather than an operator runbook.
type StateAdopter interface {
	// AdoptsStateOf returns registered connector names whose persisted state this
	// connector can decode.
	AdoptsStateOf() []string
}
