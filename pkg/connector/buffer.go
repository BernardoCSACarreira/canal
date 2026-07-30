package connector

import (
	"context"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Buffer is a pluggable node between the source side and the sink side.
//
// It is the ONLY component permitted to shorten the acknowledgement chain, and it may do
// so only by declaring a durability domain at least as wide as the lane's assignment
// domain — at which point the CORE, not the buffer, settles the group on a successful Put.
// A buffer cannot lie its way into early acknowledgement because it does not perform the
// settling.
//
// Buffers are advanced components. If you are not certain you need one, you do not.
type Buffer interface {
	Open(ctx context.Context, rt BufferRuntime) error

	// Put offers a batch. It returns how many records were accepted and which were refused.
	//
	// BOUNDED BY CONSTRUCTION (design rule R6): Put can always refuse, and a buffer with no
	// refusal path is not a buffer. The engine applies the configured when-full policy to
	// the remainder.
	//
	// Put deep-copies what it retains. In-process gets no zero-copy advantage where a
	// semantic depends on it, because a builtin connector mutating a record the engine still
	// holds would be impossible over a wire and must therefore be impossible in-process too.
	Put(ctx context.Context, b *record.Batch) (Accepted, error)

	// Get fills dst, blocking until something is available, until Drain has been called and
	// the buffer is empty (fault.ErrEndOfInput), or until ctx is done.
	//
	// Get is NON-DESTRUCTIVE: the records stay until Trim. One interface with a destructive
	// pop and a separate non-destructive trim is two incompatible models inside one type,
	// and it makes a popped-but-unsettled batch unrecoverable after a crash.
	Get(ctx context.Context, dst *record.Batch) error

	// Trim releases everything up to and including the given group, which the core calls
	// only after those records have settled downstream. For a durable buffer this is what
	// reclaims disk, and per-lane segmentation is what makes it an unlink rather than a
	// compaction.
	Trim(ctx context.Context, through record.GroupID) error

	// Drain declares that no more Puts will come. Idempotent. This is how end-of-input
	// propagates through a stateful node.
	Drain(ctx context.Context) error

	// Depth reports occupancy for metrics and the read model.
	Depth() Depth

	Close(ctx context.Context) error
}

// Accepted is what a buffer took.
type Accepted struct {
	Records int

	// Refused is non-empty when the buffer took only part of the batch.
	Refused []record.RecordID
}

// Depth is a buffer's occupancy. Capacities are reported alongside the levels so a UI can
// render fullness without knowing the buffer's config.
type Depth struct {
	Records        int
	RecordCapacity int
	Bytes          int64
	ByteCapacity   int64

	// OldestAge is how long the oldest un-trimmed record has been held. It is the soft
	// counterpart to hard blocking: "waiting for a buffer" and "blocked on the downstream"
	// are different diagnoses.
	OldestAge time.Duration
}

// Durability is a buffer's durability DOMAIN, not a bool.
//
// A bool is a fatal defect: a write-ahead log in one pod's data directory is
// process-durable and node-local, while the commit it authorises is global, so node loss
// or lane reassignment orphans the log after the source has already committed past it. A
// kill -9 test proves process durability and says nothing about node loss.
type Durability uint8

const (
	// DurabilityNone is memory. It never shortens the ack chain.
	DurabilityNone Durability = iota
	// DurabilityProcess survives a crash of this process only.
	DurabilityProcess
	// DurabilityNode survives a process crash but is lost with the node.
	DurabilityNode
	// DurabilityCluster survives node loss and is readable by another worker. No shipped
	// buffer declares it: a cluster-durable buffer is a distributed log, and canal declines
	// to reimplement one.
	DurabilityCluster
)

var durabilityNames = [...]string{
	DurabilityNone:    "none",
	DurabilityProcess: "process",
	DurabilityNode:    "node",
	DurabilityCluster: "cluster",
}

// String returns the stable snake_case token for d.
func (d Durability) String() string {
	if int(d) < len(durabilityNames) {
		return durabilityNames[d]
	}
	return "none"
}

// WhenFull is the configured policy when a buffer or a bounded edge refuses.
//
// There is no "unbounded" member: unbounded growth to OOM is inexpressible, and silent
// loss is unconfigurable.
type WhenFull uint8

const (
	// WhenFullBlock applies backpressure. The default, and the only one that never loses
	// data.
	WhenFullBlock WhenFull = iota

	// WhenFullDropNewest discards the incoming records and COUNTS them.
	//
	// Newest, not oldest: dropping the oldest would discard data whose prefix the source
	// may already have been told is safe, breaking the cursor invariant. The affected group
	// is settled abandoned so the source is told.
	WhenFullDropNewest

	// WhenFullReject settles the group as abandoned and lets the source see a non-zero
	// Ack.Abandoned. This is the rejection path design rule R6 demands.
	WhenFullReject

	// WhenFullOverflow spills to the next buffer in the graph: a small memory buffer in
	// front of a large disk one, so the common case never touches disk and a sustained sink
	// outage still does not block.
	WhenFullOverflow
)

var whenFullNames = [...]string{
	WhenFullBlock:      "block",
	WhenFullDropNewest: "drop_newest",
	WhenFullReject:     "reject",
	WhenFullOverflow:   "overflow",
}

// String returns the stable snake_case token for w. It is the wire form of the
// stage-standard when_full field.
func (w WhenFull) String() string {
	if int(w) < len(whenFullNames) {
		return whenFullNames[w]
	}
	return "block"
}

// ParseWhenFull maps the stage-standard field's token onto the enum. It lives here, next
// to the enum, so that the only mapping between the wire token and the Go value is the one
// String produces and this reverses — not a display map, and not duplicated in the engine.
func ParseWhenFull(s string) (WhenFull, bool) {
	for i, n := range whenFullNames {
		if n == s {
			return WhenFull(i), true
		}
	}
	return WhenFullBlock, false
}
