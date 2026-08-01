package telemetry

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// PipelineStatus is THE read model. One canonical struct, one source of truth, three serialisations:
// an HTTP snapshot, an SSE stream, and the CLI.
//
// The rule that makes it a contract rather than a struct: EVERY UNKNOWN IS A NIL POINTER, never a
// zero.
type PipelineStatus struct {
	Tenant   record.TenantID   `json:"tenant"`
	Pipeline record.PipelineID `json:"pipeline"`

	// Generation is the stored config revision; ObservedGeneration is the revision actually applied.
	// Their inequality is what makes "my config change silently did not apply" visible.
	Generation         uint64 `json:"generation"`
	ObservedGeneration uint64 `json:"observedGeneration"`

	AsOf time.Time `json:"asOf"`

	// Version is monotonic: it is both the SSE cursor and the ETag.
	Version uint64 `json:"version"`

	// Complete is false when the aggregator did not hear from every worker. A status document that
	// silently omits a worker is the same lie as a health check returning 200 for a broken pipeline.
	Complete bool `json:"complete"`

	// Missing names the worker ids not heard from.
	Missing []string `json:"missing,omitempty"`

	// StaleAfterSeconds is the age past which a worker's last report stops counting, and it is what
	// makes Complete falsifiable.
	//
	// "We heard from every worker" is not a claim until "heard from" has a definition. Without a
	// stated threshold an aggregator can call a document complete on reports of any age, and the one
	// field that exists to admit partial knowledge quietly stops admitting anything. Nil means no
	// threshold applies — a single worker reporting on itself has heard from every worker there is.
	StaleAfterSeconds *float64 `json:"staleAfterSeconds"`

	Phase      Phase       `json:"phase"`
	Conditions []Condition `json:"conditions,omitempty"`

	// StoppingSince and DrainDeadline are present only in [PhaseDraining]. Drained and drain-timeout
	// are DISTINCT events, because the second means records may replay.
	StoppingSince *time.Time `json:"stoppingSince,omitempty"`
	DrainDeadline *time.Time `json:"drainDeadline,omitempty"`

	Negotiated Negotiated `json:"negotiated"`

	Throughput Throughput     `json:"throughput"`
	Nodes      []NodeStatus   `json:"nodes,omitempty"`
	Lanes      []LaneStatus   `json:"lanes,omitempty"`
	Buffers    []BufferStatus `json:"buffers,omitempty"`
	Workers    []WorkerStatus `json:"workers,omitempty"`

	// Streams is the per-stream rollup, and it is what makes a large pipeline answerable at all.
	//
	// ScanProgress used to be the only rollup in this document, so a UI asking "which of my 900
	// tables is behind" had to download every lane to find out. Streams is bounded by the number of
	// STREAMS rather than by the number of lanes, which is the difference between 900 rows and the
	// 29,000 a 900-table snapshot at 32-way chunking produces.
	Streams []StreamStatus `json:"streams,omitempty"`

	// LaneCount is how many lanes EXIST; Lanes carries at most one page of them and LanesTruncated
	// says so. A source with 900 streams or 10^5 scan chunks makes an unpaginated lane array the
	// largest thing in the document and the slowest thing in the UI, and a silently short array is
	// the same lie as a status document that omits a worker.
	LaneCount      int  `json:"laneCount"`
	LanesTruncated bool `json:"lanesTruncated"`

	// LanesCursor continues the lane list, and it is empty when this page is the last.
	//
	// OPAQUE ON PURPOSE. It is a keyset today — the id of the last lane on this page — and nothing
	// outside the producer may parse it, so the implementation can become an index, a shard key or a
	// per-worker fan-out cursor without a wire change. Feed it back as [StatusQuery.LaneCursor].
	LanesCursor string `json:"lanesCursor,omitempty"`

	// Scan is nil when no scan lane exists, so the UI stops showing a scan bar without anything
	// switching on a phase.
	Scan *ScanProgress `json:"scan"`

	RecentEvents []Event    `json:"recentEvents,omitempty"`
	LastFault    *FaultInfo `json:"lastFault"`

	// Config is the REDACTED config tree. It is the only form that ever leaves the process.
	Config map[string]any `json:"config,omitempty"`
}

// StatusQuery selects which part of the read model to materialise.
//
// IT EXISTS BEFORE PAGINATION DOES, and that is the whole point. PipelineStatus is a pinned wire
// contract with an ETag/SSE protocol built on Version; splitting the document later would change the
// document, the stream and [store.StatusStore] at the same moment. Deciding the SHAPE now costs a
// struct, and every implementation that arrives later fits inside it.
//
// The zero query is the default view and is what a scrape or a first page asks for.
type StatusQuery struct {
	// Stream narrows Lanes to one stream. It is the drill-down half of the Streams rollup: read the
	// rollup to find which of 900 tables is behind, then ask for that one table's lanes.
	Stream record.StreamName `json:"stream,omitempty"`

	// LaneCursor continues from a previous page's [PipelineStatus.LanesCursor]. Opaque; a caller
	// echoes it back and never constructs one.
	LaneCursor string `json:"laneCursor,omitempty"`

	// LaneLimit is how many lanes to return. Nil is the producer's default; 0 asks for NONE, which is
	// what a health banner wants — it needs the phase, the conditions and the rollup, and downloading
	// lanes to render none of them is the cost this field removes.
	//
	// A pointer because nil and 0 are different requests, which is this package's rule everywhere
	// else and is exactly the distinction a plain int cannot make.
	LaneLimit *int `json:"laneLimit,omitempty"`
}

// StreamStatus is one stream's lanes, rolled up.
//
// AGGREGATION IS BY MAX FOR AGES AND BY SUM FOR COUNTS, and the choice is not cosmetic: the alert on
// a stream is its WORST lane, so an average would hide one stuck chunk behind thirty-one healthy
// ones. Every pointer here is nil when no lane could answer, never zero.
type StreamStatus struct {
	Stream record.StreamName `json:"stream"`

	Lanes         int `json:"lanes"`
	LanesFinished int `json:"lanesFinished"`
	LanesBlocked  int `json:"lanesBlocked"`

	// LanesIdle counts the lanes a source has REPORTED quiet, which is what lets a rollup distinguish
	// a stream with nothing to say from one that is stuck.
	LanesIdle int `json:"lanesIdle"`

	RecordsRead      uint64 `json:"recordsRead"`
	RecordsCommitted uint64 `json:"recordsCommitted"`
	RecordsAbandoned uint64 `json:"recordsAbandoned"`
	InFlight         uint64 `json:"inFlight"`

	// MaxCheckpointAge is the oldest durable cursor among this stream's lanes: the number an alert
	// fires on. MaxEventTimeLag is the same idea for event time.
	MaxCheckpointAge *float64 `json:"maxCheckpointAgeSeconds"`
	MaxEventTimeLag  *float64 `json:"maxEventTimeLagSeconds"`

	// Backlog is summed across the stream's lanes, and only when EVERY lane could answer. A partial
	// sum reads as a small backlog rather than as an unknown one, which is the more dangerous of the
	// two mistakes.
	Backlog *Backlog `json:"backlog"`
}

// Throughput is the pipeline-level rate summary. Every field is a pointer because a pipeline that has
// not run long enough to have a rate must report unknown, not zero.
type Throughput struct {
	RecordsPerSecondIn  *float64 `json:"recordsPerSecondIn"`
	RecordsPerSecondOut *float64 `json:"recordsPerSecondOut"`
	BytesPerSecondOut   *float64 `json:"bytesPerSecondOut"`

	// ReconcileDelta is records in minus records out for the last checkpoint. A persistent divergence
	// is the only cheap way to notice a sink that silently drops, and it is CHECKED, not merely
	// recorded.
	ReconcileDelta *int64 `json:"reconcileDelta"`
}

// NodeStatus is the per-node view. Node ids are metric labels, which is what makes a fan-out branch
// nameable at all.
type NodeStatus struct {
	ID    record.NodeID `json:"id"`
	Kind  string        `json:"kind"`
	Name  string        `json:"name"`
	Label string        `json:"label,omitempty"`

	Connected bool `json:"connected"`

	RecordsIn  uint64 `json:"recordsIn"`
	RecordsOut uint64 `json:"recordsOut"`
	Faults     uint64 `json:"faults"`

	// Utilization is the fraction of wall time this node spent doing work rather than waiting. It is
	// the bottleneck finder, and it is nil until enough samples exist.
	Utilization *float64 `json:"utilization"`

	// BlockedForSeconds is hard blocking: the node could not hand work downstream. A buffer filling is
	// SOFT and is reported as depth, because "waiting for a buffer" and "blocked on the downstream"
	// are different diagnoses.
	BlockedForSeconds *float64 `json:"blockedForSeconds"`

	// BackoffSeconds is cumulative TIME spent backing off, not a retry count: a count says retries
	// happened, seconds says the pipeline spends most of its life backing off.
	BackoffSeconds *float64 `json:"backoffSeconds"`
}

// LaneStatus is the per-lane view, and it is why this design's observability is not a compromise:
// every field is derived from core-owned ledger and store state plus TWO connector-authored display
// strings.
type LaneStatus struct {
	ID     record.LaneID `json:"id"`
	Name   string        `json:"name"`
	Stream string        `json:"stream"`

	// Kind is reporting only. Nothing in the core branches on it.
	Kind  string `json:"kind"`
	Group string `json:"group,omitempty"`

	// Label is connector-authored and rendered verbatim.
	Label string `json:"label"`

	Worker string `json:"worker"`
	Epoch  uint64 `json:"epoch"`

	// GatedOn is non-empty while this lane is waiting on a lane group, so "why is nothing happening"
	// answers itself.
	GatedOn []string `json:"gatedOn,omitempty"`

	// Position is the DURABLE cursor's label, verbatim. Resolved is the delivered prefix's. Two
	// fields for two facts: the delivered prefix is not progress until it is persisted.
	Position *string `json:"position"`
	Resolved *string `json:"resolved"`

	CommittedAt *time.Time `json:"committedAt"`

	// CheckpointAge is the primary alert signal and the one always-available, unfakeable metric that
	// catches every stall mode.
	CheckpointAge *float64 `json:"checkpointAgeSeconds"`

	// Idle and IdleSince say that the source has REPORTED this lane quiet through
	// connector.Heartbeater, as distinct from the lane being stuck.
	//
	// Without them, hundreds of healthy quiet streams each reported a forever-rising CheckpointAge —
	// the design's own primary alert signal — for the sole offence of having nothing to say, and the
	// only way to keep the signal usable was to stop believing it. An idle lane's CheckpointAge is
	// still reported truthfully; Idle is what tells the alert rule to ignore it.
	Idle      bool       `json:"idle"`
	IdleSince *time.Time `json:"idleSince,omitempty"`

	RecordsRead      uint64 `json:"recordsRead"`
	RecordsCommitted uint64 `json:"recordsCommitted"`
	RecordsAbandoned uint64 `json:"recordsAbandoned"`

	InFlight       uint64 `json:"inFlight"`
	InFlightBudget uint64 `json:"inFlightBudget"`

	// ReplayRecords is the MEASURED worst-case re-read on crash: records admitted since the last
	// durable safe position. It is computed, never the budget under another name.
	ReplayRecords uint64 `json:"replayRecords"`

	Blocked          bool     `json:"blocked"`
	BlockedFor       *float64 `json:"blockedForSeconds"`
	OldestPendingAge *float64 `json:"oldestPendingAgeSeconds"`

	// Backlog is nil when the source cannot report it. Nil renders as "unknown", never as zero.
	Backlog *Backlog `json:"backlog"`

	EventTimeLag *float64 `json:"eventTimeLagSeconds"`

	// Progress comes from the position's scalar projection, when the connector supplies one.
	Progress *float64 `json:"progress"`

	Finished bool `json:"finished"`
}

// Backlog is the read-model projection of what a source reported.
//
// Records and Bytes are POINTERS for the same reason connector.Backlog's are: nil is "the source
// cannot answer" and 0 is "caught up", and the read model's own rule is that every unknown is a nil
// pointer and never a zero.
type Backlog struct {
	Records *uint64 `json:"records"`
	Bytes   *uint64 `json:"bytes"`

	// Exact distinguishes a count from an estimate. It is its own field and never a label, because a
	// label would split the series whenever the source changed strategy.
	Exact bool      `json:"exact"`
	AsOf  time.Time `json:"asOf"`
}

// BufferStatus is the per-buffer-node view.
type BufferStatus struct {
	Node record.NodeID `json:"node"`
	Name string        `json:"name"`

	// Durability is the declared DOMAIN token, so the read model discloses that a node-local buffer
	// cannot authorise a global commit.
	Durability string `json:"durability"`

	Records        int   `json:"records"`
	RecordCapacity int   `json:"recordCapacity"`
	Bytes          int64 `json:"bytes"`
	ByteCapacity   int64 `json:"byteCapacity"`

	OldestAgeSeconds *float64 `json:"oldestAgeSeconds"`
	RefusedTotal     uint64   `json:"refusedTotal"`
}

// WorkerStatus is one worker's membership and lease state.
type WorkerStatus struct {
	ID    string    `json:"id"`
	Since time.Time `json:"since"`

	// Leader is planning-only. Leadership is NEVER trusted for correctness: the lease epoch is the
	// fencing token.
	Leader bool `json:"leader"`

	Lanes        int        `json:"lanes"`
	LastHeard    time.Time  `json:"lastHeard"`
	LeaseExpires *time.Time `json:"leaseExpires"`
}

// ScanProgress summarises every scan lane. It is computed by the core from lane weights and finished
// counts, for any source, with no connector code.
type ScanProgress struct {
	LanesTotal    int `json:"lanesTotal"`
	LanesFinished int `json:"lanesFinished"`

	// Fraction is nil unless enough lanes declared a weight. A progress bar that guesses is worse than
	// no progress bar.
	Fraction *float64 `json:"fraction"`

	StartedAt time.Time  `json:"startedAt"`
	ETA       *time.Time `json:"eta"`
}

// Event is the read model's projection of a connector or engine event.
type Event struct {
	At       time.Time     `json:"at"`
	Kind     string        `json:"kind"`
	Severity string        `json:"severity,omitempty"`
	Node     record.NodeID `json:"node,omitempty"`
	Lane     record.LaneID `json:"lane,omitempty"`
	Stream   string        `json:"stream,omitempty"`
	Message  string        `json:"message"`
	Detail   string        `json:"detail,omitempty"`
}

// FaultInfo is the last fault, rendered for an operator.
//
// Class, Blame and User answer "what happened, whose problem is it, and what do I do" — which is the
// whole of what a status page owes an operator about a failure.
type FaultInfo struct {
	At    time.Time `json:"at"`
	Class string    `json:"class"`
	Blame string    `json:"blame"`
	Op    string    `json:"op"`

	Node   record.NodeID `json:"node,omitempty"`
	Lane   record.LaneID `json:"lane,omitempty"`
	Stream string        `json:"stream,omitempty"`

	// User is operator-facing and carries no stack trace and no Go type. Dev is for the log.
	User string `json:"user"`
	Dev  string `json:"dev,omitempty"`

	Attempts int `json:"attempts"`
}

// NewFaultInfo projects a fault into the read model, mapping its class and blame to their stable
// tokens. It lives here so the projection exists exactly once.
func NewFaultInfo(at time.Time, f *fault.Fault, attempts int) *FaultInfo {
	if f == nil {
		return nil
	}
	return &FaultInfo{
		At:       at,
		Class:    f.Class.String(),
		Blame:    f.Class.Blames().String(),
		Op:       f.Op.String(),
		Node:     f.Node,
		Lane:     f.Lane,
		Stream:   string(f.Stream),
		User:     f.User,
		Dev:      f.Dev,
		Attempts: attempts,
	}
}
