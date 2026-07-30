package connector

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// APIVersion is the connector-surface contract version this build of the core implements.
// A component declaring a version outside [MinAPIVersion, APIVersion] is refused at
// registration with a message naming both numbers.
const APIVersion = 1

// MinAPIVersion is the oldest connector-surface contract version this core still accepts.
const MinAPIVersion = 1

// Caps is embedded in every component kind's capability struct. It is the versioning and
// forward-compatibility contract, and every registered kind has one — transforms, buffers,
// codecs, framers and compressors included, not only sources and sinks.
type Caps struct {
	// APIVersion is the connector-surface contract version the component was written
	// against.
	//
	// Its absence is a fatal defect: without it, a semantic change to a frozen interface is
	// undetectable in-process and uncatchable across a future RPC boundary.
	APIVersion int `json:"api_version"`

	// Unknown holds capability names the core did not recognise, populated only for an
	// out-of-process component that declared something newer than this core.
	//
	// THE RULE: an unknown capability is IGNORED and REPORTED, never an error. Anything else
	// makes a newer connector unusable by an older core, which is the downgrade path nobody
	// tests.
	Unknown []string `json:"unknown,omitempty"`
}

// SourceCaps is the declarative half of a source's capability set. It serialises, crosses a
// process boundary, is queryable by the registry and the UI without instantiating anything,
// and is checkable at submit time.
type SourceCaps struct {
	Caps

	// DefaultOrdering applies to lanes that do not override it.
	DefaultOrdering Ordering `json:"default_ordering"`

	// Boundedness declares what this source can produce. Required and non-empty.
	Boundedness []Boundedness `json:"boundedness"`

	// LaneKinds declares which kinds this source can announce. Required and non-empty. A
	// source that cannot do a full scan omits [LaneKindScan] and the UI greys out the option
	// rather than failing at three in the morning.
	LaneKinds []LaneKind `json:"lane_kinds"`

	// MaxLanes caps announced lanes; 0 means unlimited. HARD-ENFORCED at announce time for
	// a NEW lane: exceeding it fails the pipeline. An advisory task cap is an eight-year
	// bug waiting to threaten cluster stability.
	//
	// It never fails a RE-announcement of a lane that already exists durably, however low
	// the running binary's cap is. Enforcing the cap against a shared durable lane plan
	// means a rollback to a binary with a narrower cap cannot read the plan it is holding,
	// which turns a rollback into an outage. The cap governs GROWTH; the plan governs
	// recovery.
	//
	// It answers "how many lanes can this source's ALGORITHM manage", not "how many should
	// run at once". A source whose chunk count is data-dependent and reaches 10^4 or 10^5
	// declares 0 and the operator's spec.Spec.Parallelism bounds concurrency, which is the
	// knob an operator can actually see and change.
	MaxLanes int `json:"max_lanes"`

	// UpstreamRetention is THE capability that makes canal's commit protocol safe, and it is
	// the single most important field in this struct.
	//
	// It answers: what happens upstream when this source acts on a Commit? If a guarantee
	// depends on connector semantics, the interface must let the connector declare those
	// semantics. This field is that sentence made real.
	UpstreamRetention Retention `json:"upstream_retention"`

	// ReplayWindow is how far back this source can be resumed from, when it knows. Zero
	// means unknown.
	//
	// At restart the core compares it against the committed cursor's age and REFUSES with
	// "source guarantees 6h; this cursor is 9h old" rather than silently starting a lossy
	// stream.
	ReplayWindow time.Duration `json:"replay_window,omitempty"`

	// UnitAssignment declares who owns the division of work. A source whose upstream already
	// solves assignment declares [UnitsExternal], and the core's planner then announces
	// exactly one lane and lets the upstream rebalance. Without this, a planning core
	// actively fights the broker.
	UnitAssignment UnitAssignment `json:"unit_assignment"`

	// Interface-backed flags. Registration PANICS if a flag is set and the interface is
	// absent. The reverse — implemented but not declared — is a WARNING recorded on the
	// descriptor, never a panic, which is what lets a v2 core add an optional interface
	// without retroactively breaking a v1 connector.
	Discoverable   bool `json:"discoverable"`    // Discoverer
	Nackable       bool `json:"nackable"`        // Nackable
	ReportsBacklog bool `json:"reports_backlog"` // BacklogReporter
	Heartbeats     bool `json:"heartbeats"`      // Heartbeater
	Validates      bool `json:"validates"`       // Validator
	Probes         bool `json:"probes"`          // Prober
	Choices        bool `json:"choices"`         // ChoiceProvider
	AdoptsState    bool `json:"adopts_state"`    // StateAdopter
	ReadsLanes     bool `json:"reads_lanes"`     // LaneReader

	// ReadConcurrency is how many [LaneReader.ReadLanes] calls the core may run at once,
	// each over a DISJOINT set of this instance's lanes. Meaningful only with ReadsLanes.
	//
	// Zero and one both mean one call carrying every held lane, which is what a source
	// multiplexing many streams over one connection wants. N means the core partitions the
	// held lanes into at most N groups and reads them concurrently, which is what a source
	// with N independent connections wants. It is capped by spec.Spec.Parallelism, so the
	// operator's number always wins downward.
	ReadConcurrency int `json:"read_concurrency,omitempty"`

	// Pure declarations with no interface behind them.

	ProducesEventTime bool `json:"produces_event_time"`
	ProducesChange    bool `json:"produces_change"`
	ProducesSchema    bool `json:"produces_schema"`

	// CompleteImages means every Change this source emits has AfterComplete ==
	// record.CompletenessComplete. A sink that requires whole images is refused against a
	// source that does not declare this, which is the only defence in the surveyed field
	// against an upsert writing nulls over live data.
	CompleteImages bool `json:"complete_images"`

	// ComparablePositions means every Position carries Order. Required for mid-lane
	// monotonicity assertions and for any future range filter.
	ComparablePositions bool `json:"comparable_positions"`

	// Replayable means the source can re-read from a committed position. False means a lost
	// in-flight window is lost data, and the core refuses AtLeastOnce — UNLESS
	// RedeliversUnacked is set.
	Replayable bool `json:"replayable"`

	// RedeliversUnacked means the source's PEER re-sends anything this source did not
	// acknowledge, so a lost in-flight window is not lost data even though the source
	// cannot re-read a committed position.
	//
	// This is the push source. An HTTP or gRPC ingress has no cursor and nothing to rewind
	// to, so it must declare Replayable false; the negotiation then clamped the pipeline to
	// AtMostOnce, which settles on HAND-OVER and therefore acknowledges the peer before the
	// data is durable — the one thing a push ingress must never do. The peer's own retry is
	// the replay mechanism, and this field is how the source says so.
	//
	// It is a PROMISE with a precise content: the source will not return success to its
	// peer until the core has settled the records, and the peer will re-send on any other
	// answer. A source declaring it and acknowledging early has lied in exactly the way a
	// sink returning a clean WriteResult before durability has lied, and it is the only
	// other way to violate design rule R4 in this design.
	RedeliversUnacked bool `json:"redelivers_unacked"`

	// StableKeys means Origin.Key is populated and stable across re-reads. Required for
	// EffectivelyOnce, for dedupe, and for Request.IdempotencyKey.
	//
	// Declaring it with empty registration Notes fails registration lint, because a derived
	// key with an undocumented derivation is a key nobody can reason about.
	StableKeys bool `json:"stable_keys"`

	// MidLaneResume means a position that is not at a lane boundary is a legal resume point.
	// False forces the core to commit only at end of lane, which for a bounded lane means
	// all-or-nothing.
	//
	// It is the SOURCE-WIDE declaration, read at submit time when no lane exists yet. A
	// source whose lanes disagree — atomic chunks that cannot resume halfway alongside a
	// tail that can — declares the permissive value here and the restrictive one per lane
	// in LaneSpec.MidLaneResume, which is what the core's commit points actually consult.
	MidLaneResume bool `json:"mid_lane_resume"`

	// UpstreamRetention, Replayable, CompleteImages and StableKeys are declared ONCE PER
	// REGISTERED NAME and cannot be narrowed per configuration, because SourceCaps is
	// queryable without instantiating anything and a per-configuration answer would require
	// construction inside Build.
	//
	// A connector whose answer genuinely differs by configuration — the same driver used
	// once against a pruning replication slot and once against a bounded table — REGISTERS
	// TWO NAMES sharing one implementation. The registry keys capabilities by name, so two
	// names is two honest capability sets and zero duplicated code, and the operator sees
	// two entries whose difference is the thing that actually differs. Over-declaring the
	// strictest value across both uses is the alternative, and it silently refuses
	// pipelines that were safe.
}

// Retention declares what happens upstream when a source acts on a Commit.
type Retention uint8

const (
	// RetentionUnknown is the zero value and is REFUSED at registration. A source that has
	// not thought about this question has not thought about correctness.
	RetentionUnknown Retention = iota

	// PrunesOnCommit means the upstream DISCARDS data when the source commits — a
	// replication slot advancing its flush position frees log for recycling.
	//
	// canal MUST have durably flushed its own record of the position before calling Commit,
	// or a crash inside the window is an unrecoverable gap. One surveyed system shipped the
	// unsafe ordering and it was a confirmed severity-zero defect that survived years,
	// because nothing in the interface distinguished this class of source from the benign
	// one.
	PrunesOnCommit

	// RetentionWindow means the upstream keeps data for a bounded time regardless. Commit
	// ordering is then a latency question, not a correctness one.
	RetentionWindow

	// RetentionUnbounded means the upstream never discards: a file, an object store, a
	// bounded table.
	RetentionUnbounded
)

var retentionNames = [...]string{
	RetentionUnknown:   "unknown",
	PrunesOnCommit:     "prunes_on_commit",
	RetentionWindow:    "retention_window",
	RetentionUnbounded: "unbounded",
}

// String returns the stable snake_case token for r.
func (r Retention) String() string {
	if int(r) < len(retentionNames) {
		return retentionNames[r]
	}
	return "unknown"
}

// UnitAssignment declares who owns the division of work.
type UnitAssignment uint8

const (
	// UnitsStatic means the lane set is fixed once the source has opened. The planner plans
	// once.
	UnitsStatic UnitAssignment = iota
	// UnitsDynamic means the lane set may change at runtime; the planner reconciles.
	UnitsDynamic
	// UnitsExternal means units exist but SOMEONE ELSE assigns them. canal announces one
	// lane per source instance and does not attempt to place work.
	UnitsExternal
)

var unitAssignmentNames = [...]string{
	UnitsStatic:   "static",
	UnitsDynamic:  "dynamic",
	UnitsExternal: "external",
}

// String returns the stable snake_case token for u.
func (u UnitAssignment) String() string {
	if int(u) < len(unitAssignmentNames) {
		return unitAssignmentNames[u]
	}
	return "static"
}

// SinkCaps is the sink half of the declarative capability set.
type SinkCaps struct {
	Caps

	// MaxConcurrency is how many Write calls may be in flight. Required, at least 1.
	MaxConcurrency int `json:"max_concurrency"`

	// MaxRequestRecords and MaxRequestBytes are hard limits the engine's batcher and
	// splitter respect. Zero means no limit.
	MaxRequestRecords int   `json:"max_request_records,omitempty"`
	MaxRequestBytes   int64 `json:"max_request_bytes,omitempty"`

	// Idempotent means re-delivering an identical request is harmless. Required for
	// EffectivelyOnce, and it is what lets the engine retry an Indeterminate write instead
	// of stalling.
	Idempotent bool `json:"idempotent"`

	// PartialFailure means Write may return a non-empty WriteResult.Failed. When false the
	// engine never attempts sub-batch retry and does not have to guess.
	PartialFailure bool `json:"partial_failure"`

	// Modes are the destination modes this sink can honour. Refused at submit time against a
	// configured mode it does not list — so upsert against an append-only destination is a
	// diagnostic, not a corruption.
	Modes []DestMode `json:"modes"`

	// RequiresCompleteImages means this sink writes whole rows and would null out live
	// columns given a partial after-image. Refused against a source that does not declare
	// CompleteImages.
	RequiresCompleteImages bool `json:"requires_complete_images"`

	// RequiresKey means every record must carry Origin.Key.
	RequiresKey bool `json:"requires_key"`

	// SchemaChanges declares which change kinds ApplySchemaChange can perform. Data, not a
	// method, so Build never instantiates a sink to negotiate.
	SchemaChanges []schema.ChangeKind `json:"schema_changes,omitempty"`

	// Interface-backed flags, cross-checked in one direction at registration.
	Flushes       bool `json:"flushes"`        // Flusher
	Structured    bool `json:"structured"`     // StructuredSink
	Partitions    bool `json:"partitions"`     // Partitioner
	AppliesSchema bool `json:"applies_schema"` // SchemaApplier
	Commits       bool `json:"commits"`        // Committer
	KeepsState    bool `json:"keeps_state"`    // WriterState
	StoresToken   bool `json:"stores_token"`   // TokenSink
	Prepares      bool `json:"prepares"`       // Preparer
	Validates     bool `json:"validates"`      // Validator
	Probes        bool `json:"probes"`         // Prober
	Choices       bool `json:"choices"`        // ChoiceProvider
	ResolvesStale bool `json:"resolves_stale"` // StaleResolver
}

// MaxGuarantee reports the strongest tier this sink can support on its own. The negotiated
// tier is the minimum of this, the source's, the buffer's and the request.
func (c SinkCaps) MaxGuarantee() Guarantee {
	switch {
	case c.Commits || c.StoresToken:
		return ExactlyOnce
	case c.Idempotent:
		return EffectivelyOnce
	default:
		return AtLeastOnce
	}
}

// BufferCaps is the buffer half.
type BufferCaps struct {
	Caps

	// Durability is the DOMAIN, not a bool. Only a buffer whose domain is at least as wide
	// as the lane's assignment domain may shorten the ack chain, and the core — not the
	// buffer — does the shortening.
	Durability Durability `json:"durability"`

	// Chains means this buffer can act as the overflow target of another.
	Chains bool `json:"chains"`

	// Bounded must be true. The field exists so the assertion is explicit and so a future
	// unbounded buffer is a registration failure rather than a surprise.
	Bounded bool `json:"bounded"`
}

// TransformCaps is the transform half.
type TransformCaps struct {
	Caps

	// Expands declares a one-to-N shape, so the engine reserves expansion accounting.
	Expands bool `json:"expands"`
	// Filters declares a one-to-zero shape.
	Filters bool `json:"filters"`
	// Regroups declares an N-to-M shape that uses record.Batch.Merge.
	Regroups bool `json:"regroups"`

	// PreservesOrder is false for a transform that reorders. The engine refuses to place it
	// on a lane whose Ordering is prefix unless the pipeline declares it accepts the
	// consequence. A concurrency knob that silently destroys ordering and does not say so at
	// the knob is the failure this field prevents.
	PreservesOrder bool `json:"preserves_order"`

	KeepsState bool `json:"keeps_state"` // StatefulTransform
	Validates  bool `json:"validates"`   // Validator
}
