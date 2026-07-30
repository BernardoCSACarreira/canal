package ledger

import (
	"errors"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// ErrTrackerClosed is returned by Track after Close. It is a fault so that it classifies like every
// other error the engine handles.
var ErrTrackerClosed = fault.New(fault.TransientInternal, fault.OpBuffer, errors.New("ledger: tracker is closed"))

// Disposition is TERMINAL ONLY.
//
// There is deliberately no non-terminal "failed" member: a retryable failure is not a settlement, it is
// a retry, tracked by the engine's retry loop and invisible to the ledger. Including a non-terminal
// disposition makes settlement admit both a deadlock reading and a loss reading simultaneously.
//
// Note that this is a DIFFERENT concept from connector.Disposition, which is the outcome set for a
// two-phase committable. One is "what happened to this record's settlement", the other is "what
// happened to this staged artifact". They are genuinely different questions, and this one is internal
// so a connector never sees both.
type Disposition uint8

const (
	// Delivered means durable at the sink, or in a buffer whose durability domain the core validated as
	// wide enough to shorten the acknowledgement chain.
	Delivered Disposition = iota + 1

	// Duplicate means the sink already had it. It counts as delivered — that is the whole point of an
	// idempotent write — and is counted separately so the rate is visible.
	Duplicate

	// Abandoned is terminal non-delivery: dead-lettered or dropped. It advances the prefix and makes the
	// acknowledgement's abandoned count non-zero, so the source is TOLD and can decide.
	Abandoned
)

var dispositionNames = map[Disposition]string{
	Delivered: "delivered",
	Duplicate: "duplicate",
	Abandoned: "abandoned",
}

// String returns the stable snake_case token for d.
func (d Disposition) String() string {
	if s, ok := dispositionNames[d]; ok {
		return s
	}
	return "abandoned"
}

// Outcome is one record's terminal settlement.
type Outcome struct {
	Record      record.RecordID
	Disposition Disposition

	// Node is the sink node that produced this outcome. It is what lets an acknowledgement attribute
	// abandonments per branch, so a fan-out's by-design best-effort shed is distinguishable from its
	// warehouse branch dead-lettering.
	Node record.NodeID

	// Fault is set for [Abandoned] and carries the classified reason, which becomes the metric's reason
	// label and the dead-letter envelope's explanation.
	Fault *fault.Fault
}

// Leak is a group that exceeded the configured time-to-live without settling.
//
// Rust's ownership gives one surveyed system this safety for free: dropping an unsettled finaliser
// resolves the batch as not-delivered, the safe direction. Go has no such hook, so canal uses a reaper —
// and the reaper is strictly BETTER, because it turns "someone forgot to settle" from a silent stall
// into a named condition with the offending node, lane and group.
type Leak struct {
	Group   record.GroupID
	Lane    record.LaneID
	Node    record.NodeID
	Age     time.Duration
	Records int
}
