package connector

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Event is a bounded, ordered, operator-visible note.
//
// Drift, schema change and lane lifecycle are EVENTS, not log lines and not metrics: a log
// line is unqueryable from a UI and a metric cannot carry a message. The read model keeps a
// bounded ring of the most recent ones.
type Event struct {
	At   time.Time `json:"at"`
	Kind EventKind `json:"kind"`

	// Severity reuses the fault class vocabulary rather than inventing a second one (design
	// rule R9). Zero means the event is not a failure.
	Severity fault.Class `json:"severity,omitempty"`

	Stream record.StreamName `json:"stream,omitempty"`
	Lane   record.LaneID     `json:"lane,omitempty"`

	Message string `json:"message"`
	Detail  string `json:"detail,omitempty"`
}

// EventKind is the closed event vocabulary.
type EventKind uint8

const (
	EventNote EventKind = iota + 1
	EventSchemaChange
	EventDrift
	EventLaneAnnounced
	EventLaneFinished
	EventLaneRevoked
	EventDegraded
	EventRecovered
	// EventDowngrade records that a pipeline is running under an operator-signed waiver at a
	// weaker guarantee than requested.
	EventDowngrade
)

var eventKindNames = map[EventKind]string{
	EventNote:          "note",
	EventSchemaChange:  "schema_change",
	EventDrift:         "drift",
	EventLaneAnnounced: "lane_announced",
	EventLaneFinished:  "lane_finished",
	EventLaneRevoked:   "lane_revoked",
	EventDegraded:      "degraded",
	EventRecovered:     "recovered",
	EventDowngrade:     "downgrade",
}

// String returns the stable snake_case token for k.
func (k EventKind) String() string {
	if s, ok := eventKindNames[k]; ok {
		return s
	}
	return "note"
}
