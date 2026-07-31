package telemetry

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Negotiated is the resolved, honest answer to "what did I actually get", surfaced in the read model
// and on the submit screen so an operator sees what they GOT rather than what they asked for.
//
// It is defined here, in telemetry, because telemetry owns the read model's JSON contract and must not
// import the engine. The engine returns this type from Build rather than defining a parallel one: two
// structs of one concept would need a mapping function, and a function mapping between two
// representations of the same concept is evidence of a modelling error (design rule R9).
type Negotiated struct {
	// Guarantee is the PIPELINE-WIDE summary: the weakest progress-bearing branch's tier. It is
	// derived from Nodes so that the headline and the detail cannot disagree.
	Guarantee connector.Guarantee `json:"guarantee"`

	// Nodes is the per-branch contract, and it is the honest answer for any graph that is not a
	// straight line.
	//
	// A fan-out to an exactly-once warehouse plus an at-least-once queue has TWO true answers and one
	// summary. Reporting only the summary told the operator who asked for exactly-once that they had
	// not got it, on the branch where they had.
	Nodes map[record.NodeID]NodeContract `json:"nodes,omitempty"`

	// Why is one sentence per factor: "sink stdout is not idempotent",
	// "node warehouse: min(requested=exactly_once, achievable=exactly_once)".
	Why []string `json:"why,omitempty"`

	// DurabilityEdge and AckPoint summarise the WEAKEST branch's answer; the per-node values are in
	// Nodes.
	//
	// They were single-valued strings assigned inside a map-range loop over the sinks, so the
	// disclosed answer depended on Go's map iteration order — 200 identical Builds of one graph
	// produced four durability_edge values and two ack_point values. Deriving them from a named branch
	// makes them deterministic.
	DurabilityEdge string `json:"durability_edge"`

	// AckPoint is which sink interface earns the ack: "write", "flush", "commit" or "token".
	AckPoint string `json:"ack_point"`

	// ReplayBudget is the CONFIGURED in-flight bound, labelled as the configured worst case rather
	// than as a measurement. The measured replay window is a separate, computed series; exporting the
	// budget as if it were the measurement is a config value dressed up as an observation.
	ReplayBudget int `json:"replay_budget"`

	// Defaults labels every value the core supplied rather than the operator (design rule R10).
	Defaults []DefaultNote `json:"defaults,omitempty"`

	// Downgrades lists operator-acknowledged waivers in force.
	Downgrades []Downgrade `json:"downgrades,omitempty"`
}

// NodeContract is one branch's negotiated answer.
type NodeContract struct {
	Guarantee connector.Guarantee `json:"guarantee"`

	// AckPoint is which sink interface earns the ack on this branch: "write", "flush", "commit" or
	// "token". DurabilityEdge names where — "sink:warehouse", "buffer:wal".
	AckPoint       string `json:"ack_point"`
	DurabilityEdge string `json:"durability_edge"`

	// BestEffort is true when every inbound edge of this node is declared spec.Edge.BestEffort, so the
	// branch bears no progress and cannot lower the pipeline's guarantee. It is disclosed because "this
	// branch may silently drop by design" is exactly the fact an operator must be able to see.
	BestEffort bool `json:"best_effort,omitempty"`
}

// DefaultNote records one value the operator did not choose, and who chose it.
type DefaultNote struct {
	Path  []string `json:"path,omitempty"`
	Value any      `json:"value"`

	// From is "core default", "sink declared" or "connector spec".
	From string `json:"from"`
}

// Downgrade is an OPERATOR-SIGNED, DURABLE, CONFIG-DECLARED waiver that lets a pipeline run at a
// weaker guarantee than requested.
//
// The core can never mint one itself, it is recorded with actor and time, and it raises
// [CondDegraded] true for the pipeline's whole life. This is the correct alternative to silent
// capability degradation: the operator says "yes, I know, run it anyway", once, visibly, and the UI
// never stops saying so.
type Downgrade struct {
	Requested string `json:"requested"`
	Effective string `json:"effective"`

	// Missing names the capabilities whose absence caused the downgrade, as their stable tokens.
	Missing []string `json:"missing,omitempty"`

	Node record.NodeID `json:"node"`

	AcknowledgedBy string    `json:"acknowledged_by"`
	AcknowledgedAt time.Time `json:"acknowledged_at"`
	Reason         string    `json:"reason"`
}
