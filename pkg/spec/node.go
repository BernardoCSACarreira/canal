package spec

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

// Node is one vertex of a pipeline graph.
//
// Adding a node KIND is not a contract change: Kind is a registry kind string, and the validator
// checks it against the registry rather than against a switch statement.
type Node struct {
	ID   record.NodeID `json:"id"`
	Kind registry.Kind `json:"kind"`

	// Name is the registered component name.
	Name string `json:"name"`

	// Label is for display, and it namespaces this node's metrics so a fan-out branch is nameable.
	Label string `json:"label,omitempty"`

	// Config is the node's raw config, validated against the component's declared spec at build
	// time. It includes the stage-standard fields the registry appended.
	Config map[string]any `json:"config,omitempty"`

	// Inputs are the edges feeding this node. A node with no inputs is a source node; a node with no
	// outgoing edge is a terminal node.
	Inputs []Edge `json:"inputs,omitempty"`

	// Guarantee overrides Spec.Guarantee FOR THIS BRANCH. Nil means the pipeline's request.
	//
	// One requested tier per pipeline, folded with Min over every sink, made a mixed-guarantee
	// fan-out inexpressible in BOTH directions. Requesting exactly-once for the warehouse branch
	// was a hard error on each of the other three branches, which implement neither Committer nor
	// TokenSink. Requesting at-least-once so the graph would build then MIS-REPORTED the warehouse,
	// which really is exactly-once, to the operator who asked for it.
	//
	// The negotiated tier is still a fold of Min, per branch, over that branch's own factors: the
	// sources feeding it, the buffers on it and this value. A branch may never exceed what the
	// deployment's store supports, so a per-node override cannot be used to route around the
	// store-capability refusal.
	Guarantee *connector.Guarantee `json:"guarantee,omitempty"`

	// Cadence overrides the deployment's durability cadence for this node. Nil means the
	// deployment's.
	//
	// engine.Deps.FlushInterval drives both Flusher.Flush and Committer.PrepareCommit, and it was
	// pipeline-wide. A graph holding a 30-second-batch warehouse and a 1-millisecond queue has two
	// correct cadences and one field: the warehouse cadence starves the queue of latency, the queue
	// cadence asks the warehouse to seal 30000 tiny files. Whichever number is chosen, one branch is
	// wrong, and neither branch can say so.
	Cadence *Cadence `json:"cadence,omitempty"`
}

// Cadence is one node's durability cadence: how far phase three may lag phase one for it.
type Cadence struct {
	// Interval and Records are the two bounds, whichever comes first. Zero means the deployment's
	// value for that bound rather than "no bound", because a node that disables both would never be
	// flushed and would silently hold data forever.
	Interval time.Duration `json:"interval,omitempty"`
	Records  int           `json:"records,omitempty"`
}

// Edge is a directed connection with a selector.
//
// This is the ENTIRE routing vocabulary: fan-out is two nodes naming one input; fan-in is one node
// naming two inputs; dead-lettering is an edge with [EdgeFailed]; a fallback is a sink node whose
// only input is another sink node's failed edge.
//
// One mechanism, not two. Recursive component-valued composition — a sink whose config contains N
// child sinks — is deliberately NOT used for topology: it needs a child-resolution accessor the
// config layer cannot have without an import cycle, it makes per-branch metric labels and
// per-branch settlement invisible, and it is two representations of one concept (design rule R9).
type Edge struct {
	From   record.NodeID `json:"from"`
	Select EdgeSelect    `json:"select"`

	// BestEffort declares that this branch DOES NOT BEAR PROGRESS: the engine adds no settlement
	// reference for it, so a record lost on this branch never holds the source's cursor and never
	// appears in Ack.Abandoned.
	//
	// It is the operator declaring intent, which nothing else in the graph could express. A fan-out
	// to a warehouse, a queue, a metrics feed and a dead-letter sink has one branch whose loss is a
	// data-loss incident and three whose loss is a Tuesday; without a way to say which, every branch
	// contributed a reference, so a shed metrics record held the prefix of the warehouse's source and
	// a source with a DESTRUCTIVE commit — one that deletes the queue message it read — had to refuse
	// to advance on it. The by-design shed and the real dead-letter were the same uint64.
	//
	// It is deliberately NOT a per-sink capability. A sink cannot know whether the operator considers
	// it load-bearing; the same metrics sink is best-effort in one graph and the whole point of
	// another. Faults on a best-effort branch are still counted, still classified and still visible
	// as EventDegraded — unobserved is not the same as unimportant.
	BestEffort bool `json:"best_effort,omitempty"`
}

// EdgeSelect is which records an edge carries.
type EdgeSelect uint8

const (
	// EdgeMain carries records with no attached fault.
	EdgeMain EdgeSelect = iota

	// EdgeFailed carries records that reached a terminal disposition, with their fault, provenance,
	// attempt count and the redacted config revision attached.
	//
	// It works for SOURCE-side failures too, which a sink-only error reporter cannot. The original
	// record is carried in an ENVELOPE, never re-nested inside a payload field: a design that nests
	// needs unwrap processors whose only job is to undo the nesting.
	EdgeFailed

	// EdgeAll carries both. Used by a tap or an audit sink.
	EdgeAll
)

var edgeSelectNames = [...]string{
	EdgeMain:   "main",
	EdgeFailed: "failed",
	EdgeAll:    "all",
}

// String returns the stable snake_case token for s.
func (s EdgeSelect) String() string {
	if int(s) < len(edgeSelectNames) {
		return edgeSelectNames[s]
	}
	return "main"
}

// CarriesFailed reports whether s routes faulted records. Retry policy validation uses it to check
// that a dead-letter terminal has somewhere to go.
func (s EdgeSelect) CarriesFailed() bool { return s == EdgeFailed || s == EdgeAll }

// CarriesMain reports whether s routes clean records.
func (s EdgeSelect) CarriesMain() bool { return s == EdgeMain || s == EdgeAll }
