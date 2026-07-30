package spec

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// DriftPolicy is the five-mode schema-drift set, with never-destructive [DriftLenient] as the
// default.
//
// It is CORE config, not a per-sink decision, because "should I alter this table?" is an
// unanswerable question to land on every sink author. A sink declares which change kinds it can
// perform and nothing more.
type DriftPolicy uint8

const (
	// DriftLenient applies additive changes and NEVER destructive ones: an alter-column-type becomes
	// a rename plus an add, keeping the old column, so no data is lost. The default.
	DriftLenient DriftPolicy = iota
	// DriftEvolve applies everything the sink supports, destructive included.
	DriftEvolve
	// DriftTryEvolve applies what is supported and emits an event for the rest.
	DriftTryEvolve
	// DriftIgnore never applies a change and keeps writing the old shape.
	DriftIgnore
	// DriftFail stops the pipeline on any change.
	DriftFail
)

var driftPolicyNames = [...]string{
	DriftLenient:   "lenient",
	DriftEvolve:    "evolve",
	DriftTryEvolve: "try_evolve",
	DriftIgnore:    "ignore",
	DriftFail:      "fail",
}

// String returns the stable snake_case token for p.
func (p DriftPolicy) String() string {
	if int(p) < len(driftPolicyNames) {
		return driftPolicyNames[p]
	}
	return "lenient"
}

// Permits reports whether p allows a change of the given kind to be applied at all.
//
// [DriftLenient] permits only non-destructive kinds; the engine rewrites a destructive kind into an
// additive pair rather than refusing outright. Truncate and drop are excluded from every policy
// except an explicit [DriftEvolve], and must be opted into.
//
// PERMITS IS NOT A REQUIREMENT ON A SINK, and conflating the two produced a defect worth naming here
// because the shape recurs. Build used to refuse any pipeline whose sink's declared SchemaChanges did
// not cover EVERY kind this method permits — so a sink honestly declaring
// {CreateStream, AddField} was refused, while the SAME sink omitting AppliesSchema built and ran.
// Volunteering a capability was strictly worse than withholding it, which inverts the incentive the
// whole capability model depends on.
//
// Every policy's own definition is already relative to the sink: Evolve "applies everything THE SINK
// SUPPORTS", TryEvolve "applies what is supported and emits an event for the rest", Ignore and Fail
// ask the sink for nothing. So NO policy makes a sink capability a precondition, and the honest
// submit-time output is a WARNING naming the gap plus a runtime EventDrift and a degraded condition
// if the gap is ever actually hit.
func (p DriftPolicy) Permits(k schema.ChangeKind) bool {
	switch p {
	case DriftIgnore, DriftFail:
		return false
	case DriftEvolve:
		return true
	default:
		return !k.Destructive()
	}
}

// ClockPolicy decides what happens to a source timestamp that is implausible relative to the local
// clock. The behaviour is CONFIGURED, never silently chosen.
type ClockPolicy struct {
	// MaxSkew is how far a source timestamp may lead the local clock before Behaviour applies. Zero
	// disables the check entirely.
	MaxSkew time.Duration `json:"max_skew"`

	Behaviour ClockBehaviour `json:"behaviour"`
}

// ClockBehaviour is the closed set of clock-skew responses.
type ClockBehaviour uint8

const (
	// ClockClamp clamps the timestamp to now, records a field change on the record so the adjustment
	// is visible, and counts it. The default.
	ClockClamp ClockBehaviour = iota
	// ClockReject raises a fault.ClockSkew and routes the record on the failed edge.
	ClockReject
	// ClockPass accepts the timestamp verbatim and counts it.
	ClockPass
)

var clockBehaviourNames = [...]string{
	ClockClamp:  "clamp",
	ClockReject: "reject",
	ClockPass:   "pass",
}

// String returns the stable snake_case token for b.
func (b ClockBehaviour) String() string {
	if int(b) < len(clockBehaviourNames) {
		return clockBehaviourNames[b]
	}
	return "clamp"
}
