package connector

// Guarantee is the delivery tier, and the tier IS which interfaces the connector
// implements:
//
//	Sink                   -> AtLeastOnce: advance the prefix on a clean WriteResult
//	+ WriterState          -> AtLeastOnce, with resumable in-progress work
//	+ Committer            -> ExactlyOnce by two-phase commit
//	+ TokenSink            -> ExactlyOnce in one durability domain
//
// The negotiated guarantee is min(source, sink, buffer, requested), computed by a pure
// function of config before anything starts, surfaced with per-factor reasons, and
// refused outright when the request is impossible.
//
// It lives in this package rather than in spec because [Opening] carries it: a sink is
// told which tier it is running at so that a sink requiring upsert semantics and handed
// append-only mode can fail Open loudly rather than write wrong data. spec imports
// connector, so defining it here keeps one Go spelling for one concept (design rule R9)
// with no cycle and no cross-map.
type Guarantee uint8

const (
	// AtMostOnce settles on hand-over. Fast, lossy on crash, EXPLICITLY chosen and never
	// a silent degradation.
	AtMostOnce Guarantee = iota

	// AtLeastOnce settles on sink durability. Duplicates on crash are bounded by the lane
	// budget, counted, and disclosed in the read model.
	AtLeastOnce

	// EffectivelyOnce is AtLeastOnce plus an idempotent sink plus stable keys, so
	// duplicates are absorbed at the destination.
	EffectivelyOnce

	// ExactlyOnce requires [Committer] or [TokenSink]. The core refuses to name anything
	// else exactly-once.
	ExactlyOnce
)

var guaranteeNames = [...]string{
	AtMostOnce:      "at_most_once",
	AtLeastOnce:     "at_least_once",
	EffectivelyOnce: "effectively_once",
	ExactlyOnce:     "exactly_once",
}

// String returns the stable snake_case token for g. It is simultaneously the wire form,
// the metric label value and the i18n key suffix.
func (g Guarantee) String() string {
	if int(g) < len(guaranteeNames) {
		return guaranteeNames[g]
	}
	return "at_most_once"
}

// Min returns the weaker of g and h. The negotiated tier is a fold of Min over every
// factor, which is why "what did I actually get" is a computation rather than a hope.
func (g Guarantee) Min(h Guarantee) Guarantee {
	if h < g {
		return h
	}
	return g
}
