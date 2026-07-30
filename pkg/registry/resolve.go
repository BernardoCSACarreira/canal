package registry

import (
	"github.com/BernardoCSACarreira/canal/pkg/connector"
)

// ResolvedSource is a source with every optional capability snapshotted into a plain struct of
// nilable handles at one well-defined moment.
//
// This answers "an optional interface is hostile to a UI and to a wire". A UI cannot type-assert; a
// gRPC adapter cannot either. So the core collapses the capability set into a struct, once, and
// everything downstream — the engine, the API, the frontend, the conformance kit — reads only
// fields.
//
// [ResolveSource] is called in EXACTLY ONE PLACE in the whole codebase. A design that type-asserts
// at every wrapper pays a hand-written forwarder per capability per wrapper; canal pays one
// function, and the engine nil-checks a field rather than re-asserting, so adding a capability
// touches one file.
type ResolvedSource struct {
	Source connector.Source
	Name   string
	Caps   connector.SourceCaps

	// Every field is nil when the capability is absent OR undeclared. The DECLARATION governs:
	// implementing an interface without declaring it leaves the handle nil and is reported as
	// [CapUndeclared], because the engine must behave identically in-process and over a wire, and a
	// wire only ever sees the declaration.
	Discoverer connector.Discoverer
	Nackable   connector.Nackable
	Backlog    connector.BacklogReporter
	Heartbeat  connector.Heartbeater
	Validator  connector.Validator
	Prober     connector.Prober
	Choices    connector.ChoiceProvider
	Adopter    connector.StateAdopter
	LaneReader connector.LaneReader

	Report []CapReport
}

// ReadConcurrency is how many concurrent ReadLanes calls the engine may make, bounded by the
// operator's parallelism. It is a method here rather than a bare field read so that the "zero means
// one" rule has one implementation.
func (r *ResolvedSource) ReadConcurrency(parallelism int) int {
	if r.LaneReader == nil {
		return 1
	}
	n := r.Caps.ReadConcurrency
	if n < 1 {
		n = 1
	}
	if parallelism > 0 && n > parallelism {
		n = parallelism
	}
	return n
}

// ResolveSource snapshots s's optional capabilities against what c declares.
//
// It returns an error only for an inconsistency the registry could not already have caught — chiefly
// a remotely-supplied Caps whose declarations do not match the adapter's method set. For an
// in-process component the registry has already panicked at init on any over-declaration, so this
// path is quiet by the time it runs.
func ResolveSource(name string, s connector.Source, c connector.SourceCaps) (*ResolvedSource, error) {
	out := &ResolvedSource{Source: s, Name: name, Caps: c}

	checks := []capCheck{
		{name: "discoverable", title: "cap.discoverable", iface: "connector.Discoverer", declared: c.Discoverable},
		{name: "nackable", title: "cap.nackable", iface: "connector.Nackable", declared: c.Nackable},
		{name: "reports_backlog", title: "cap.reports_backlog", iface: "connector.BacklogReporter", declared: c.ReportsBacklog},
		{name: "heartbeats", title: "cap.heartbeats", iface: "connector.Heartbeater", declared: c.Heartbeats},
		{name: "validates", title: "cap.validates", iface: "connector.Validator", declared: c.Validates},
		{name: "probes", title: "cap.probes", iface: "connector.Prober", declared: c.Probes},
		{name: "choices", title: "cap.choices", iface: "connector.ChoiceProvider", declared: c.Choices},
		{name: "adopts_state", title: "cap.adopts_state", iface: "connector.StateAdopter", declared: c.AdoptsState},
		{name: "reads_lanes", title: "cap.reads_lanes", iface: "connector.LaneReader", declared: c.ReadsLanes},
	}

	// The single type-assertion site in the whole codebase.
	if v, ok := s.(connector.Discoverer); ok {
		checks[0].implemented = true
		if c.Discoverable {
			out.Discoverer = v
		}
	}
	if v, ok := s.(connector.Nackable); ok {
		checks[1].implemented = true
		if c.Nackable {
			out.Nackable = v
		}
	}
	if v, ok := s.(connector.BacklogReporter); ok {
		checks[2].implemented = true
		if c.ReportsBacklog {
			out.Backlog = v
		}
	}
	if v, ok := s.(connector.Heartbeater); ok {
		checks[3].implemented = true
		if c.Heartbeats {
			out.Heartbeat = v
		}
	}
	if v, ok := s.(connector.Validator); ok {
		checks[4].implemented = true
		if c.Validates {
			out.Validator = v
		}
	}
	if v, ok := s.(connector.Prober); ok {
		checks[5].implemented = true
		if c.Probes {
			out.Prober = v
		}
	}
	if v, ok := s.(connector.ChoiceProvider); ok {
		checks[6].implemented = true
		if c.Choices {
			out.Choices = v
		}
	}
	if v, ok := s.(connector.StateAdopter); ok {
		checks[7].implemented = true
		if c.AdoptsState {
			out.Adopter = v
		}
	}
	if v, ok := s.(connector.LaneReader); ok {
		checks[8].implemented = true
		if c.ReadsLanes {
			out.LaneReader = v
		}
	}

	rep, _, err := report(KindSource, name, checks, c.Unknown)
	out.Report = rep
	return out, err
}

// ResolvedSink is the sink half of the one type-assertion site.
type ResolvedSink struct {
	Sink connector.Sink
	Name string
	Caps connector.SinkCaps

	Flusher    connector.Flusher
	Structured connector.StructuredSink
	Partition  connector.Partitioner
	SchemaApp  connector.SchemaApplier
	Committer  connector.Committer
	State      connector.WriterState
	Token      connector.TokenSink
	Preparer   connector.Preparer
	Validator  connector.Validator
	Prober     connector.Prober
	Choices    connector.ChoiceProvider

	// Stale is preferred over Committer.AbortStale whenever it is present, because a per-item answer
	// strictly dominates one naked error for a batch.
	Stale connector.StaleResolver

	Report []CapReport
}

// ResolveSink snapshots k's optional capabilities against what c declares.
func ResolveSink(name string, k connector.Sink, c connector.SinkCaps) (*ResolvedSink, error) {
	out := &ResolvedSink{Sink: k, Name: name, Caps: c}

	checks := []capCheck{
		{name: "flushes", title: "cap.flushes", iface: "connector.Flusher", declared: c.Flushes},
		{name: "structured", title: "cap.structured", iface: "connector.StructuredSink", declared: c.Structured},
		{name: "partitions", title: "cap.partitions", iface: "connector.Partitioner", declared: c.Partitions},
		{name: "applies_schema", title: "cap.applies_schema", iface: "connector.SchemaApplier", declared: c.AppliesSchema},
		{name: "commits", title: "cap.commits", iface: "connector.Committer", declared: c.Commits},
		{name: "keeps_state", title: "cap.keeps_state", iface: "connector.WriterState", declared: c.KeepsState},
		{name: "stores_token", title: "cap.stores_token", iface: "connector.TokenSink", declared: c.StoresToken},
		{name: "prepares", title: "cap.prepares", iface: "connector.Preparer", declared: c.Prepares},
		{name: "validates", title: "cap.validates", iface: "connector.Validator", declared: c.Validates},
		{name: "probes", title: "cap.probes", iface: "connector.Prober", declared: c.Probes},
		{name: "choices", title: "cap.choices", iface: "connector.ChoiceProvider", declared: c.Choices},
		{name: "resolves_stale", title: "cap.resolves_stale", iface: "connector.StaleResolver", declared: c.ResolvesStale},
	}

	if v, ok := k.(connector.Flusher); ok {
		checks[0].implemented = true
		if c.Flushes {
			out.Flusher = v
		}
	}
	if v, ok := k.(connector.StructuredSink); ok {
		checks[1].implemented = true
		if c.Structured {
			out.Structured = v
		}
	}
	if v, ok := k.(connector.Partitioner); ok {
		checks[2].implemented = true
		if c.Partitions {
			out.Partition = v
		}
	}
	if v, ok := k.(connector.SchemaApplier); ok {
		checks[3].implemented = true
		if c.AppliesSchema {
			out.SchemaApp = v
		}
	}
	if v, ok := k.(connector.Committer); ok {
		checks[4].implemented = true
		if c.Commits {
			out.Committer = v
		}
	}
	if v, ok := k.(connector.WriterState); ok {
		checks[5].implemented = true
		if c.KeepsState {
			out.State = v
		}
	}
	if v, ok := k.(connector.TokenSink); ok {
		checks[6].implemented = true
		if c.StoresToken {
			out.Token = v
		}
	}
	if v, ok := k.(connector.Preparer); ok {
		checks[7].implemented = true
		if c.Prepares {
			out.Preparer = v
		}
	}
	if v, ok := k.(connector.Validator); ok {
		checks[8].implemented = true
		if c.Validates {
			out.Validator = v
		}
	}
	if v, ok := k.(connector.Prober); ok {
		checks[9].implemented = true
		if c.Probes {
			out.Prober = v
		}
	}
	if v, ok := k.(connector.ChoiceProvider); ok {
		checks[10].implemented = true
		if c.Choices {
			out.Choices = v
		}
	}
	if v, ok := k.(connector.StaleResolver); ok {
		checks[11].implemented = true
		if c.ResolvesStale {
			out.Stale = v
		}
	}

	rep, _, err := report(KindSink, name, checks, c.Unknown)
	out.Report = rep
	return out, err
}

// AckPoint reports which sink interface earns the acknowledgement, as the stable token the
// negotiation discloses to the operator. It is derived from the RESOLVED handles, so the answer is
// the one the engine will actually act on.
func (r *ResolvedSink) AckPoint() string {
	switch {
	case r.Token != nil:
		return "token"
	case r.Committer != nil:
		return "commit"
	case r.Flusher != nil:
		return "flush"
	default:
		return "write"
	}
}

// ResolvedTransform is the transform half. It is smaller because a transform has exactly two
// optional capabilities, but it exists so that the engine nil-checks a field here too rather than
// asserting in a node loop.
type ResolvedTransform struct {
	Transform connector.Transform
	Name      string
	Caps      connector.TransformCaps

	State     connector.StatefulTransform
	Validator connector.Validator

	Report []CapReport
}

// ResolveTransform snapshots t's optional capabilities against what c declares.
func ResolveTransform(name string, t connector.Transform, c connector.TransformCaps) (*ResolvedTransform, error) {
	out := &ResolvedTransform{Transform: t, Name: name, Caps: c}
	checks := []capCheck{
		{name: "keeps_state", title: "cap.keeps_state", iface: "connector.StatefulTransform", declared: c.KeepsState},
		{name: "validates", title: "cap.validates", iface: "connector.Validator", declared: c.Validates},
	}
	if v, ok := t.(connector.StatefulTransform); ok {
		checks[0].implemented = true
		if c.KeepsState {
			out.State = v
		}
	}
	if v, ok := t.(connector.Validator); ok {
		checks[1].implemented = true
		if c.Validates {
			out.Validator = v
		}
	}
	rep, _, err := report(KindTransform, name, checks, c.Unknown)
	out.Report = rep
	return out, err
}
