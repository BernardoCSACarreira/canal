package registry

import "github.com/BernardoCSACarreira/canal/pkg/config"

// withStageStandard returns a copy of s with the stage-standard composite fields for kind appended,
// unless the component already declares a field of that name.
//
// The connector author writes none of them; the operator configures them per node; the ENGINE reads
// them. This is what makes a per-sink wire format expressible — fan-out to two sinks in two formats
// is two sink nodes with two codec blocks — and it is why the codec stages have a caller at all.
//
// It copies rather than mutates because a *config.Spec built at init is shared: a connector that
// registers the same spec pointer under two names must not accumulate two sets of appended fields.
func withStageStandard(kind Kind, s *config.Spec, structuredSink, heartbeats bool) *config.Spec {
	out := &config.Spec{
		Summary:     s.Summary,
		Description: s.Description,
		Version:     s.Version,
		Deprecated:  s.Deprecated,
		Fields:      append([]config.Field(nil), s.Fields...),
		Examples:    append([]config.Example(nil), s.Examples...),
		Lints:       append([]config.Lint(nil), s.Lints...),
	}

	add := func(f config.Field) {
		for i := range out.Fields {
			if out.Fields[i].Name == f.Name {
				return
			}
		}
		out.Fields = append(out.Fields, f)
	}

	// Every kind gets retry and a when-full policy, because every node can fail and every edge is
	// bounded.
	add(config.Fields.Retry(config.FieldRetry))
	add(config.Fields.WhenFull(config.FieldWhenFull))

	switch kind {
	case KindSource:
		add(config.Fields.LaneBudget(config.FieldLaneBudget))
		if heartbeats {
			add(config.Fields.HeartbeatInterval(config.FieldHeartbeatInterval))
		}
	case KindSink:
		if !structuredSink {
			// A structured sink is handed records, not bytes, so attaching an encoder to it would
			// be a double encoding. The refusal is structural: the field is not even offered.
			add(config.Fields.Codec(config.FieldCodec))
		}
		add(config.Fields.Batching(config.FieldBatching))
		add(config.Fields.MaxInFlight(config.FieldMaxInFlight))
		add(config.Fields.Dedupe(config.FieldDedupe))
	case KindBuffer:
		add(config.Fields.Capacity(config.FieldCapacity))
	}
	return out
}
