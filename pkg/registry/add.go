package registry

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
)

// AddSource registers a source and cross-checks Caps against the method set of S.
//
// IT PANICS at init time when:
//   - the name is already registered in this registry;
//   - a DECLARED capability has no corresponding interface on S;
//   - UpstreamRetention is RetentionUnknown, or Boundedness or LaneKinds is empty;
//   - StableKeys is declared with empty Notes;
//   - Caps.APIVersion is outside the range this core supports;
//   - the config spec fails its own lint — a duplicate path, an undocumented field, a predicate
//     referencing a nonexistent field, or a declared example that does not validate.
//
// IT DOES NOT PANIC, and instead records a warning on the [Descriptor], when an optional interface
// is IMPLEMENTED BUT NOT DECLARED.
//
// Panicking at init is the right severity for the first list: it fires on the author's first
// `go test`, in their own package, before anything is deployed.
func AddSource[S connector.Source](r *Registry, d SourceDef[S]) {
	var z S
	caps := d.Caps

	if err := checkCommon(KindSource, d.Name, caps.Caps); err != nil {
		panic("canal/registry: " + err.Error())
	}
	if _, dup := r.sources[d.Name]; dup {
		panic(fmt.Sprintf("canal/registry: source %q is already registered", d.Name))
	}
	if d.New == nil {
		panic(fmt.Sprintf("canal/registry: source %q has no New", d.Name))
	}
	if caps.UpstreamRetention == connector.RetentionUnknown {
		panic(fmt.Sprintf("canal/registry: source %q must declare UpstreamRetention; it is what makes the commit protocol safe", d.Name))
	}
	if len(caps.Boundedness) == 0 {
		panic(fmt.Sprintf("canal/registry: source %q declares no Boundedness", d.Name))
	}
	if len(caps.LaneKinds) == 0 {
		panic(fmt.Sprintf("canal/registry: source %q declares no LaneKinds", d.Name))
	}
	if caps.StableKeys && d.Notes == "" {
		panic(fmt.Sprintf("canal/registry: source %q declares StableKeys with empty Notes; document how Origin.Key is derived", d.Name))
	}
	if caps.MaxLanes < 0 {
		panic(fmt.Sprintf("canal/registry: source %q declares a negative MaxLanes", d.Name))
	}

	spec := withStageStandard(KindSource, mustSpec(KindSource, d.Name, d.Spec), false, caps.Heartbeats)
	mustLint(KindSource, d.Name, spec)

	checks := []capCheck{
		{name: "discoverable", title: "cap.discoverable", iface: "connector.Discoverer",
			declared: caps.Discoverable, implemented: implements[connector.Discoverer](any(z)),
			unlocks: []string{"a stream picker with no frontend code", "drift as a diff against a stored catalog"}},
		{name: "nackable", title: "cap.nackable", iface: "connector.Nackable",
			declared: caps.Nackable, implemented: implements[connector.Nackable](any(z)),
			unlocks: []string{"upstream is told when a record is dead-lettered"}},
		{name: "reports_backlog", title: "cap.reports_backlog", iface: "connector.BacklogReporter",
			declared: caps.ReportsBacklog, implemented: implements[connector.BacklogReporter](any(z)),
			unlocks: []string{"backlog records and bytes", "event-time lag"}},
		{name: "heartbeats", title: "cap.heartbeats", iface: "connector.Heartbeater",
			declared: caps.Heartbeats, implemented: implements[connector.Heartbeater](any(z)),
			unlocks: []string{"an idle lane does not pin a pruning upstream's retention", "a gated tail lane behind a long scan"}},
		{name: "validates", title: "cap.validates", iface: "connector.Validator",
			declared: caps.Validates, implemented: implements[connector.Validator](any(z)),
			unlocks: []string{"per-field diagnostics that required a connection to discover"}},
		{name: "probes", title: "cap.probes", iface: "connector.Prober",
			declared: caps.Probes, implemented: implements[connector.Prober](any(z)),
			unlocks: []string{"a named liveness breakdown rather than one boolean"}},
		{name: "choices", title: "cap.choices", iface: "connector.ChoiceProvider",
			declared: caps.Choices, implemented: implements[connector.ChoiceProvider](any(z)),
			unlocks: []string{"\"pick a table from this database\" with no core knowledge that tables exist"}},
		{name: "adopts_state", title: "cap.adopts_state", iface: "connector.StateAdopter",
			declared: caps.AdoptsState, implemented: implements[connector.StateAdopter](any(z)),
			unlocks: []string{"a rename or rewrite adopts the old connector's cursors"}},
	}
	rep, warnings, err := report(KindSource, d.Name, checks, caps.Unknown)
	if err != nil {
		panic("canal/registry: " + err.Error())
	}

	rep = declaredPlain(rep, "produces_event_time", "cap.produces_event_time", caps.ProducesEventTime,
		"the connector does not declare an event time", []string{"event-time lag per node"})
	rep = declaredPlain(rep, "produces_change", "cap.produces_change", caps.ProducesChange,
		"the connector does not emit typed change events", []string{"insert/update/delete semantics at the sink"})
	rep = declaredPlain(rep, "produces_schema", "cap.produces_schema", caps.ProducesSchema,
		"the connector does not report a schema", []string{"destination creation and drift handling"})
	rep = declaredPlain(rep, "complete_images", "cap.complete_images", caps.CompleteImages,
		"the connector may emit partial after-images", []string{"a sink that writes whole rows"})
	rep = declaredPlain(rep, "comparable_positions", "cap.comparable_positions", caps.ComparablePositions,
		"the connector supplies no order encoding on its positions",
		[]string{"mid-lane monotonicity assertions", "position-fraction progress"})
	rep = declaredPlain(rep, "replayable", "cap.replayable", caps.Replayable,
		"the connector cannot re-read from a committed position", []string{"at-least-once delivery"})
	rep = declaredPlain(rep, "stable_keys", "cap.stable_keys", caps.StableKeys,
		"the connector does not populate a stable Origin.Key",
		[]string{"effectively-once delivery", "engine-owned dedupe", "a request idempotency key"})
	rep = declaredPlain(rep, "mid_lane_resume", "cap.mid_lane_resume", caps.MidLaneResume,
		"the connector can only resume at a lane boundary", []string{"a commit inside a bounded lane"})

	r.sources[d.Name] = SourceEntry{
		Meta:       d.Meta,
		Spec:       spec,
		Caps:       caps,
		Descriptor: descriptor(KindSource, d.Meta, spec, caps, rep, warnings),
		New: func(ctx context.Context, cfg *config.Config) (connector.Source, error) {
			return d.New(ctx, cfg)
		},
	}
}

// AddSink registers a sink and cross-checks Caps against the method set of K. Same panic rules as
// [AddSource], plus MaxConcurrency and Modes, which the engine cannot default on a sink's behalf.
func AddSink[K connector.Sink](r *Registry, d SinkDef[K]) {
	var z K
	caps := d.Caps

	if err := checkCommon(KindSink, d.Name, caps.Caps); err != nil {
		panic("canal/registry: " + err.Error())
	}
	if _, dup := r.sinks[d.Name]; dup {
		panic(fmt.Sprintf("canal/registry: sink %q is already registered", d.Name))
	}
	if d.New == nil {
		panic(fmt.Sprintf("canal/registry: sink %q has no New", d.Name))
	}
	if caps.MaxConcurrency < 1 {
		panic(fmt.Sprintf("canal/registry: sink %q must declare MaxConcurrency of at least 1", d.Name))
	}
	if len(caps.Modes) == 0 {
		panic(fmt.Sprintf("canal/registry: sink %q declares no destination Modes", d.Name))
	}
	if caps.AppliesSchema && len(caps.SchemaChanges) == 0 {
		panic(fmt.Sprintf("canal/registry: sink %q declares AppliesSchema with no SchemaChanges; which kinds can it apply?", d.Name))
	}

	spec := withStageStandard(KindSink, mustSpec(KindSink, d.Name, d.Spec), caps.Structured, false)
	mustLint(KindSink, d.Name, spec)

	checks := []capCheck{
		{name: "flushes", title: "cap.flushes", iface: "connector.Flusher",
			declared: caps.Flushes, implemented: implements[connector.Flusher](any(z)),
			unlocks: []string{"buffered writing with an honest acknowledgement point"}},
		{name: "structured", title: "cap.structured", iface: "connector.StructuredSink",
			declared: caps.Structured, implemented: implements[connector.StructuredSink](any(z)),
			unlocks: []string{"records rather than bytes, for an SDK-shaped destination"}},
		{name: "partitions", title: "cap.partitions", iface: "connector.Partitioner",
			declared: caps.Partitions, implemented: implements[connector.Partitioner](any(z)),
			unlocks: []string{"per-table, per-tenant or per-day batching with no batching code"}},
		{name: "applies_schema", title: "cap.applies_schema", iface: "connector.SchemaApplier",
			declared: caps.AppliesSchema, implemented: implements[connector.SchemaApplier](any(z)),
			unlocks: []string{"schema drift applied at the destination"}},
		{name: "commits", title: "cap.commits", iface: "connector.Committer",
			declared: caps.Commits, implemented: implements[connector.Committer](any(z)),
			unlocks: []string{"exactly-once by two-phase commit"}},
		{name: "keeps_state", title: "cap.keeps_state", iface: "connector.WriterState",
			declared: caps.KeepsState, implemented: implements[connector.WriterState](any(z)),
			unlocks: []string{"in-progress work survives a restart"}},
		{name: "stores_token", title: "cap.stores_token", iface: "connector.TokenSink",
			declared: caps.StoresToken, implemented: implements[connector.TokenSink](any(z)),
			unlocks: []string{"exactly-once in one durability domain"}},
		{name: "prepares", title: "cap.prepares", iface: "connector.Preparer",
			declared: caps.Prepares, implemented: implements[connector.Preparer](any(z)),
			unlocks: []string{"the destination is created or verified before any data flows"}},
		{name: "validates", title: "cap.validates", iface: "connector.Validator",
			declared: caps.Validates, implemented: implements[connector.Validator](any(z)),
			unlocks: []string{"per-field diagnostics that required a connection to discover"}},
		{name: "probes", title: "cap.probes", iface: "connector.Prober",
			declared: caps.Probes, implemented: implements[connector.Prober](any(z)),
			unlocks: []string{"a named liveness breakdown rather than one boolean"}},
		{name: "choices", title: "cap.choices", iface: "connector.ChoiceProvider",
			declared: caps.Choices, implemented: implements[connector.ChoiceProvider](any(z)),
			unlocks: []string{"a destination picker with no core knowledge of the destination"}},
	}
	rep, warnings, err := report(KindSink, d.Name, checks, caps.Unknown)
	if err != nil {
		panic("canal/registry: " + err.Error())
	}

	rep = declaredPlain(rep, "idempotent", "cap.idempotent", caps.Idempotent,
		"re-delivering an identical request is not known to be harmless",
		[]string{"effectively-once delivery", "retrying an indeterminate write instead of stalling"})
	rep = declaredPlain(rep, "partial_failure", "cap.partial_failure", caps.PartialFailure,
		"the sink cannot say which records of a request failed",
		[]string{"sub-batch retry of exactly the failed records"})

	r.sinks[d.Name] = SinkEntry{
		Meta:       d.Meta,
		Spec:       spec,
		Caps:       caps,
		Descriptor: descriptor(KindSink, d.Meta, spec, caps, rep, warnings),
		New: func(ctx context.Context, cfg *config.Config) (connector.Sink, error) {
			return d.New(ctx, cfg)
		},
	}
}

// AddTransform registers a transform.
func AddTransform[T connector.Transform](r *Registry, d TransformDef[T]) {
	var z T
	caps := d.Caps

	if err := checkCommon(KindTransform, d.Name, caps.Caps); err != nil {
		panic("canal/registry: " + err.Error())
	}
	if _, dup := r.transforms[d.Name]; dup {
		panic(fmt.Sprintf("canal/registry: transform %q is already registered", d.Name))
	}
	if d.New == nil {
		panic(fmt.Sprintf("canal/registry: transform %q has no New", d.Name))
	}

	spec := withStageStandard(KindTransform, mustSpec(KindTransform, d.Name, d.Spec), false, false)
	mustLint(KindTransform, d.Name, spec)

	checks := []capCheck{
		{name: "keeps_state", title: "cap.keeps_state", iface: "connector.StatefulTransform",
			declared: caps.KeepsState, implemented: implements[connector.StatefulTransform](any(z)),
			unlocks: []string{"windowing or aggregation state survives a restart"}},
		{name: "validates", title: "cap.validates", iface: "connector.Validator",
			declared: caps.Validates, implemented: implements[connector.Validator](any(z))},
	}
	rep, warnings, err := report(KindTransform, d.Name, checks, caps.Unknown)
	if err != nil {
		panic("canal/registry: " + err.Error())
	}
	rep = declaredPlain(rep, "preserves_order", "cap.preserves_order", caps.PreservesOrder,
		"the transform may reorder records within a lane",
		[]string{"placement on a prefix-ordered lane without an acknowledged consequence"})

	r.transforms[d.Name] = TransformEntry{
		Meta:       d.Meta,
		Spec:       spec,
		Caps:       caps,
		Descriptor: descriptor(KindTransform, d.Meta, spec, caps, rep, warnings),
		New: func(ctx context.Context, cfg *config.Config) (connector.Transform, error) {
			return d.New(ctx, cfg)
		},
	}
}

// AddBuffer registers a buffer. A buffer that does not declare itself bounded is refused, because a
// buffer with no bound is not a buffer (design rule R6).
func AddBuffer[B connector.Buffer](r *Registry, d BufferDef[B]) {
	caps := d.Caps
	if err := checkCommon(KindBuffer, d.Name, caps.Caps); err != nil {
		panic("canal/registry: " + err.Error())
	}
	if _, dup := r.buffers[d.Name]; dup {
		panic(fmt.Sprintf("canal/registry: buffer %q is already registered", d.Name))
	}
	if d.New == nil {
		panic(fmt.Sprintf("canal/registry: buffer %q has no New", d.Name))
	}
	if !caps.Bounded {
		panic(fmt.Sprintf("canal/registry: buffer %q does not declare Bounded; an unbounded buffer is not a buffer", d.Name))
	}

	spec := withStageStandard(KindBuffer, mustSpec(KindBuffer, d.Name, d.Spec), false, false)
	mustLint(KindBuffer, d.Name, spec)

	var rep []CapReport
	rep = declaredPlain(rep, "durability", "cap.durability", caps.Durability != connector.DurabilityNone,
		"this buffer keeps records in memory only", []string{"shortening the acknowledgement chain, if its domain is wide enough"})
	rep = declaredPlain(rep, "chains", "cap.chains", caps.Chains,
		"this buffer cannot be the overflow target of another", []string{"a small memory buffer in front of a large disk one"})

	r.buffers[d.Name] = BufferEntry{
		Meta:       d.Meta,
		Spec:       spec,
		Caps:       caps,
		Descriptor: descriptor(KindBuffer, d.Meta, spec, caps, rep, nil),
		New: func(ctx context.Context, cfg *config.Config) (connector.Buffer, error) {
			return d.New(ctx, cfg)
		},
	}
}

// AddEncoder registers an encoder.
func AddEncoder[E connector.Encoder](r *Registry, d EncoderDef[E]) {
	if err := checkCommon(KindEncoder, d.Name, d.Caps.Caps); err != nil {
		panic("canal/registry: " + err.Error())
	}
	if _, dup := r.encoders[d.Name]; dup {
		panic(fmt.Sprintf("canal/registry: encoder %q is already registered", d.Name))
	}
	if d.New == nil {
		panic(fmt.Sprintf("canal/registry: encoder %q has no New", d.Name))
	}
	spec := withStageStandard(KindEncoder, mustSpec(KindEncoder, d.Name, d.Spec), false, false)
	mustLint(KindEncoder, d.Name, spec)
	r.encoders[d.Name] = EncoderEntry{
		Meta: d.Meta, Spec: spec, Caps: d.Caps,
		Descriptor: descriptor(KindEncoder, d.Meta, spec, d.Caps, nil, nil),
		New: func(ctx context.Context, cfg *config.Config) (connector.Encoder, error) {
			return d.New(ctx, cfg)
		},
	}
}

// AddDecoder registers a decoder.
func AddDecoder[D connector.Decoder](r *Registry, d DecoderDef[D]) {
	if err := checkCommon(KindDecoder, d.Name, d.Caps.Caps); err != nil {
		panic("canal/registry: " + err.Error())
	}
	if _, dup := r.decoders[d.Name]; dup {
		panic(fmt.Sprintf("canal/registry: decoder %q is already registered", d.Name))
	}
	if d.New == nil {
		panic(fmt.Sprintf("canal/registry: decoder %q has no New", d.Name))
	}
	spec := withStageStandard(KindDecoder, mustSpec(KindDecoder, d.Name, d.Spec), false, false)
	mustLint(KindDecoder, d.Name, spec)
	r.decoders[d.Name] = DecoderEntry{
		Meta: d.Meta, Spec: spec, Caps: d.Caps,
		Descriptor: descriptor(KindDecoder, d.Meta, spec, d.Caps, nil, nil),
		New: func(ctx context.Context, cfg *config.Config) (connector.Decoder, error) {
			return d.New(ctx, cfg)
		},
	}
}

// AddFramer registers a framer.
func AddFramer[F connector.Framer](r *Registry, d FramerDef[F]) {
	if err := checkCommon(KindFramer, d.Name, d.Caps.Caps); err != nil {
		panic("canal/registry: " + err.Error())
	}
	if _, dup := r.framers[d.Name]; dup {
		panic(fmt.Sprintf("canal/registry: framer %q is already registered", d.Name))
	}
	if d.New == nil {
		panic(fmt.Sprintf("canal/registry: framer %q has no New", d.Name))
	}
	spec := withStageStandard(KindFramer, mustSpec(KindFramer, d.Name, d.Spec), false, false)
	mustLint(KindFramer, d.Name, spec)
	r.framers[d.Name] = FramerEntry{
		Meta: d.Meta, Spec: spec, Caps: d.Caps,
		Descriptor: descriptor(KindFramer, d.Meta, spec, d.Caps, nil, nil),
		New: func(ctx context.Context, cfg *config.Config) (connector.Framer, error) {
			return d.New(ctx, cfg)
		},
	}
}

// AddDeframer registers a deframer.
func AddDeframer[D connector.Deframer](r *Registry, d DeframerDef[D]) {
	if err := checkCommon(KindDeframer, d.Name, d.Caps.Caps); err != nil {
		panic("canal/registry: " + err.Error())
	}
	if _, dup := r.deframers[d.Name]; dup {
		panic(fmt.Sprintf("canal/registry: deframer %q is already registered", d.Name))
	}
	if d.New == nil {
		panic(fmt.Sprintf("canal/registry: deframer %q has no New", d.Name))
	}
	spec := withStageStandard(KindDeframer, mustSpec(KindDeframer, d.Name, d.Spec), false, false)
	mustLint(KindDeframer, d.Name, spec)
	r.deframers[d.Name] = DeframerEntry{
		Meta: d.Meta, Spec: spec, Caps: d.Caps,
		Descriptor: descriptor(KindDeframer, d.Meta, spec, d.Caps, nil, nil),
		New: func(ctx context.Context, cfg *config.Config) (connector.Deframer, error) {
			return d.New(ctx, cfg)
		},
	}
}

// AddCompressor registers a compressor.
func AddCompressor[C connector.Compressor](r *Registry, d CompressorDef[C]) {
	if err := checkCommon(KindCompressor, d.Name, d.Caps.Caps); err != nil {
		panic("canal/registry: " + err.Error())
	}
	if _, dup := r.compressors[d.Name]; dup {
		panic(fmt.Sprintf("canal/registry: compressor %q is already registered", d.Name))
	}
	if d.New == nil {
		panic(fmt.Sprintf("canal/registry: compressor %q has no New", d.Name))
	}
	spec := withStageStandard(KindCompressor, mustSpec(KindCompressor, d.Name, d.Spec), false, false)
	mustLint(KindCompressor, d.Name, spec)
	r.compressors[d.Name] = CompressorEntry{
		Meta: d.Meta, Spec: spec, Caps: d.Caps,
		Descriptor: descriptor(KindCompressor, d.Meta, spec, d.Caps, nil, nil),
		New: func(ctx context.Context, cfg *config.Config) (connector.Compressor, error) {
			return d.New(ctx, cfg)
		},
	}
}

func mustSpec(kind Kind, name string, s *config.Spec) *config.Spec {
	if s == nil {
		panic(fmt.Sprintf("canal/registry: %s %q has a nil config.Spec; use config.NewSpec()", kind, name))
	}
	return s
}

func mustLint(kind Kind, name string, s *config.Spec) {
	if err := lintSpec(kind, name, s); err != nil {
		panic("canal/registry: " + err.Error())
	}
	if err := lintExamples(kind, name, s); err != nil {
		panic("canal/registry: " + err.Error())
	}
}

// descriptor builds the instantiation-free projection once, at registration, so serving it later
// runs no connector code and cannot be broken by a panicking constructor.
func descriptor(kind Kind, m Meta, spec *config.Spec, caps any, rep []CapReport, warnings []string) Descriptor {
	capsJSON, err := json.Marshal(caps)
	if err != nil {
		panic(fmt.Sprintf("canal/registry: %s %q has unmarshallable Caps: %v", kind, m.Name, err))
	}
	schemaJSON, err := spec.JSONSchema()
	if err != nil {
		panic(fmt.Sprintf("canal/registry: %s %q has an unrenderable config spec: %v", kind, m.Name, err))
	}
	return Descriptor{
		Kind:         kind,
		Name:         m.Name,
		Version:      m.Version,
		Title:        m.Title,
		Summary:      m.Summary,
		Docs:         m.Docs,
		Notes:        m.Notes,
		Support:      m.Support,
		Config:       spec,
		Caps:         capsJSON,
		JSONSchema:   schemaJSON,
		Capabilities: rep,
		Warnings:     warnings,
	}
}
