package connector

import (
	"context"
	"log/slog"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// SourceRuntime is the source's handle onto the core. Everything a connector needs from
// canal arrives here.
//
// It is an INTERFACE, not a struct, for three reasons: a connector can be unit tested
// against a fake; the conformance kit can build one; and an out-of-process adapter can
// implement it over a reverse RPC channel. A concrete struct with unexported state is
// untestable and un-wireable.
//
// THIS IS THE GROWTH PATH. Adding a method here does not break a single connector, because
// the core implements it and the connector only calls it. Every capability canal adds to
// the core in v2 and v3 arrives on a runtime interface or as a field on a request struct.
// Nothing is ever added to [Source], [Sink], [Buffer] or [Transform].
type SourceRuntime interface {
	// Context returns a context whose lifetime is the COMPONENT'S, not a single call's.
	//
	// This is where a source takes the context it stores, and it exists so no connector
	// invents its own shutdown signaller — which every connector must do when the only
	// context it is given is call-scoped.
	Context() context.Context

	// Lanes is how a source announces, retires and observes its lanes.
	Lanes() LaneCtl

	// State is the durable byte store: one slot per lane plus one node-scoped slot. A
	// source whose progress lives upstream never touches it.
	State() StateHandle

	// Streams is the operator's per-stream choice for this source node, as configured and
	// validated.
	//
	// Its absence forced a source that discovers 900 streams to duplicate the operator's
	// selection in its own connector config: spec.Spec.Streams reached the SINK through
	// Opening.Streams and never reached the source at all, so a source could not learn
	// which streams to read, with which lane kinds, or with which keys. It is a snapshot
	// the caller may retain, and it changes only across generations.
	Streams() []ConfiguredStream

	// Schemas resolves and registers schemas. A source that discovers structure needs the
	// same table a codec needs.
	Schemas() SchemaLookup

	// Declare publishes an ordered schema change and its RESULTING schema, and returns the
	// ref that records written after it carry.
	//
	// This is the drift subsystem's PRODUCER, and without it the subsystem had none: the
	// core tracked changes into the checkpoint, quiesced streams, negotiated sink
	// capabilities and applied changes, while no interface let a source say a column had
	// appeared. connector.Event carried Message and Detail strings and nothing typed.
	//
	// It is legal ONLY from inside Read or ReadLanes, on the read goroutine, so a change
	// has a defined position in the record order — the schema epoch is committed
	// atomically with the cursors of the records that follow it, which is what makes
	// "encountered a change event whose schema isn't known" structurally absent. Calling
	// it from the control goroutine is fault.PermanentContract.
	//
	// The returned ref is stamped on subsequent records through record.Record.Schema. The
	// core owns the epoch, the durable schema history and the quiesce; the source owns
	// only the observation.
	Declare(ctx context.Context, ch schema.Change, result *schema.Schema) (schema.Ref, error)

	// Instance identifies THIS process instance, matching the deployment's worker id.
	//
	// Without it, N replicas of a push source behind one load balancer all derived the same
	// LaneID — because record.DeriveLaneID takes (tenant, pipeline, node, name) and every
	// replica agrees on all four — so exactly one replica held the lease and the others
	// dropped everything their peers sent them. UnitsExternal is unimplementable without
	// per-instance identity.
	Instance() string

	// Config re-renders this node's configuration with secret references resolved AS OF NOW.
	//
	// New receives config once, and Source is frozen, so a rotated credential used to
	// require a whole new pipeline generation per rotation. A source that reconnects calls
	// this on the reconnect path and gets the current secret. Non-secret values are the
	// generation's, unchanged: this is credential freshness, not live reconfiguration,
	// which remains a new generation.
	Config(ctx context.Context) (*config.Config, error)

	Log() *slog.Logger
	Metrics() Metrics

	// Batcher returns a goroutine-free batcher a source may use to shape its own reads.
	Batcher(p config.BatchPolicy) *Batcher

	// Note publishes a pipeline event: a schema change, a lane note, a drift observation, a
	// derivation explanation.
	//
	// Events are ordered, bounded, and appear in the read model's recent-events ring. Drift
	// is an event, not a log line and not a metric. Note is best-effort by contract: it is
	// the one thing on a runtime that cannot fail, which is why a component reports outcomes
	// through its RETURN VALUE and never through the runtime.
	Note(e Event)

	Tenant() record.TenantID
	Pipeline() record.PipelineID
	Node() record.NodeID
}

// SinkRuntime is the sink's handle onto the core.
//
// It deliberately has NO Lanes and NO State. A sink is structurally incapable of holding
// progress, which is why a new sink cannot get progress wrong. Everything added here is
// about SHAPE, never about progress, and that boundary is what keeps the asymmetry.
type SinkRuntime interface {
	Context() context.Context

	// Schemas resolves a schema.Ref, including one minted AFTER Open.
	//
	// Opening.Schemas is the set known at Open, and a drifting pipeline mints refs later.
	// Without a lookup a sink handed a later ref could only refuse the whole epoch, and
	// SchemaApplier.ApplySchemaChange for a CreateStream was unappliable because the change
	// named a stream whose body the sink could not fetch.
	Schemas() SchemaLookup

	// Streams is the LIVE set of configured streams, which Opening.Streams froze at Open.
	//
	// A stream that appears mid-run — a new table, a renamed one — has a destination mode
	// and a key list the operator configured, and a sink applying a CreateStream needs both
	// to create the destination correctly. Reading them from a frozen Opening meant a
	// mid-run stream had neither.
	Streams() []ConfiguredStream

	// Config re-renders this node's configuration with secret references resolved as of
	// now. See [SourceRuntime.Config]; a destination's credentials rotate too.
	Config(ctx context.Context) (*config.Config, error)

	Log() *slog.Logger
	Metrics() Metrics
	Note(e Event)
	Tenant() record.TenantID
	Pipeline() record.PipelineID
	Node() record.NodeID
}

// TransformRuntime is a transform's handle onto the core.
type TransformRuntime interface {
	Context() context.Context

	// Lanes is the READ-ONLY lane table. A transform can see the plan and cannot change it.
	//
	// ADR 0008's prescribed shadow transform — the one that suppresses a tail record already
	// covered by a concurrent chunked scan — cannot be written without it: the transform
	// must know how many scan lanes exist and whether they have finished in order to
	// self-retire, and it must not be able to announce a lane. Its own durable state comes
	// from [StatefulTransform], which the checkpoint already keys by node.
	Lanes() LaneView

	Log() *slog.Logger
	Metrics() Metrics
	Note(e Event)
	Node() record.NodeID
}

// BufferRuntime is a buffer's handle onto the core.
type BufferRuntime interface {
	Context() context.Context
	Log() *slog.Logger
	Metrics() Metrics
	Note(e Event)
	Node() record.NodeID

	// DataDir is a directory this buffer instance owns. Its durability DOMAIN is declared in
	// [BufferCaps] and validated against the deployment: a node-local directory cannot back
	// a cluster-durable claim.
	DataDir() string
}

// CodecRuntime is a codec's handle onto the core.
type CodecRuntime interface {
	Context() context.Context
	Log() *slog.Logger
	Metrics() Metrics

	// Schemas resolves a schema reference, so a registry-backed codec can look up what it is
	// encoding. Its absence is what makes a schema-registry Avro codec unwritable.
	Schemas() SchemaLookup
}

// SchemaLookup is the codec-facing view of the pipeline's schema table.
type SchemaLookup interface {
	Get(ctx context.Context, ref schema.Ref) (*schema.Schema, error)

	// Register returns a ref for a schema, registering it if new. A codec that mints a wire
	// id — an Avro registry magic byte — uses this.
	Register(ctx context.Context, s *schema.Schema) (schema.Ref, error)
}

// Metrics is the connector's metric surface.
//
// THE CORE OWNS metric naming, tagging, cardinality and export; a connector registers
// through this handle and can never name a metric or invent a label. Names are namespaced
// under canal_connector_<component>_<metric> automatically and the label set is fixed by
// the core. A connector requesting an unbounded label gets an error, not a cardinality
// explosion.
type Metrics interface {
	Counter(name string, labels ...string) (Counter, error)
	Gauge(name string, labels ...string) (Gauge, error)
	Histogram(name string, buckets []float64, labels ...string) (Histogram, error)
}

// Counter is a monotonically increasing series.
type Counter interface {
	Add(delta float64, labelValues ...string)
}

// Gauge is a point-in-time value.
//
// A gauge whose value is unmeasurable must be OMITTED, never set to zero: a fully stalled
// pipeline that reports zero lag is worse than one that reports nothing.
type Gauge interface {
	Set(v float64, labelValues ...string)
}

// Histogram is a distribution.
type Histogram interface {
	Observe(v float64, labelValues ...string)
}
