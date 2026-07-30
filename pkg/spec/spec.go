package spec

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// Spec is a pipeline, as data.
type Spec struct {
	Tenant   record.TenantID   `json:"tenant"`
	ID       record.PipelineID `json:"id"`
	Revision uint64            `json:"revision"`
	Title    string            `json:"title,omitempty"`

	// Graph is the topology. Exactly one representation of exactly one entity.
	Graph []Node `json:"graph"`

	// Guarantee is the REQUESTED tier. Build returns the negotiated one and refuses if the request is
	// impossible, with per-factor reasons, at submit time — not at three in the morning with a
	// quieter delivery guarantee.
	Guarantee connector.Guarantee `json:"guarantee"`

	// Retry, WhenFull, LaneBudget, Drift and Clock are pipeline-wide policy. A node may override
	// Retry and WhenFull through its own stage-standard fields.
	Retry      fault.RetryPolicy  `json:"retry"`
	WhenFull   connector.WhenFull `json:"when_full"`
	LaneBudget int                `json:"lane_budget"`
	Drift      DriftPolicy        `json:"drift"`
	Clock      ClockPolicy        `json:"clock"`

	// Parallelism is the maximum number of lanes one worker reads concurrently. It is validated
	// against SourceCaps.MaxLanes rather than being advisory.
	Parallelism int `json:"parallelism"`

	// Streams is the operator's per-stream, optionally per-NODE choice, validated against the
	// source's discovered catalog and against the sink's declared modes.
	//
	// Source-side and destination-side modes are ORTHOGONAL, which is what makes M times N
	// source/sink combinations free.
	Streams []StreamConfig `json:"streams,omitempty"`

	// Downgrades are the operator-signed waivers in force. ADR 0024 names a waiver as the ONLY
	// sanctioned way to run below the requested tier, and this is where one is written down.
	//
	// Without this field telemetry.Negotiated.Downgrades was always nil and the mechanism the
	// guarantee ADR rests on was unreachable: negotiate had nothing to read, so the choice was
	// between refusing a pipeline outright and degrading it silently — the two outcomes the waiver
	// exists to avoid. Build matches each waiver against the diagnostics it claims to cover;
	// a waiver that covers nothing is itself a diagnostic, so a stale one is visible rather than
	// load-bearing.
	Downgrades []telemetry.Downgrade `json:"downgrades,omitempty"`
}

// Node returns the node with the given id.
func (s *Spec) Node(id record.NodeID) (*Node, bool) {
	for i := range s.Graph {
		if s.Graph[i].ID == id {
			return &s.Graph[i], true
		}
	}
	return nil, false
}

// Consumers returns the nodes reading from id, with the selector each uses. Fan-out is a node with
// more than one consumer, and it needs no other representation.
func (s *Spec) Consumers(id record.NodeID) []Edge {
	var out []Edge
	for i := range s.Graph {
		for _, e := range s.Graph[i].Inputs {
			if e.From == id {
				out = append(out, Edge{From: s.Graph[i].ID, Select: e.Select})
			}
		}
	}
	return out
}

// Terminals returns the nodes with no outgoing edge.
func (s *Spec) Terminals() []record.NodeID {
	has := map[record.NodeID]bool{}
	for i := range s.Graph {
		for _, e := range s.Graph[i].Inputs {
			has[e.From] = true
		}
	}
	var out []record.NodeID
	for i := range s.Graph {
		if !has[s.Graph[i].ID] {
			out = append(out, s.Graph[i].ID)
		}
	}
	return out
}

// StreamFor returns the operator's choice for one stream at one node: the node-scoped entry if
// there is one, otherwise the unscoped entry, otherwise false.
//
// It is a method rather than an inline loop in negotiate because the precedence rule — specific
// beats general — must have exactly one implementation. Two call sites resolving it independently is
// how a fan-out branch ends up validated against another branch's mode.
func (s *Spec) StreamFor(node record.NodeID, stream record.StreamName) (StreamConfig, bool) {
	var general StreamConfig
	var haveGeneral bool
	for i := range s.Streams {
		sc := s.Streams[i]
		if sc.Stream != stream {
			continue
		}
		if sc.Node != "" && sc.Node == node {
			return sc, true
		}
		if sc.Node == "" {
			general, haveGeneral = sc, true
		}
	}
	return general, haveGeneral
}

// StreamsFor returns every stream choice that applies to one node, node-scoped entries taking
// precedence over unscoped ones for the same stream name.
func (s *Spec) StreamsFor(node record.NodeID) []StreamConfig {
	seen := map[record.StreamName]bool{}
	var out []StreamConfig
	for i := range s.Streams {
		if s.Streams[i].Node == "" || s.Streams[i].Node != node {
			continue
		}
		seen[s.Streams[i].Stream] = true
		out = append(out, s.Streams[i])
	}
	for i := range s.Streams {
		if s.Streams[i].Node != "" || seen[s.Streams[i].Stream] {
			continue
		}
		out = append(out, s.Streams[i])
	}
	return out
}

// StreamConfig is the operator's choice for one logical stream, optionally scoped to ONE NODE.
type StreamConfig struct {
	Stream record.StreamName `json:"stream"`

	// Node scopes this entry to one graph node. EMPTY MEANS EVERY NODE, which is the two-node
	// pipeline's answer and stays the default.
	//
	// Its absence made two whole classes of graph unbuildable. A FAN-OUT to a warehouse that must
	// upsert plus an append-only queue, a metrics feed and a dead-letter sink was refused at Build,
	// because one Write mode per stream was checked against every sink in turn and at most one of
	// four could match. A FAN-IN of two sources with different lane kinds was refused for the mirror
	// reason: Read was cross-multiplied against every source, and record.DefaultStream makes the
	// name collision the DEFAULT case for two single-stream sources.
	//
	// With a node scope, negotiate stops cross-multiplying and becomes a lookup: for this node, what
	// did the operator ask for. Node-scoped entries win over the unscoped one for the same stream.
	Node record.NodeID `json:"node,omitempty"`

	// Read is the source-side mode: which lane kinds the operator wants for this stream. Validated
	// against the discovered catalog's declared support, for the node this entry scopes to.
	Read []connector.LaneKind `json:"read"`

	// Write is the destination-side mode. Validated against SinkCaps.Modes, so upsert against an
	// append-only destination is a diagnostic rather than a corruption.
	Write connector.DestMode `json:"write"`

	Keys [][]string `json:"keys,omitempty"`

	// Dedupe enables the engine's keyed dedupe for this stream. Nil means no dedupe.
	Dedupe *DedupeConfig `json:"dedupe,omitempty"`
}

// DedupeConfig configures the engine-owned keyed dedupe for one stream.
//
// Dedupe is NOT a transform. A transform returns immediately and has no channel through which to
// observe settlement, so a transform-based dedupe cannot mark a key seen after the write. Dedupe is
// a property of a sink node, configured per stream, implemented by the engine, which owns
// settlement.
type DedupeConfig struct {
	// Layer chooses which identity layer to key on. The SCOPE is not configurable: the full durable
	// key is always
	//
	//	(tenant, pipeline, source-node, stream, layer, identity)
	//
	// because an earlier attempt keyed on the bare event id, so two connectors or two tenants
	// emitting 1, 2, 3 silently discarded each other's events (design rule R5).
	Layer DedupeLayer `json:"layer"`

	// Window is the retention over which "duplicate" is meaningful. It is REQUIRED and has no
	// default, because a process-lifetime cache described in documentation as a platform retention
	// window matches neither semantics: after a restart the same retry is accepted and re-appended.
	//
	// The engine trims entries older than Window in the SAME durable write that advances the cursor,
	// so the store never grows without bound and the trim cannot diverge from the progress it
	// protects.
	Window time.Duration `json:"window"`
}

// DedupeLayer names which of the three idempotency layers the dedupe keys on.
type DedupeLayer uint8

const (
	// DedupeUpstream keys on record.Origin.Upstream, the vendor's own id. Layer one.
	DedupeUpstream DedupeLayer = iota + 1
	// DedupeKey keys on record.Origin.Key, canal's canonical identity. Layer two.
	DedupeKey
)

var dedupeLayerNames = map[DedupeLayer]string{
	DedupeUpstream: "upstream",
	DedupeKey:      "key",
}

// String returns the stable snake_case token for l.
func (l DedupeLayer) String() string {
	if s, ok := dedupeLayerNames[l]; ok {
		return s
	}
	return "key"
}

// Summary is the listing projection of a spec: enough to render a list without loading a graph.
//
// It exists because the config store's List must not have to deserialise every graph in a tenant to
// answer "what pipelines are there".
type Summary struct {
	Tenant    record.TenantID     `json:"tenant"`
	ID        record.PipelineID   `json:"id"`
	Revision  uint64              `json:"revision"`
	Title     string              `json:"title,omitempty"`
	Guarantee connector.Guarantee `json:"guarantee"`

	// Nodes is the node count, so a list can show shape without the graph.
	Nodes int `json:"nodes"`

	UpdatedAt time.Time `json:"updated_at"`
}

// Summarise projects s. It is a method rather than a store-side conversion so that the projection
// cannot drift per store implementation.
func (s *Spec) Summarise(updatedAt time.Time) Summary {
	return Summary{
		Tenant:    s.Tenant,
		ID:        s.ID,
		Revision:  s.Revision,
		Title:     s.Title,
		Guarantee: s.Guarantee,
		Nodes:     len(s.Graph),
		UpdatedAt: updatedAt,
	}
}
