// This file is the HARNESS, not the connector. It imports engine, spec and memstore, which a
// third-party connector may not, and its only job is to turn the findings argued in connector.go
// from claims into observations. Everything it asserts is a claim in that file's summary block.
package fanout

import (
	"context"
	"strings"
	"testing"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/internal/example/memstore"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

func deps() engine.Deps { return engine.Deps{State: memstore.New()} }

// durableState is memstore with one lie removed: it declares a node-scoped durability domain.
//
// memstore is honest scaffolding — it reports DurabilityNone because it is a map. store.StoreCaps
// now refuses any tier above at-least-once on such a store, since the dedupe additions and pending
// committables ARE what those tiers are made of and they cannot live only in RAM. A test about the
// GUARANTEE LADDER therefore needs a store that claims to be durable, or it is testing the store
// rather than the ladder.
type durableState struct{ *memstore.StateStore }

func (durableState) Capabilities() store.StoreCaps {
	return store.StoreCaps{
		AtomicMultiKey: true,
		CAS:            true,
		EpochFencing:   true,
		Durability:     connector.DurabilityNode,
		FlushIsDurable: true,
	}
}

func durableDeps() engine.Deps { return engine.Deps{State: durableState{memstore.New()}} }

// fanOut is the commissioned topology: one source, three sinks with three different guarantees and
// three different speeds, plus a dead-letter destination on the best-effort branch's failed edge.
func fanOut(g connector.Guarantee, write connector.DestMode, drift spec.DriftPolicy) spec.Spec {
	return spec.Spec{
		Tenant: "default", ID: "fanout", Guarantee: g,
		Drift: drift,
		Retry: fault.RetryPolicy{
			MaxAttempts: 4, Backoff: fault.DefaultBackoff, Terminal: fault.TerminalDeadLetter,
		},
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: "hostile_cdc",
				Config: map[string]any{"slot": "canal_slot", "stream": "orders"}},
			{ID: "wh", Kind: registry.KindSink, Name: "hostile_warehouse", Label: "warehouse 30s",
				Config: map[string]any{"table": "orders_mirror"},
				Inputs: []spec.Edge{{From: "in", Select: spec.EdgeMain}}},
			{ID: "q", Kind: registry.KindSink, Name: "hostile_queue", Label: "queue 1ms",
				Config: map[string]any{"topic": "orders"},
				Inputs: []spec.Edge{{From: "in", Select: spec.EdgeMain}}},
			{ID: "m", Kind: registry.KindSink, Name: "hostile_metrics", Label: "metrics best effort",
				Config: map[string]any{"endpoint": "udp://statsd:8125"},
				Inputs: []spec.Edge{{From: "in", Select: spec.EdgeMain}}},
			{ID: "dlq", Kind: registry.KindSink, Name: "hostile_dlq",
				Config: map[string]any{"path": "/var/lib/canal/dlq"},
				Inputs: []spec.Edge{{From: "m", Select: spec.EdgeFailed}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "orders",
			Read:   []connector.LaneKind{connector.LaneKindStream},
			Write:  write,
			Keys:   [][]string{{"id"}},
		}},
	}
}

// fanIn is case (2): two sources merging into one sink.
func fanIn(streams []spec.StreamConfig) spec.Spec {
	return spec.Spec{
		Tenant: "default", ID: "fanin", Guarantee: connector.AtLeastOnce, Retry: fault.DefaultRetry,
		Graph: []spec.Node{
			{ID: "l", Kind: registry.KindSource, Name: "hostile_merge_scan",
				Config: map[string]any{"stream": "left"}},
			{ID: "r", Kind: registry.KindSource, Name: "hostile_merge_tail",
				Config: map[string]any{"stream": "right"}},
			{ID: "out", Kind: registry.KindSink, Name: "hostile_merge_sink",
				Config: map[string]any{"table": "merged"},
				Inputs: []spec.Edge{{From: "l"}, {From: "r"}}},
		},
		Streams: streams,
	}
}

func dump(t *testing.T, d config.Diagnostics) {
	t.Helper()
	for _, x := range d {
		t.Logf("  %s [%s] node=%s path=%v: %s", x.Severity, x.Code, x.Node, x.Path, x.Message)
	}
}

func has(d config.Diagnostics, substr string) bool {
	for _, x := range d {
		if strings.Contains(x.Message, substr) {
			return true
		}
	}
	return false
}

// TestRegistrationIsClean proves the six components' declared capabilities match their method sets.
// Over-declaration panics at init, so reaching this test at all proves half of it; the warning list
// proves the other half.
func TestRegistrationIsClean(t *testing.T) {
	for _, n := range []string{"hostile_cdc", "hostile_merge_scan", "hostile_merge_tail"} {
		if _, ok := Reg.Source(n); !ok {
			t.Fatalf("source %q did not register", n)
		}
	}
	for _, n := range []string{"hostile_warehouse", "hostile_queue", "hostile_metrics", "hostile_merge_sink", "hostile_dlq"} {
		if _, ok := Reg.Sink(n); !ok {
			t.Fatalf("sink %q did not register", n)
		}
	}
	for _, d := range Reg.Descriptors() {
		if len(d.Warnings) != 0 {
			t.Errorf("%s/%s registered with warnings: %v", d.Kind, d.Name, d.Warnings)
		}
	}
}

// TestFanOutIsRefused is F1: the commissioned fan-out cannot be configured, because
// StreamConfig.Write is one destination mode for the whole pipeline and negotiate checks it against
// every sink.
func TestFanOutIsRefused(t *testing.T) {
	// Drift is pinned to ignore so that F12 (below) does not contaminate this observation.
	//
	// The warehouse mirrors a changelog, so the stream must be upsert.
	_, _, d := engine.Build(context.Background(), Reg, fanOut(connector.AtLeastOnce, connector.DestUpsert, spec.DriftIgnore), deps())
	t.Log("Write: upsert ->")
	dump(t, d)
	if !d.HasErrors() {
		t.Fatal("F1 does not reproduce: upsert was accepted against append-only sinks")
	}
	if !has(d, `sink "hostile_queue" does not support`) || !has(d, `sink "hostile_metrics" does not support`) {
		t.Error("F1: expected a mode refusal naming both append-only sinks")
	}
	// Additional observation: the DEAD-LETTER sink is refused too. It writes fault envelopes and has
	// nothing to do with stream "orders", but the stream table is pipeline-global, so a dead-letter
	// destination can never satisfy a pipeline whose data stream is not append-only.
	if !has(d, `sink "hostile_dlq" does not support`) {
		t.Error("F1: expected the dead-letter sink to be caught by the pipeline-wide stream table too")
	}

	// The only other option, and it builds — which is the problem, because the warehouse is now
	// being handed a mode that makes its output wrong. Proven separately below.
	p, neg, d2 := engine.Build(context.Background(), Reg, fanOut(connector.AtLeastOnce, connector.DestAppend, spec.DriftIgnore), deps())
	t.Log("Write: append ->")
	dump(t, d2)
	if d2.HasErrors() {
		t.Fatalf("F1: append should build; the fan-out has no configuration at all if it does not")
	}
	defer func() {
		if p != nil {
			_ = p.Close(context.Background())
		}
	}()
	t.Logf("negotiated: guarantee=%s ack_point=%s durability_edge=%s", neg.Guarantee, neg.AckPoint, neg.DurabilityEdge)

	_ = neg
}

// TestAckPointIsNondeterministic is F11: Negotiated.AckPoint and .DurabilityEdge are single-valued
// strings assigned inside `for id, sk := range res.sinks`, so with four sinks the operator is shown
// whichever branch Go's map iteration happened to visit last. Build the identical spec repeatedly
// and count the distinct answers.
func TestAckPointIsNondeterministic(t *testing.T) {
	edges := map[string]int{}
	points := map[string]int{}
	s := fanOut(connector.AtLeastOnce, connector.DestAppend, spec.DriftIgnore)
	for i := 0; i < 200; i++ {
		p, neg, d := engine.Build(context.Background(), Reg, s, deps())
		if d.HasErrors() {
			t.Fatalf("iteration %d refused: %v", i, d)
		}
		edges[neg.DurabilityEdge]++
		points[neg.AckPoint]++
		_ = p.Close(context.Background())
	}
	t.Logf("F11 over 200 identical Builds: durability_edge=%v ack_point=%v", edges, points)
	if len(edges) == 1 && len(points) == 1 {
		t.Errorf("F11 did not reproduce: got one stable answer %v / %v", edges, points)
	}
}

// TestWarehouseRefusesAppend is the second horn of F1: given the only mode that builds, the sink
// that needs upsert must fail Open rather than write a table of duplicates.
func TestWarehouseRefusesAppend(t *testing.T) {
	e, ok := Reg.Sink("hostile_warehouse")
	if !ok {
		t.Fatal("hostile_warehouse is not registered")
	}
	cfg, diag := e.Spec.Validate(map[string]any{"table": "orders_mirror"})
	if diag.HasErrors() {
		t.Fatalf("config did not validate: %v", diag)
	}
	sk, err := e.New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = sk.Close(context.Background()) }()

	err = sk.Open(context.Background(), nil, connector.Opening{
		Guarantee: connector.AtLeastOnce,
		Streams:   []connector.ConfiguredStream{{Stream: "orders", Mode: connector.DestAppend, Keys: [][]string{{"id"}}}},
	})
	if err == nil {
		t.Fatal("F1: the warehouse accepted append; it should refuse rather than mirror a changelog by appending")
	}
	if fault.ClassOf(err) != fault.PermanentContract {
		t.Errorf("expected PermanentContract, got %s", fault.ClassOf(err))
	}
	t.Logf("F1 second horn: %v", err)
}

// TestExactlyOnceRequestIsRefusedByTheWeakBranches is F2: one requested tier for the whole pipeline
// means the exactly-once branch's request is a hard error for every other branch.
func TestExactlyOnceRequestIsRefusedByTheWeakBranches(t *testing.T) {
	_, _, d := engine.Build(context.Background(), Reg, fanOut(connector.ExactlyOnce, connector.DestUpsert, spec.DriftIgnore), deps())
	dump(t, d)
	if !d.HasErrors() {
		t.Fatal("F2 does not reproduce")
	}
	var named int
	for _, x := range d {
		if x.Iface == "connector.Committer" {
			named++
		}
	}
	if named < 2 {
		t.Errorf("F2: expected a Committer refusal per non-committing sink, got %d", named)
	}
}

// TestNegotiatedGuaranteeCannotReachExactlyOnce is F3, and it is a plain core bug rather than an
// expressiveness gap: engine/negotiate.go folds `effective = effective.Min(AtLeastOnce)` for every
// Replayable source, so Negotiated.Guarantee can never report anything stronger for any pipeline.
//
// Build ACCEPTS the request here — the sink is a Committer, the store is atomic, the source is
// replayable with stable keys — and then reports at_least_once.
func TestNegotiatedGuaranteeCannotReachExactlyOnce(t *testing.T) {
	s := spec.Spec{
		Tenant: "default", ID: "eo", Guarantee: connector.ExactlyOnce, Retry: fault.DefaultRetry,
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: "hostile_cdc",
				Config: map[string]any{"slot": "canal_slot", "stream": "orders"}},
			{ID: "out", Kind: registry.KindSink, Name: "hostile_merge_sink",
				Config: map[string]any{"table": "orders_mirror"},
				Inputs: []spec.Edge{{From: "in"}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "orders",
			Read:   []connector.LaneKind{connector.LaneKindStream},
			Write:  connector.DestAppend,
		}},
	}
	p, neg, d := engine.Build(context.Background(), Reg, s, durableDeps())
	dump(t, d)
	if d.HasErrors() {
		t.Fatal("a single-sink exactly-once pipeline against a Committer sink should build")
	}
	defer func() {
		if p != nil {
			_ = p.Close(context.Background())
		}
	}()
	t.Logf("requested exactly_once, negotiated %s; why=%v", neg.Guarantee, neg.Why)

	// F3's regression assertion. negotiate used to clamp the fold to AtLeastOnce for EVERY
	// Replayable source, so Negotiated.Guarantee could never report anything stronger for any
	// pipeline whatsoever: Build accepted exactly-once and then under-reported it for the
	// pipeline's whole life. Replayability bounds a source at AtLeastOnce only when it is the
	// ONLY evidence available; stable keys reach EffectivelyOnce and a Committer sink reaches
	// ExactlyOnce, which is what the guarantee ladder says.
	if neg.Guarantee != connector.ExactlyOnce {
		t.Fatalf("negotiated %s against a Committer sink, a replayable source with stable keys and an "+
			"atomic store: the fold is clamping again", neg.Guarantee)
	}
	nc, ok := neg.Nodes["out"]
	if !ok {
		t.Fatal("no per-node contract for the sink; Negotiated.Nodes is how a fan-out reports two truths")
	}
	if nc.Guarantee != connector.ExactlyOnce || nc.AckPoint != "commit" {
		t.Fatalf("node out reports %s at %q, want exactly_once at commit", nc.Guarantee, nc.AckPoint)
	}
	if neg.DurabilityEdge != "sink:out" {
		t.Fatalf("durability edge is %q, want sink:out", neg.DurabilityEdge)
	}
}

// TestFanInTopologyIsFine isolates case (2)'s finding: the two-source-one-sink GRAPH is accepted
// with no special case, so what follows is about StreamConfig and not about topology.
func TestFanInTopologyIsFine(t *testing.T) {
	p, neg, d := engine.Build(context.Background(), Reg, fanIn(nil), deps())
	dump(t, d)
	if d.HasErrors() {
		t.Fatal("two sources feeding one sink was refused structurally; that would be a topology finding")
	}
	defer func() {
		if p != nil {
			_ = p.Close(context.Background())
		}
	}()
	t.Logf("fan-in negotiated %s at %s", neg.Guarantee, neg.AckPoint)
}

// TestFanInIsAcceptedWhenStreamsAreNodeScoped is F10's regression test, inverted.
//
// F10 said: spec.StreamConfig had no node scope, so a read mode wanted from ONE source was
// cross-multiplied against every source and the tail-only source was blamed for the other
// source's stream. spec.StreamConfig.Node fixes it, and negotiate became a lookup.
//
// Both halves are asserted, because the cheap wrong fix is to stop checking: an UNSCOPED entry
// asking a tail-only source for a scan must still be refused.
func TestFanInIsAcceptedWhenStreamsAreNodeScoped(t *testing.T) {
	// Scoped: each source is validated against the entry that names it, and nothing else.
	p, neg, d := engine.Build(context.Background(), Reg, fanIn([]spec.StreamConfig{
		{Node: "l", Stream: "left", Read: []connector.LaneKind{connector.LaneKindScan, connector.LaneKindStream}, Write: connector.DestAppend},
		{Node: "r", Stream: "right", Read: []connector.LaneKind{connector.LaneKindStream}, Write: connector.DestAppend},
		{Node: "out", Stream: "left", Write: connector.DestAppend},
		{Node: "out", Stream: "right", Write: connector.DestAppend},
	}), deps())
	dump(t, d)
	if d.HasErrors() {
		t.Fatalf("two merging sources with different lane kinds were refused even with per-node scoping: %s", d.Error())
	}
	if p != nil {
		_ = p.Close(context.Background())
	}
	t.Logf("fan-in with per-node stream scoping negotiated %s over %d branch contracts", neg.Guarantee, len(neg.Nodes))

	// Unscoped, and genuinely wrong: an entry that applies to every node asks the tail-only
	// source for a scan lane it cannot produce. Still refused, and now the message names the node.
	_, _, bad := engine.Build(context.Background(), Reg, fanIn([]spec.StreamConfig{
		{Stream: "left", Read: []connector.LaneKind{connector.LaneKindScan}, Write: connector.DestAppend},
	}), deps())
	if !bad.HasErrors() {
		t.Fatal("an unscoped scan request against a tail-only source must still be refused")
	}
	if !has(bad, `which source "hostile_merge_tail" cannot produce`) {
		t.Errorf("the refusal no longer names the source that cannot comply: %s", bad.Error())
	}
}

// TestSchemaApplierIsPunishedForBeingHonest is F12, which this case found by accident: a sink that
// implements SchemaApplier and declares an HONEST partial SchemaChanges list is REFUSED under the
// DEFAULT drift policy, while the same sink with AppliesSchema false builds. The incentive is
// exactly inverted, and DriftTryEvolve — whose entire documented purpose is "applies what is
// supported and emits an event for the rest" — is validated identically to DriftLenient.
//
// Not fan-out-specific. It bites any pipeline with a schema-applying sink.
func TestSchemaApplierIsNotPunishedForBeingHonest(t *testing.T) {
	for _, pol := range []spec.DriftPolicy{spec.DriftLenient, spec.DriftTryEvolve, spec.DriftEvolve} {
		p, _, diags := engine.Build(context.Background(), Reg,
			fanOut(connector.AtLeastOnce, connector.DestAppend, pol), deps())
		var errs, warns int
		for _, x := range diags {
			if !strings.Contains(x.Message, "drift policy may apply") {
				continue
			}
			if x.Severity == config.SeverityError {
				errs++
			} else {
				warns++
			}
		}
		t.Logf("drift=%s -> %d errors, %d warnings against hostile_warehouse's declared {create_stream, add_field}",
			pol, errs, warns)
		if errs != 0 {
			t.Errorf("drift=%s still REFUSES a sink for declaring an honest partial capability list; "+
				"volunteering a capability must never be worse than withholding it", pol)
		}
		if diags.HasErrors() {
			t.Errorf("drift=%s: the graph does not build: %s", pol, diags.Error())
		}
		if warns == 0 {
			t.Errorf("drift=%s: the gap is not reported at all; a silent gap is the opposite mistake", pol)
		}
		if p != nil {
			_ = p.Close(context.Background())
		}
	}
}

// TestCheckpointCannotSeparateTwoWriterStates is F4. It is asserted against the checkpoint type
// rather than against a running engine, because Pipeline.Run is not implemented yet — but the type
// is the finding: two sinks in this graph declare KeepsState and there is one unkeyed slice.
func TestCheckpointCannotSeparateTwoWriterStates(t *testing.T) {
	var stateful []string
	for _, n := range []string{"hostile_warehouse", "hostile_queue", "hostile_metrics", "hostile_merge_sink", "hostile_dlq"} {
		e, ok := Reg.Sink(n)
		if !ok {
			t.Fatalf("sink %q is not registered", n)
		}
		if e.Caps.KeepsState {
			stateful = append(stateful, n)
		}
	}
	if len(stateful) < 2 {
		t.Fatalf("F4 needs two WriterState sinks in one graph to bite; found %v", stateful)
	}
	t.Logf("F4: %v both implement WriterState and share engine.Checkpoint.WriterState []record.Blob, "+
		"which has no node key while TransformState map[NodeID][]record.Blob does", stateful)
}
