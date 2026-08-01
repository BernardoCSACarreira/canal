package example_test

import (
	"context"
	"testing"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	_ "github.com/BernardoCSACarreira/canal/internal/example/linefile"
	"github.com/BernardoCSACarreira/canal/internal/example/memstore"
	_ "github.com/BernardoCSACarreira/canal/internal/example/stdoutsink"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"

	// A byte sink needs an encoder, and this build refuses one that has none registered. Linking
	// the shipped codecs is the same choice a deployment makes; see cmd/canal.
	_ "github.com/BernardoCSACarreira/canal/pkg/codec"
)

// TestRegistrationIsClean asserts that registering the trivial connectors passes the
// registry's own cross-check and spec lint, and that neither over-declares a capability.
//
// Over-declaration panics at init, so reaching this test at all proves half of it; the
// warning list proves the other half — an interface implemented without being declared is
// reported rather than silently active.
func TestRegistrationIsClean(t *testing.T) {
	for _, name := range []string{"line_file"} {
		if _, ok := registry.Default.Source(name); !ok {
			t.Fatalf("source %q is not registered", name)
		}
	}
	if _, ok := registry.Default.Sink("stdout"); !ok {
		t.Fatal("sink \"stdout\" is not registered")
	}
	for _, d := range registry.Default.Descriptors() {
		if len(d.Warnings) != 0 {
			t.Errorf("%s/%s registered with warnings: %v", d.Kind, d.Name, d.Warnings)
		}
		// Every capability report entry must carry a reason when absent and none when
		// present. This is design rule R8 applied to the capability report itself.
		for _, c := range d.Capabilities {
			if c.Present && c.Reason != "" {
				t.Errorf("%s/%s: capability %q is present but carries a reason", d.Kind, d.Name, c.Name)
			}
			if !c.Present && c.Reason == "" {
				t.Errorf("%s/%s: capability %q is absent with no reason", d.Kind, d.Name, c.Name)
			}
		}
	}
}

// TestBuildNegotiatesWalkthroughA builds the two-node pipeline from the architecture's first
// walkthrough and asserts the negotiated contract it documents: at-least-once, earned at the
// sink's write, with the reason for the absent stronger tier named.
func TestBuildNegotiatesWalkthroughA(t *testing.T) {
	s := spec.Spec{
		Tenant:    "default",
		ID:        "p1",
		Guarantee: connector.AtLeastOnce,
		Retry:     fault.DefaultRetry,
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: "line_file",
				Config: map[string]any{"path": "testdata/in.jsonl"}},
			{ID: "out", Kind: registry.KindSink, Name: "stdout",
				Inputs: []spec.Edge{{From: "in", Select: spec.EdgeMain}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "lines",
			Read:   []connector.LaneKind{connector.LaneKindScan},
			Write:  connector.DestAppend,
		}},
	}

	p, neg, diags := engine.Build(context.Background(), registry.Default, s, engine.Deps{State: memstore.New()})
	for _, d := range diags {
		t.Logf("%s [%s] %v: %s", d.Severity, d.Code, d.Path, d.Message)
	}
	if diags.HasErrors() {
		t.Fatal("Build refused a valid two-node pipeline")
	}
	defer func() {
		if p != nil {
			_ = p.Close(context.Background())
		}
	}()

	if neg.Guarantee != connector.AtLeastOnce {
		t.Errorf("negotiated %s, want at_least_once", neg.Guarantee)
	}
	if neg.AckPoint != "write" {
		t.Errorf("ack point %q, want \"write\"", neg.AckPoint)
	}
	if neg.DurabilityEdge != "sink:out" {
		t.Errorf("durability edge %q, want \"sink:out\"", neg.DurabilityEdge)
	}
	if len(neg.Why) == 0 {
		t.Error("negotiation gave no per-factor reasons; an operator cannot see why they got this tier")
	}
}

// TestBuildRefusesExactlyOnce asserts the refusal table's most important row: nothing is named
// exactly-once unless the sink implements Committer or TokenSink, and the diagnostic names the
// interface that would fix it.
func TestBuildRefusesExactlyOnce(t *testing.T) {
	s := spec.Spec{
		Tenant: "default", ID: "p2", Guarantee: connector.ExactlyOnce, Retry: fault.DefaultRetry,
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: "line_file",
				Config: map[string]any{"path": "testdata/in.jsonl"}},
			{ID: "out", Kind: registry.KindSink, Name: "stdout",
				Inputs: []spec.Edge{{From: "in"}}},
		},
	}
	_, _, diags := engine.Build(context.Background(), registry.Default, s, engine.Deps{State: memstore.New()})
	if !diags.HasErrors() {
		t.Fatal("Build accepted exactly-once against a sink that implements neither Committer nor TokenSink")
	}
	var named bool
	for _, d := range diags {
		if d.Iface == "connector.Committer" {
			named = true
		}
	}
	if !named {
		t.Error("the refusal did not name the Go interface that would fix it")
	}
}
