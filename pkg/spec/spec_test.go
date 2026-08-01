package spec_test

import (
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
)

// pkg/spec is topology as data, and the two functions worth testing are the ones the negotiation
// resolves precedence with. StreamFor's rule — specific beats general — has exactly one
// implementation on purpose, because two call sites resolving it independently is how a fan-out
// branch ends up validated against another branch's mode.

func graph() spec.Spec {
	return spec.Spec{
		Tenant: "acme", ID: "p1",
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: "src"},
			{ID: "mid", Kind: registry.KindTransform, Name: "t", Inputs: []spec.Edge{{From: "in"}}},
			{ID: "a", Kind: registry.KindSink, Name: "s1", Inputs: []spec.Edge{{From: "mid"}}},
			{ID: "b", Kind: registry.KindSink, Name: "s2", Inputs: []spec.Edge{{From: "mid"}}},
		},
	}
}

func TestNodeFindsWhatIsThereAndRefusesWhatIsNot(t *testing.T) {
	g := graph()
	n, ok := g.Node("mid")
	if !ok || n.Name != "t" {
		t.Fatalf("Node(mid) returned (%v, %v)", n, ok)
	}
	if _, ok := g.Node("nope"); ok {
		t.Error("Node found a node that is not in the graph")
	}
}

// Terminals are the nodes nothing consumes — the leaves the negotiation walks back from.
func TestTerminalsAreTheNodesNothingConsumes(t *testing.T) {
	got := map[record.NodeID]bool{}
	g := graph()
	for _, id := range g.Terminals() {
		got[id] = true
	}
	want := map[record.NodeID]bool{"a": true, "b": true}
	if len(got) != len(want) {
		t.Fatalf("terminals are %v, want a and b", got)
	}
	for id := range want {
		if !got[id] {
			t.Errorf("%s is a leaf but was not reported as a terminal", id)
		}
	}
}

func TestConsumersFindsEveryInboundEdge(t *testing.T) {
	g := graph()
	if n := len(g.Consumers("mid")); n != 2 {
		t.Errorf("mid feeds %d nodes, want 2", n)
	}
	if n := len(g.Consumers("a")); n != 0 {
		t.Errorf("a sink feeds %d nodes, want 0", n)
	}
}

// SPECIFIC BEATS GENERAL. A node-scoped entry wins over an unscoped one for the same stream, and
// this is the precedence rule the whole per-node scoping feature rests on.
func TestStreamForPrefersTheNodeScopedEntry(t *testing.T) {
	g := graph()
	g.Streams = []spec.StreamConfig{
		{Stream: "orders", Write: connector.DestAppend},
		{Stream: "orders", Node: "a", Write: connector.DestUpsert},
	}

	scoped, ok := g.StreamFor("a", "orders")
	if !ok {
		t.Fatal("no entry found for the scoped node")
	}
	if scoped.Write != connector.DestUpsert {
		t.Errorf("node a got %s, want the node-scoped upsert", scoped.Write)
	}

	general, ok := g.StreamFor("b", "orders")
	if !ok {
		t.Fatal("no entry found for the unscoped node")
	}
	if general.Write != connector.DestAppend {
		t.Errorf("node b got %s, want the unscoped append", general.Write)
	}

	if _, ok := g.StreamFor("a", "nosuchstream"); ok {
		t.Error("an undeclared stream resolved to something")
	}
}

// Declaration ORDER must not decide the answer. A scoped entry written before the general one has
// to win just the same, or the precedence rule is really "whichever came last".
func TestStreamForIgnoresDeclarationOrder(t *testing.T) {
	g := graph()
	g.Streams = []spec.StreamConfig{
		{Stream: "orders", Node: "a", Write: connector.DestUpsert},
		{Stream: "orders", Write: connector.DestAppend},
	}
	got, ok := g.StreamFor("a", "orders")
	if !ok || got.Write != connector.DestUpsert {
		t.Errorf("got (%v, %v); the scoped entry must win wherever it is written", got.Write, ok)
	}
}

// StreamsFor projects the same precedence over every stream at one node: one entry per stream name,
// the scoped one where it exists.
func TestStreamsForCollapsesToOneEntryPerStream(t *testing.T) {
	g := graph()
	g.Streams = []spec.StreamConfig{
		{Stream: "orders", Write: connector.DestAppend},
		{Stream: "orders", Node: "a", Write: connector.DestUpsert},
		{Stream: "customers", Write: connector.DestAppend},
	}

	got := g.StreamsFor("a")
	if len(got) != 2 {
		t.Fatalf("node a resolved %d streams, want 2 (orders and customers): %+v", len(got), got)
	}
	byName := map[record.StreamName]spec.StreamConfig{}
	for _, sc := range got {
		if _, dup := byName[sc.Stream]; dup {
			t.Errorf("stream %s appears twice; the scoped and unscoped entries were not collapsed", sc.Stream)
		}
		byName[sc.Stream] = sc
	}
	if byName["orders"].Write != connector.DestUpsert {
		t.Errorf("orders resolved to %s at node a, want the scoped upsert", byName["orders"].Write)
	}
	if byName["customers"].Write != connector.DestAppend {
		t.Errorf("customers resolved to %s, want the unscoped append", byName["customers"].Write)
	}
}

// A node with no scoped entries sees exactly the unscoped ones.
func TestStreamsForAnUnscopedNode(t *testing.T) {
	g := graph()
	g.Streams = []spec.StreamConfig{
		{Stream: "orders", Write: connector.DestAppend},
		{Stream: "orders", Node: "a", Write: connector.DestUpsert},
	}
	got := g.StreamsFor("b")
	if len(got) != 1 || got[0].Write != connector.DestAppend {
		t.Errorf("node b resolved %+v, want the one unscoped append", got)
	}
}

func TestAnEmptyGraphAnswersEmptily(t *testing.T) {
	var s spec.Spec
	if len(s.Terminals()) != 0 {
		t.Error("an empty graph has terminals")
	}
	if len(s.Consumers("x")) != 0 {
		t.Error("an empty graph has consumers")
	}
	if _, ok := s.Node("x"); ok {
		t.Error("an empty graph has a node")
	}
	if _, ok := s.StreamFor("x", "y"); ok {
		t.Error("an empty graph resolved a stream")
	}
}
