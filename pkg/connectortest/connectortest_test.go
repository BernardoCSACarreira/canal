package connectortest_test

import (
	"context"
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/connectortest"
)

// pkg/connectortest exists so that adding a method to a *Runtime interface does not break every
// connector's test suite. That promise is only kept if these types actually SATISFY the interfaces
// — and nothing asserted that they do, which is the one failure mode a package of embeddable stubs
// has.

func TestTheStubsSatisfyTheRuntimeInterfaces(t *testing.T) {
	var (
		_ connector.SourceRuntime = (*connectortest.SourceRuntime)(nil)
		_ connector.SinkRuntime   = (*connectortest.SinkRuntime)(nil)
		_ connector.Metrics       = connectortest.Metrics{}
	)
}

// A stub that panics on use is worse than no stub: a connector's test would fail inside the test
// double rather than in the connector. Every accessor has to answer something.
func TestAZeroSourceRuntimeAnswersRatherThanPanics(t *testing.T) {
	rt := &connectortest.SourceRuntime{}

	if rt.Context() == nil {
		t.Error("Context returned nil; a connector storing it would nil-panic later")
	}
	if rt.Log() == nil {
		t.Error("Log returned nil")
	}
	if rt.Metrics() == nil {
		t.Error("Metrics returned nil")
	}
	if rt.Lanes() == nil {
		t.Error("Lanes returned nil")
	}
	if rt.State() == nil {
		t.Error("State returned nil")
	}
	if rt.Schemas() == nil {
		t.Error("Schemas returned nil")
	}
	_ = rt.Streams()
	_ = rt.Node()
	_ = rt.Pipeline()
	_ = rt.Tenant()

	if _, err := rt.Config(context.Background()); err != nil {
		t.Errorf("Config on a zero runtime returned an error: %v", err)
	}
}

func TestAZeroSinkRuntimeAnswersRatherThanPanics(t *testing.T) {
	rt := &connectortest.SinkRuntime{}
	if rt.Context() == nil || rt.Log() == nil || rt.Metrics() == nil || rt.Schemas() == nil {
		t.Error("a zero sink runtime returned a nil accessor")
	}
	_ = rt.Streams()
}

// Note is how a connector raises an event, and a stub that discarded them would make an
// event-raising connector untestable.
func TestNoteRecordsTheEvent(t *testing.T) {
	rt := &connectortest.SourceRuntime{}
	rt.Note(connector.Event{Message: "hello"})
	if len(rt.Events) != 1 || rt.Events[0].Message != "hello" {
		t.Errorf("events are %+v, want the one that was raised", rt.Events)
	}
}

// The metric stubs accept every call and record nothing, which is what lets a connector that
// instruments itself be unit-tested without a registry.
func TestTheMetricStubsAcceptEveryCall(t *testing.T) {
	m := connectortest.Metrics{}

	c, err := m.Counter("x", "pipeline")
	if err != nil || c == nil {
		t.Fatalf("Counter: (%v, %v)", c, err)
	}
	c.Add(1, "p")

	g, err := m.Gauge("y")
	if err != nil || g == nil {
		t.Fatalf("Gauge: (%v, %v)", g, err)
	}
	g.Set(1)

	h, err := m.Histogram("z", nil)
	if err != nil || h == nil {
		t.Fatalf("Histogram: (%v, %v)", h, err)
	}
	h.Observe(1)
}
