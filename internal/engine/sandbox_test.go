package engine_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
)

// ABANDONING A CALL DOES NOT END IT, and Close used to enter the component anyway.
//
// sandbox gives up on a wedged plugin call and returns, leaving the goroutine carrying it inside the
// connector — its documented, counted cost. The host then does what the API says and calls
// Pipeline.Close, which called the connector's Close while the abandoned Open was still assigning
// its fields. The race detector finds it on a real pipeline by sweeping a cancel across the startup
// window: linefile writes s.f in Open and reads it in Close with nothing between them, and every
// connector in the module has that shape. A connector author could not defend against it, because
// the contract never said the two could overlap.
//
// These two tests are the guarantee [connector.Source.Open] now states, in both its halves: a call
// that unwinds is waited for and the component IS closed, and a call that never comes back is not
// entered at all. They assert it directly rather than relying on the race detector noticing, because
// a timing race that reproduces one run in twenty is not a regression test.

// wedgedSource parks in Open until it is released, and records whether Close ever ran while it was
// still in there.
type wedgedSource struct {
	entered chan struct{}
	release chan struct{}

	inOpen  atomic.Bool
	overlap atomic.Bool
	closed  atomic.Bool
}

func newWedgedSource() *wedgedSource {
	return &wedgedSource{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *wedgedSource) Open(ctx context.Context, _ connector.SourceRuntime) error {
	s.inOpen.Store(true)
	defer s.inOpen.Store(false)
	close(s.entered)

	// DELIBERATELY NOT SELECTING ON ctx. This is the connector that does not respect cancellation,
	// which is the whole reason sandbox abandons calls — a source that unwinds promptly never
	// produces the state under test.
	<-s.release
	return ctx.Err()
}

func (s *wedgedSource) Read(ctx context.Context, _ *record.Batch) error { return ctx.Err() }
func (s *wedgedSource) Commit(context.Context, connector.Ack) error     { return nil }

func (s *wedgedSource) Close(context.Context) error {
	if s.inOpen.Load() {
		s.overlap.Store(true)
	}
	s.closed.Store(true)
	return nil
}

func registerWedgedSource(t *testing.T, s *wedgedSource) string {
	t.Helper()
	name := fmt.Sprintf("wedged_source_%d", sinkSeq.Add(1))
	registry.AddSource(registry.Default, registry.SourceDef[*wedgedSource]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Wedged source",
			Summary: "Parks in Open until released, and reports whether Close overlapped it.",
			Notes:   "Origin.Key is unused; this source never produces a record.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SourceCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering:   connector.OrderingPrefix,
			Boundedness:       []connector.Boundedness{connector.Unbounded},
			LaneKinds:         []connector.LaneKind{connector.LaneKindStream},
			MaxLanes:          1,
			StableKeys:        true,
			Replayable:        true,
			UpstreamRetention: connector.RetentionUnbounded,
		},
		New: func(context.Context, *config.Config) (*wedgedSource, error) { return s, nil },
	})
	return name
}

// startWedged builds and runs a pipeline whose source parks in Open, cancels it, and returns once
// Run has given up on the call. The source is still inside Open when this returns.
func startWedged(t *testing.T, grace time.Duration) (*engine.Pipeline, *wedgedSource) {
	t.Helper()
	dir := t.TempDir()
	src := newWedgedSource()
	srcName := registerWedgedSource(t, src)
	sinkName := registerCollector(t, "wedged_sink", &collector{})

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the state store: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	s := pipelineSpec(sinkName, filepath.Join(dir, "unused.txt"))
	s.Graph[0].Name, s.Graph[0].Config = srcName, map[string]any{}
	s.Streams = []spec.StreamConfig{{
		Stream: "lines",
		Read:   []connector.LaneKind{connector.LaneKindStream},
		Write:  connector.DestAppend,
	}}

	p, _, diags := engine.Build(context.Background(), registry.Default, s,
		engine.Deps{State: st, Worker: "test", GracePeriod: grace})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Gated on the source having ENTERED Open, so the cancel below lands inside the call rather than
	// before or after it. That is the whole window this test is about.
	select {
	case <-src.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the source never entered Open")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run never returned; sandbox is supposed to abandon a call it cannot cancel")
	}
	if !src.inOpen.Load() {
		t.Fatal("the source left Open on its own, so nothing was abandoned and this test proves nothing")
	}
	return p, src
}

// A CALL THAT NEVER COMES BACK MEANS THE COMPONENT IS NOT CLOSED. Leaking a file handle until the
// process exits is a far smaller harm than a second goroutine inside a connector that is
// demonstrably not expecting one.
func TestCloseDoesNotEnterAComponentWithAnAbandonedCallInside(t *testing.T) {
	// A short grace period, because the wait for the abandoned call comes out of it and this source
	// is never going to come back.
	p, src := startWedged(t, 50*time.Millisecond)

	start := time.Now()
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waited := time.Since(start)

	if src.overlap.Load() {
		t.Error("Close ran while the abandoned Open was still executing.\n" +
			"  Two of the core's goroutines were inside one component at once, on fields the " +
			"connector protects with nothing, because the contract never told it to")
	}
	if src.closed.Load() {
		t.Error("Close was called on a component whose abandoned call has not returned; the only " +
			"safe answer is not to enter it at all")
	}
	if waited < 50*time.Millisecond {
		t.Errorf("Close returned after %v without spending the grace period; it did not wait for "+
			"the abandoned call at all", waited)
	}

	// Released only now, so the assertions above ran with the call genuinely still inside.
	close(src.release)
}

// THE SHAPE THE DEFECT WAS ACTUALLY FOUND IN, kept because the two tests above assert the mechanism
// and this one asserts that nothing reaches a connector around it.
//
// A real pipeline with a real connector, Ctrl-C'd at every point across its startup window, then
// closed — which is what a host does and what no test did. It is only meaningful under -race: the
// overlap it catches is a plain unsynchronised read of a field, and the sweep is what puts the cancel
// inside linefile's Open often enough for the detector to see it. Reverting the fix fails it every
// run; before the fix existed it failed on main.
func TestCancellingAcrossTheStartupWindowThenClosingIsRaceFree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 3)
	name := registerCollector(t, "sweep_sink", &collector{})

	for i := 0; i < 150; i++ {
		st, err := wal.Open(filepath.Join(t.TempDir(), "state"))
		if err != nil {
			t.Fatalf("opening the state store: %v", err)
		}
		p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name, path),
			engine.Deps{State: st, Worker: "test", GracePeriod: time.Second})
		if diags.HasErrors() {
			t.Fatalf("Build: %v", diags)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- p.Run(ctx) }()
		// Swept rather than fixed: the window that matters is the microseconds linefile spends inside
		// Open, and no single delay lands in it reliably on every machine.
		time.Sleep(time.Duration(i%75) * 10 * time.Microsecond)
		cancel()
		<-done

		if err := p.Close(context.Background()); err != nil {
			t.Fatalf("iteration %d: Close: %v", i, err)
		}
		st.Close()
	}
}

// AND THE OTHER HALF: a call that unwinds is waited for, and the component IS closed. Without this
// the fix could be "never close anything that was ever abandoned", which would leak every
// component in every cancelled run.
func TestCloseWaitsForAnAbandonedCallAndThenClosesTheComponent(t *testing.T) {
	p, src := startWedged(t, 10*time.Second)

	// The connector comes back a moment after the core gave up on it, which is what a cancelled call
	// normally does. Close must notice and close it rather than leak it.
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(src.release)
	}()

	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if src.overlap.Load() {
		t.Error("Close ran while the abandoned Open was still executing")
	}
	if !src.closed.Load() {
		t.Error("the component was never closed although its abandoned call returned; waiting for a " +
			"call to unwind must not turn into leaking every component a cancelled run touched")
	}
}
