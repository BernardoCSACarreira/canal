package engine_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/internal/metrics"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// The half of the clock policy that IS observable from outside the engine: which records reach
// which sink, and the counter. The record-level effects are asserted in clock_internal_test.go,
// because record.Ref carries neither an event time nor a field change and a sink cannot see them.

// skewSource emits one batch of sane records followed by skewed ones.
type skewSource struct {
	normal, skewed int
	ahead          time.Duration
	done           bool
}

func (s *skewSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil || len(as) > 0 {
		return err
	}
	_, err = rt.Lanes().Announce(ctx, connector.LaneSpec{
		Name: "skew", Stream: "lines", Kind: connector.LaneKindScan,
		Ordering: connector.OrderingPrefix, Boundedness: connector.Bounded, Group: "scan",
	})
	return err
}

func (s *skewSource) Read(_ context.Context, dst *record.Batch) error {
	if s.done {
		return fault.ErrEndOfInput
	}
	s.done = true
	now := time.Now()
	dst.Reset()
	for i := 0; i < s.normal; i++ {
		r := dst.Add()
		r.Payload = record.BytesPayload([]byte(fmt.Sprintf("sane-%d", i)))
		r.EventTime = now.Add(-time.Minute)
	}
	for i := 0; i < s.skewed; i++ {
		r := dst.Add()
		r.Payload = record.BytesPayload([]byte(fmt.Sprintf("skewed-%d", i)))
		r.EventTime = now.Add(s.ahead)
	}
	var b [8]byte
	b[7] = byte(s.normal + s.skewed)
	dst.Position = record.Position{Token: record.Blob{Version: 1, Bytes: b[:]}, Order: b[:],
		Safe: true, At: now, Label: "batch 1"}
	return nil
}

func (s *skewSource) Commit(context.Context, connector.Ack) error { return nil }
func (s *skewSource) Close(context.Context) error                 { return nil }

func registerSkewSource(t *testing.T, s *skewSource) string {
	t.Helper()
	name := fmt.Sprintf("skew_source_%d", controlSeq.Add(1))
	registry.AddSource(registry.Default, registry.SourceDef[*skewSource]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Skew source",
			Summary: "Stamps event times ahead of the local clock.",
			Notes:   "Origin.Key is the row index, stable across re-reads.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SourceCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering:   connector.OrderingPrefix,
			Boundedness:       []connector.Boundedness{connector.Bounded},
			LaneKinds:         []connector.LaneKind{connector.LaneKindScan},
			MaxLanes:          1,
			StableKeys:        true,
			Replayable:        true,
			UpstreamRetention: connector.RetentionUnbounded,
		},
		New: func(context.Context, *config.Config) (*skewSource, error) { return s, nil },
	})
	return name
}

// REJECT ROUTES THE RECORD ON THE FAILED EDGE, which is what the policy says and what distinguishes
// it from clamp: the record does not reach the destination at all, and it is not silently gone
// either.
func TestRejectRoutesSkewedRecordsToTheFailedEdge(t *testing.T) {
	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	srcName := registerSkewSource(t, &skewSource{normal: 3, skewed: 2, ahead: time.Hour})
	main := &collector{}
	mainName := registerCollector(t, "clock_main", main)
	dlq := &collector{}
	dlqName := registerCollector(t, "clock_dlq", dlq)

	s := spec.Spec{
		Tenant: "acme", ID: "clk",
		Retry: fault.DefaultRetry,
		Clock: spec.ClockPolicy{MaxSkew: time.Second, Behaviour: spec.ClockReject},
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: srcName},
			{ID: "out", Kind: registry.KindSink, Name: mainName,
				Config: map[string]any{"codec": map[string]any{"encoder": "raw", "framer": "newline"}},
				Inputs: []spec.Edge{{From: "in"}}},
			{ID: "dlq", Kind: registry.KindSink, Name: dlqName,
				Config: map[string]any{"codec": map[string]any{"encoder": "raw", "framer": "newline"}},
				Inputs: []spec.Edge{{From: "in", Select: spec.EdgeFailed}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "lines", Read: []connector.LaneKind{connector.LaneKindScan},
			Write: connector.DestAppend,
		}},
	}

	reg := metrics.New()
	p, _, diags := engine.Build(context.Background(), registry.Default, s,
		engine.Deps{State: st, Worker: "test", Metrics: reg,
			FlushInterval: 5 * time.Millisecond, GracePeriod: 2 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	landed := main.got()
	if len(landed) != 3 {
		t.Errorf("%d records reached the destination, want the 3 within tolerance: %v", len(landed), landed)
	}
	for _, ln := range landed {
		if strings.HasPrefix(ln, "skewed") {
			t.Errorf("%q reached the destination under a reject policy", ln)
		}
	}

	// NOT SILENTLY GONE. The failed edge is where a rejected record goes, and a reject with nowhere
	// to route would be a drop wearing a policy's name.
	dead := dlq.got()
	if len(dead) != 2 {
		t.Errorf("%d records reached the failed edge, want 2: %v", len(dead), dead)
	}

	body := scrape(t, reg)
	if n := sumSeriesFor(t, body, telemetry.MClockSkew, "outcome", telemetry.ClockRejected); n != 2 {
		t.Errorf("canal_clock_skew_records_total{outcome=rejected} is %v, want 2", n)
	}
}

// PASS IS THE ARM THAT IS INVISIBLE WITHOUT A COUNTER. It accepts implausible timestamps by design,
// so how often it fired is the only question an operator has about it — and there was no metric in
// the closed set that could carry the answer until this one was added.
func TestPassIsCountedEvenThoughNothingChanges(t *testing.T) {
	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	srcName := registerSkewSource(t, &skewSource{normal: 1, skewed: 4, ahead: 6 * time.Hour})
	sink := &collector{}
	sinkName := registerCollector(t, "clock_pass", sink)

	s := spec.Spec{
		Tenant: "acme", ID: "clk",
		Retry: fault.DefaultRetry,
		Clock: spec.ClockPolicy{MaxSkew: time.Second, Behaviour: spec.ClockPass},
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: srcName},
			{ID: "out", Kind: registry.KindSink, Name: sinkName,
				Config: map[string]any{"codec": map[string]any{"encoder": "raw", "framer": "newline"}},
				Inputs: []spec.Edge{{From: "in"}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "lines", Read: []connector.LaneKind{connector.LaneKindScan},
			Write: connector.DestAppend,
		}},
	}

	reg := metrics.New()
	p, _, diags := engine.Build(context.Background(), registry.Default, s,
		engine.Deps{State: st, Worker: "test", Metrics: reg,
			FlushInterval: 5 * time.Millisecond, GracePeriod: 2 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := len(sink.got()); got != 5 {
		t.Errorf("%d records landed, want all 5: pass accepts them", got)
	}
	body := scrape(t, reg)
	if n := sumSeriesFor(t, body, telemetry.MClockSkew, "outcome", telemetry.ClockPassed); n != 4 {
		t.Errorf("canal_clock_skew_records_total{outcome=passed} is %v, want 4", n)
	}
}

// sumSeriesFor totals one metric's series carrying a given label value.
func sumSeriesFor(t *testing.T, body, metric, label, value string) float64 {
	t.Helper()
	var total float64
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, metric+"{") || !strings.Contains(line, label+`="`+value+`"`) {
			continue
		}
		i := strings.LastIndex(line, " ")
		if i < 0 {
			continue
		}
		var v float64
		if _, err := fmt.Sscanf(strings.TrimSpace(line[i+1:]), "%g", &v); err != nil {
			t.Fatalf("parsing %q: %v", line, err)
		}
		total += v
	}
	return total
}
