package engine_test

import (
	"context"
	"errors"
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

// record.MarkFailed HAS BEEN A PUBLIC METHOD WITH NO IMPLEMENTATION BEHIND IT.
//
// "Attaches a fault and lets the record continue. The engine's configured routing decides whether
// that means a Failed edge, a drop, or a pipeline stop." Those three are fault.Terminal's three
// values — and the engine read the mark nowhere, so a source could declare a record broken and the
// record was delivered to the destination as though nothing had been said. Silently delivering a
// record its own producer called broken is the worst available outcome and it was the default one.

// markingSource marks the middle record of its one batch.
type markingSource struct {
	total, bad int
	err        error
	done       bool
}

func (s *markingSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil || len(as) > 0 {
		return err
	}
	_, err = rt.Lanes().Announce(ctx, connector.LaneSpec{
		Name: "marked", Stream: "lines", Kind: connector.LaneKindScan,
		Ordering: connector.OrderingPrefix, Boundedness: connector.Bounded, Group: "scan",
	})
	return err
}

func (s *markingSource) Read(_ context.Context, dst *record.Batch) error {
	if s.done {
		return fault.ErrEndOfInput
	}
	s.done = true
	dst.Reset()
	for i := 0; i < s.total; i++ {
		r := dst.Add()
		r.Payload = record.BytesPayload([]byte(fmt.Sprintf("row-%d", i)))
		if i < s.bad {
			// The source itself says this one is not fit to deliver.
			r.MarkFailed(s.err)
		}
	}
	var b [8]byte
	b[7] = byte(s.total)
	dst.Position = record.Position{Token: record.Blob{Version: 1, Bytes: b[:]}, Order: b[:],
		Safe: true, At: time.Now(), Label: "batch 1"}
	return nil
}

func (s *markingSource) Commit(context.Context, connector.Ack) error { return nil }
func (s *markingSource) Close(context.Context) error                 { return nil }

func registerMarkingSource(t *testing.T, s *markingSource) string {
	t.Helper()
	name := fmt.Sprintf("marking_source_%d", controlSeq.Add(1))
	registry.AddSource(registry.Default, registry.SourceDef[*markingSource]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Marking source",
			Summary: "Marks some of its own records failed.",
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
		New: func(context.Context, *config.Config) (*markingSource, error) { return s, nil },
	})
	return name
}

type markedRun struct {
	landed, dead []string
	runErr       error
	abandoned    float64
}

func runMarked(t *testing.T, term fault.Terminal, src *markingSource, withFailedEdge bool) markedRun {
	t.Helper()
	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	srcName := registerMarkingSource(t, src)
	main := &collector{}
	mainName := registerCollector(t, "marked_main", main)
	dlq := &collector{}
	dlqName := registerCollector(t, "marked_dlq", dlq)

	retry := fault.DefaultRetry
	retry.Terminal = term
	s := spec.Spec{
		Tenant: "acme", ID: "mk", Retry: retry,
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: srcName},
			{ID: "out", Kind: registry.KindSink, Name: mainName,
				Config: map[string]any{"codec": map[string]any{"encoder": "raw", "framer": "newline"}},
				Inputs: []spec.Edge{{From: "in"}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "lines", Read: []connector.LaneKind{connector.LaneKindScan},
			Write: connector.DestAppend,
		}},
	}
	if withFailedEdge {
		s.Graph = append(s.Graph, spec.Node{
			ID: "dlq", Kind: registry.KindSink, Name: dlqName,
			Config: map[string]any{"codec": map[string]any{"encoder": "raw", "framer": "newline"}},
			Inputs: []spec.Edge{{From: "in", Select: spec.EdgeFailed}},
		})
	}

	reg := metrics.New()
	p, _, diags := engine.Build(context.Background(), registry.Default, s,
		engine.Deps{State: st, Worker: "test", Metrics: reg,
			FlushInterval: 5 * time.Millisecond, GracePeriod: 2 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	out := markedRun{}
	out.runErr = p.Run(context.Background())
	out.landed, out.dead = main.got(), dlq.got()
	out.abandoned = sumSeriesFor(t, scrape(t, reg), telemetry.MRecordsAbandoned,
		"reason", telemetry.ReasonTerminalFault)
	return out
}

var errSourceSaysBroken = errors.New("the upstream row failed its own validation")

// A MARKED RECORD MUST NOT REACH THE DESTINATION. This is the whole of it: before this, every one
// of them did.
func TestAMarkedRecordIsDeadLetteredAndNotDelivered(t *testing.T) {
	got := runMarked(t, fault.TerminalDeadLetter,
		&markingSource{total: 5, bad: 2, err: fault.New(fault.PermanentMapping, fault.OpRead, errSourceSaysBroken)},
		true)
	if got.runErr != nil {
		t.Fatalf("Run: %v", got.runErr)
	}

	if len(got.landed) != 3 {
		t.Errorf("%d records reached the destination, want the 3 the source did not mark: %v",
			len(got.landed), got.landed)
	}
	for _, ln := range got.landed {
		if ln == "row-0" || ln == "row-1" {
			t.Errorf("%q was marked failed by its own source and was delivered anyway", ln)
		}
	}
	if len(got.dead) != 2 {
		t.Errorf("%d records reached the failed edge, want 2: %v", len(got.dead), got.dead)
	}
	if got.abandoned != 2 {
		t.Errorf("canal_records_abandoned_total{reason=terminal_fault} is %v, want 2", got.abandoned)
	}
}

// THE TERMINAL DISPOSITION IS WHAT DECIDES, which is precisely what MarkFailed's own comment
// promises: "a Failed edge, a drop, or a pipeline stop" are fault.Terminal's three values.
func TestTheTerminalDispositionDecidesWhatAMarkMeans(t *testing.T) {
	drop := runMarked(t, fault.TerminalDrop,
		&markingSource{total: 4, bad: 1, err: fault.New(fault.PermanentMapping, fault.OpRead, errSourceSaysBroken)},
		true)
	if drop.runErr != nil {
		t.Fatalf("drop: Run: %v", drop.runErr)
	}
	if len(drop.landed) != 3 {
		t.Errorf("drop: %d records landed, want 3", len(drop.landed))
	}
	if len(drop.dead) != 0 {
		t.Errorf("drop: %d records reached the failed edge; drop discards rather than routes", len(drop.dead))
	}
	if drop.abandoned != 1 {
		t.Errorf("drop: abandoned is %v, want 1 — a drop is counted, never silent", drop.abandoned)
	}

	stop := runMarked(t, fault.TerminalStop,
		&markingSource{total: 4, bad: 1, err: fault.New(fault.PermanentMapping, fault.OpRead, errSourceSaysBroken)},
		true)
	if stop.runErr == nil {
		t.Error("stop: the run succeeded although terminal is stop and a record was marked failed")
	}
	if stop.runErr != nil && !strings.Contains(stop.runErr.Error(), "marked record") {
		t.Errorf("stop: the error does not say what happened: %v", stop.runErr)
	}
}

// AN UNMARKED BATCH PAYS NOTHING AND CHANGES NOTHING, which is the overwhelmingly common case: no
// source in this module calls MarkFailed at all.
func TestAnUnmarkedBatchIsUntouched(t *testing.T) {
	got := runMarked(t, fault.TerminalDeadLetter, &markingSource{total: 6, bad: 0}, true)
	if got.runErr != nil {
		t.Fatalf("Run: %v", got.runErr)
	}
	if len(got.landed) != 6 {
		t.Errorf("%d records landed, want all 6", len(got.landed))
	}
	if len(got.dead) != 0 || got.abandoned != 0 {
		t.Errorf("an unmarked batch produced %d dead letters and %v abandonments",
			len(got.dead), got.abandoned)
	}
}
