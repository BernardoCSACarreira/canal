package engine_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
)

// The table in retry_test.go proves the DECISION. These prove the ACTION: that a decision reaches a
// running pipeline and changes what arrives at a sink. A routing table nothing consults is the same
// defect as a routing table that does not exist.

// flaky is a sink whose behaviour a test scripts per call.
//
// It is a byte sink like the collector, and it answers with whatever the script says: an error for
// the whole call, or a WriteResult naming individual records. Both shapes matter, because the
// engine has to treat them as the same event at different granularities.
type flaky struct {
	mu    sync.Mutex
	lines []string
	calls int

	// answer is consulted on every Write, with the 1-based call number and the records it carries.
	// Returning a nil error and a zero WriteResult means "accept everything".
	answer func(call int, recs []record.Ref) (connector.WriteResult, error)
}

func (f *flaky) Open(context.Context, connector.SinkRuntime, connector.Opening) error { return nil }
func (f *flaky) Close(context.Context) error                                          { return nil }

func (f *flaky) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	answer := f.answer
	f.mu.Unlock()

	res, err := connector.WriteResult{}, error(nil)
	if answer != nil {
		res, err = answer(call, req.Records)
	}
	if err != nil {
		return connector.WriteResult{}, err
	}

	// Only the records that were not named as failed actually land.
	failed := map[record.RecordID]bool{}
	for _, rf := range res.Failed {
		failed[rf.Record] = true
	}
	kept := 0
	f.mu.Lock()
	for i, ln := range strings.Split(strings.TrimSuffix(string(req.Body), "\n"), "\n") {
		if ln == "" || i >= len(req.Records) {
			continue
		}
		if failed[req.Records[i].ID] {
			continue
		}
		f.lines = append(f.lines, ln)
		kept++
	}
	f.mu.Unlock()

	res.Written = int64(kept)
	return res, nil
}

func (f *flaky) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.lines...)
}

func (f *flaky) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// registerFlaky adds a uniquely-named flaky sink, optionally declaring itself idempotent.
func registerFlaky(t *testing.T, prefix string, f *flaky, idempotent bool) string {
	t.Helper()
	name := fmt.Sprintf("%s_%d", prefix, sinkSeq.Add(1))
	registry.AddSink(registry.Default, registry.SinkDef[*flaky]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Flaky",
			Summary: "Fails on a script, so the routing tree can be observed.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			Modes:          []connector.DestMode{connector.DestAppend},
			MaxConcurrency: 1,
			Idempotent:     idempotent,
		},
		New: func(context.Context, *config.Config) (*flaky, error) { return f, nil },
	})
	return name
}

// routeSpec is pipelineSpec with a retry policy and an optional dead-letter sink.
func routeSpec(sinkName, path string, retry fault.RetryPolicy, deadLetter string) spec.Spec {
	codec := map[string]any{"codec": map[string]any{"encoder": "raw", "framer": "newline"}}
	s := spec.Spec{
		Tenant: "acme", ID: "p1",
		Guarantee: connector.AtLeastOnce,
		Retry:     retry,
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: "line_file",
				Config: map[string]any{"path": path}},
			{ID: "out", Kind: registry.KindSink, Name: sinkName,
				Config: codec, Inputs: []spec.Edge{{From: "in", Select: spec.EdgeMain}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "lines",
			Read:   []connector.LaneKind{connector.LaneKindScan},
			Write:  connector.DestAppend,
		}},
	}
	if deadLetter != "" {
		s.Graph = append(s.Graph, spec.Node{
			ID: "dlq", Kind: registry.KindSink, Name: deadLetter,
			Config: codec, Inputs: []spec.Edge{{From: "in", Select: spec.EdgeFailed}},
		})
	}
	return s
}

func retryFast(max int, term fault.Terminal, ind fault.Indeterminacy) fault.RetryPolicy {
	return fault.RetryPolicy{
		MaxAttempts:     max,
		Backoff:         fault.Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond, Multiplier: 2},
		Terminal:        term,
		OnIndeterminate: ind,
	}
}

// runRouted builds and runs a pipeline over n lines and returns Run's error.
func runRouted(t *testing.T, s spec.Spec, dir string) error {
	t.Helper()
	d, closeStore := deps(t, filepath.Join(dir, "state"))
	defer closeStore()

	p, _, diags := engine.Build(context.Background(), registry.Default, s, d)
	if diags.HasErrors() {
		t.Fatalf("Build refused the pipeline: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return p.Run(ctx)
}

// TestTransientWriteIsRetried is the headline. Before the routing tree, the first transient error
// from a sink ended the run and everything after it was never read.
func TestTransientWriteIsRetried(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	want := writeLines(t, path, 20)

	f := &flaky{answer: func(call int, _ []record.Ref) (connector.WriteResult, error) {
		if call <= 2 {
			return connector.WriteResult{}, fault.Transient(fault.OpWrite, errors.New("503 from the destination"))
		}
		return connector.WriteResult{}, nil
	}}
	name := registerFlaky(t, "flaky_transient", f, false)

	if err := runRouted(t, routeSpec(name, path, retryFast(4, fault.TerminalStop, fault.IndeterminateStall), ""), dir); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := f.got()
	if len(got) != len(want) {
		t.Fatalf("got %d lines, want %d — a retried batch must arrive in full", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("line %d is %q, want %q", i, got[i], want[i])
		}
	}
	if f.callCount() < 3 {
		t.Errorf("the sink was called %d times; two failures plus a success is at least 3", f.callCount())
	}
}

// TestRetriesAreExhaustedIntoTheTerminal proves the budget is real: a sink that never recovers must
// not retry forever, because "retry forever" is the one option RetryPolicy refuses to express.
func TestRetriesAreExhaustedIntoTheTerminal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 5)

	f := &flaky{answer: func(int, []record.Ref) (connector.WriteResult, error) {
		return connector.WriteResult{}, fault.Transient(fault.OpWrite, errors.New("permanently down"))
	}}
	name := registerFlaky(t, "flaky_exhausted", f, false)

	err := runRouted(t, routeSpec(name, path, retryFast(3, fault.TerminalStop, fault.IndeterminateStall), ""), dir)
	if err == nil {
		t.Fatal("Run succeeded against a sink that never accepted anything")
	}
	if !strings.Contains(err.Error(), "permanently down") {
		t.Errorf("the error does not carry the sink's own reason: %v", err)
	}
	// Three attempts total, not three retries: MaxAttempts counts the first try.
	if c := f.callCount(); c != 3 {
		t.Errorf("the sink was called %d times, want 3 (max_attempts)", c)
	}
}

// TestPoisonRecordIsDroppedAndTheRestArrive is the per-record path, and the reason it exists.
//
// Before this, a WriteResult naming one failed record settled it Abandoned on the spot with no
// retry and no consultation of the terminal policy. That is right only by accident when the class
// is terminal, and silent loss when it is not.
func TestPoisonRecordIsDroppedAndTheRestArrive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	want := writeLines(t, path, 10)

	var poison record.RecordID
	f := &flaky{}
	f.answer = func(_ int, recs []record.Ref) (connector.WriteResult, error) {
		// Reject the third record of the first request it appears in, and keep rejecting it.
		var out connector.WriteResult
		for i, ref := range recs {
			if poison == 0 && i == 2 {
				poison = ref.ID
			}
			if ref.ID == poison {
				out.Failed = append(out.Failed, fault.RecordFault{
					Record: ref.ID, Class: fault.PermanentMapping, Op: fault.OpWrite,
					User: "this record has a field the destination cannot represent",
				})
			}
		}
		return out, nil
	}
	name := registerFlaky(t, "flaky_poison", f, false)

	if err := runRouted(t, routeSpec(name, path, retryFast(4, fault.TerminalDrop, fault.IndeterminateStall), ""), dir); err != nil {
		t.Fatalf("Run: %v — a dropped poison record must not stop the pipeline", err)
	}

	got := f.got()
	if len(got) != len(want)-1 {
		t.Fatalf("got %d lines, want %d: exactly one record should have been dropped", len(got), len(want)-1)
	}
	// A PermanentMapping is terminal, so it must be dropped on its FIRST failure. Retrying it would
	// mean several calls carrying it.
	if c := f.callCount(); c != 1 {
		t.Errorf("the sink was called %d times; a terminal class must not be retried", c)
	}
}

// TestDeadLetterRouteDelivers proves the failed edge is a real destination and not a synonym for
// drop. The record must ARRIVE somewhere.
func TestDeadLetterRouteDelivers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	want := writeLines(t, path, 8)

	var poison record.RecordID
	main := &flaky{}
	main.answer = func(_ int, recs []record.Ref) (connector.WriteResult, error) {
		var out connector.WriteResult
		for i, ref := range recs {
			if poison == 0 && i == 1 {
				poison = ref.ID
			}
			if ref.ID == poison {
				out.Failed = append(out.Failed, fault.RecordFault{
					Record: ref.ID, Class: fault.PermanentMapping, Op: fault.OpWrite,
					User: "unrepresentable",
				})
			}
		}
		return out, nil
	}
	dlq := &flaky{}

	mainName := registerFlaky(t, "flaky_main", main, false)
	dlqName := registerFlaky(t, "flaky_dlq", dlq, false)

	s := routeSpec(mainName, path, retryFast(4, fault.TerminalDeadLetter, fault.IndeterminateStall), dlqName)
	if err := runRouted(t, s, dir); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := len(main.got()); got != len(want)-1 {
		t.Errorf("the main sink got %d lines, want %d", got, len(want)-1)
	}
	dead := dlq.got()
	if len(dead) != 1 {
		t.Fatalf("the dead-letter sink got %d records, want 1: %v", len(dead), dead)
	}
	if dead[0] != want[1] {
		t.Errorf("the dead-letter sink got %q, want the rejected record %q", dead[0], want[1])
	}
}

// TestFailedEdgeSinkGetsNoMainRecords is the edge-selection guard.
//
// Every sink in the graph used to receive every batch, which is accidentally right when all edges
// carry main and catastrophic the moment one does not: a dead-letter destination would have
// received the entire healthy stream.
func TestFailedEdgeSinkGetsNoMainRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 12)

	main := &flaky{}
	dlq := &flaky{}
	mainName := registerFlaky(t, "edge_main", main, false)
	dlqName := registerFlaky(t, "edge_dlq", dlq, false)

	// Nothing fails, so the dead-letter sink must never be written to at all.
	s := routeSpec(mainName, path, retryFast(4, fault.TerminalDeadLetter, fault.IndeterminateStall), dlqName)
	if err := runRouted(t, s, dir); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := len(main.got()); got != 12 {
		t.Errorf("the main sink got %d lines, want 12", got)
	}
	if c := dlq.callCount(); c != 0 {
		t.Errorf("the failed-edge sink was written to %d times on a healthy run; it must carry only failed records", c)
	}
}

// TestIndeterminateWriteStopsANonIdempotentSink. Failing loud on an ambiguous write is the correct
// default for a data-movement tool, and the error has to be actionable: it must name the record and
// the setting that changes the behaviour.
func TestIndeterminateWriteStopsANonIdempotentSink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 4)

	f := &flaky{answer: func(int, []record.Ref) (connector.WriteResult, error) {
		return connector.WriteResult{}, fault.Unknown(fault.OpWrite, errors.New("timed out after the request was sent"))
	}}
	name := registerFlaky(t, "flaky_indeterminate", f, false)

	err := runRouted(t, routeSpec(name, path, retryFast(4, fault.TerminalStop, fault.IndeterminateStall), ""), dir)
	if err == nil {
		t.Fatal("Run succeeded after a write that may or may not have landed")
	}
	for _, want := range []string{"may or may not", "on_indeterminate"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not mention %q, so it does not tell the operator what to do:\n%v", want, err)
		}
	}
	if c := f.callCount(); c != 1 {
		t.Errorf("the sink was called %d times; a stall must not re-send an ambiguous write", c)
	}
}

// TestIndeterminateWriteRetriesAgainstAnIdempotentSink is the same fault with one capability
// changed. The sink absorbs duplicates, so re-sending is safe and the pipeline stays up.
func TestIndeterminateWriteRetriesAgainstAnIdempotentSink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	want := writeLines(t, path, 6)

	f := &flaky{answer: func(call int, _ []record.Ref) (connector.WriteResult, error) {
		if call == 1 {
			return connector.WriteResult{}, fault.Unknown(fault.OpWrite, errors.New("timed out"))
		}
		return connector.WriteResult{}, nil
	}}
	name := registerFlaky(t, "flaky_idempotent", f, true)

	if err := runRouted(t, routeSpec(name, path, retryFast(4, fault.TerminalStop, fault.IndeterminateStall), ""), dir); err != nil {
		t.Fatalf("Run: %v — an idempotent sink is what makes an indeterminate write retryable", err)
	}
	if got := len(f.got()); got != len(want) {
		t.Errorf("got %d lines, want %d", got, len(want))
	}
}
