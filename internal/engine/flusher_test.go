package engine_test

import (
	"context"
	"encoding/json"
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
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
)

// A FLUSHER SINK IS NOT DURABLE WHEN WRITE RETURNS, and the whole value of the capability is that
// the engine's accounting knows it.
//
// Before this branch the engine settled on Write returning cleanly and nowhere else, which is why
// engine.Executable had to refuse every sink that earns its acknowledgement later. Settling early
// against a Flusher would be design rule R4's violation with extra steps: the prefix advances, the
// cursor is persisted, the source is told to move on, and a crash before the next flush loses
// everything the sink was still holding.

// flusher is a sink that accepts on Write and is durable on Flush, and lets a test control both.
type flusher struct {
	mu       sync.Mutex
	accepted []string // handed to Write
	durable  []string // covered by a successful Flush
	reasons  []connector.FlushReason

	// flushErr, if set, fails the flush. deferAll holds everything back as Deferred.
	flushErr error
	deferAll bool

	// durableUpTo caps how many accepted records one flush makes durable; zero means all of them.
	//
	// It buys a PARTIAL settlement — some of a lane's groups resolved while the rest are still in
	// flight — which is the state a real sink reaches whenever its own durability boundary falls
	// mid-batch, and the only state in which several of the ledger's rules are distinguishable from
	// their absence.
	durableUpTo int

	// pending is what Write has taken since the last successful Flush, so the fixture can answer
	// Deferred with real record ids.
	pending []record.RecordID
}

func (f *flusher) Open(context.Context, connector.SinkRuntime, connector.Opening) error { return nil }
func (f *flusher) Close(context.Context) error                                          { return nil }

func (f *flusher) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ln := range strings.Split(strings.TrimSuffix(string(req.Body), "\n"), "\n") {
		if ln != "" {
			f.accepted = append(f.accepted, ln)
		}
	}
	for _, ref := range req.Records {
		f.pending = append(f.pending, ref.ID)
	}
	// Accepted, NOT durable. Honest only because SinkCaps.Flushes is declared.
	return connector.AllWritten(req.Count), nil
}

func (f *flusher) Flush(_ context.Context, reason connector.FlushReason) (connector.WriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reasons = append(f.reasons, reason)

	if f.flushErr != nil {
		return connector.WriteResult{}, f.flushErr
	}
	if f.deferAll {
		// Accepted, not durable yet, do NOT resend — the fourth quadrant a coarse Flusher needs.
		out := connector.WriteResult{Deferred: append([]record.RecordID(nil), f.pending...)}
		return out, nil
	}
	// One record per accepted line, so the two slices index alike.
	newly := f.accepted[len(f.durable):]
	n := len(newly)
	if f.durableUpTo > 0 && f.durableUpTo < n {
		n = f.durableUpTo
	}
	f.durable = append(f.durable, newly[:n]...)

	// What this flush did not cover stays pending and is reported as Deferred: accepted, not
	// durable, do not resend.
	deferred := append([]record.RecordID(nil), f.pending[min(n, len(f.pending)):]...)
	f.pending = deferred
	if len(deferred) == 0 {
		return connector.WriteResult{}, nil
	}
	return connector.WriteResult{Deferred: deferred}, nil
}

func (f *flusher) snapshot() (accepted, durable []string, reasons []connector.FlushReason) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.accepted...), append([]string(nil), f.durable...),
		append([]connector.FlushReason(nil), f.reasons...)
}

func (f *flusher) set(fn func(*flusher)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

var flusherSeq = &sinkSeq

func registerFlusher(t *testing.T, prefix string, f *flusher) string {
	t.Helper()
	name := fmt.Sprintf("%s_%d", prefix, flusherSeq.Add(1))
	registry.AddSink(registry.Default, registry.SinkDef[*flusher]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Flusher",
			Summary: "Accepts on Write, durable on Flush.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			Modes:          []connector.DestMode{connector.DestAppend},
			MaxConcurrency: 1,
			Flushes:        true,
		},
		New: func(context.Context, *config.Config) (*flusher, error) { return f, nil },
	})
	return name
}

// cursorCount reports how many lane cursors the store holds a non-zero position for.
//
// This is the assertion that matters: a persisted cursor is canal promising the records behind it
// are safe. If one exists before any flush succeeded, the promise is false.
func cursorCount(t *testing.T, dir string) int {
	t.Helper()
	st, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("reopening the store: %v", err)
	}
	defer st.Close()
	seq, err := st.Range(context.Background(), storeLanePrefix())
	if err != nil {
		t.Fatalf("ranging: %v", err)
	}
	// Decoded rather than pattern-matched, because Announce writes the lane row with a ZERO cursor
	// before a single record moves. A row existing proves nothing; a row carrying a non-empty
	// resume token is the promise this test is about.
	n := 0
	for _, v := range seq {
		var row struct {
			Cursor struct {
				Token struct {
					Bytes []byte `json:"bytes"`
				} `json:"token"`
			} `json:"cursor"`
		}
		if err := json.Unmarshal(v.Value, &row); err != nil {
			t.Fatalf("decoding a lane row: %v", err)
		}
		if len(row.Cursor.Token.Bytes) > 0 {
			n++
		}
	}
	return n
}

// TestAFlushThatNeverSucceedsAdvancesNoCursor is the R4 property for a deferring sink.
//
// The sink accepts every record and its flush always fails. Nothing may be settled, so no prefix
// resolves, so no cursor is ever persisted — and on restart every record is read again. A cursor
// written here would be canal telling the source to discard data no sink ever made durable.
func TestAFlushThatNeverSucceedsAdvancesNoCursor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 12)
	stateDir := filepath.Join(dir, "state")

	f := &flusher{flushErr: fault.Transient(fault.OpFlush, errors.New("disk is unavailable"))}
	name := registerFlusher(t, "flusher_never", f)

	st, err := wal.Open(stateDir)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name, path),
		engine.Deps{State: st, Worker: "test", FlushInterval: 10 * time.Millisecond, GracePeriod: time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	runErr := p.Run(ctx)
	_ = p.Close(context.Background())
	_ = st.Close()

	if runErr == nil {
		t.Error("Run succeeded although the sink never made anything durable")
	}
	accepted, durable, _ := f.snapshot()
	if len(accepted) == 0 {
		t.Fatal("the sink was never written to, so this test proves nothing")
	}
	if len(durable) != 0 {
		t.Fatalf("the fixture reported %d durable records after only failing flushes", len(durable))
	}
	if n := cursorCount(t, stateDir); n != 0 {
		t.Errorf("%d lane cursors were persisted for records no flush ever made durable", n)
	}
}

// The complement: a flush that succeeds settles, and the cursor is then allowed to move.
func TestASuccessfulFlushSettlesAndTheCursorMoves(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	want := writeLines(t, path, 12)
	stateDir := filepath.Join(dir, "state")

	f := &flusher{}
	name := registerFlusher(t, "flusher_ok", f)

	st, err := wal.Open(stateDir)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	p, neg, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name, path),
		engine.Deps{State: st, Worker: "test", FlushInterval: 10 * time.Millisecond, GracePeriod: 5 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	// The negotiation says where durability is earned, and it is no longer refused.
	if neg.AckPoint != "flush" {
		t.Errorf("ack point is %q, want flush", neg.AckPoint)
	}
	if err := engine.Executable(neg); err != nil {
		t.Fatalf("a flush ack point is implemented now, but Executable refused it: %v", err)
	}
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_ = p.Close(context.Background())
	_ = st.Close()

	_, durable, reasons := f.snapshot()
	if len(durable) != len(want) {
		t.Fatalf("%d records durable, want %d", len(durable), len(want))
	}
	if n := cursorCount(t, stateDir); n == 0 {
		t.Error("no cursor was persisted although every record was made durable")
	}

	// A bounded pipeline that reached the end of its input is FINALISING, and a staging sink has to
	// be told that rather than being left to guess from a periodic checkpoint.
	if len(reasons) == 0 || reasons[len(reasons)-1] != connector.FlushEndOfInput {
		t.Errorf("the last flush reason was %v, want end_of_input", reasons)
	}
}

// Deferred is the fourth quadrant: accepted, not durable yet, DO NOT RESEND. It must not settle and
// it must not cause a re-delivery, which is the pair of properties that distinguishes it from
// Failed.
func TestDeferredHoldsWithoutResending(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 10)
	stateDir := filepath.Join(dir, "state")

	f := &flusher{deferAll: true}
	name := registerFlusher(t, "flusher_deferred", f)

	st, err := wal.Open(stateDir)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name, path),
		engine.Deps{State: st, Worker: "test", FlushInterval: 10 * time.Millisecond, GracePeriod: time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = p.Run(ctx)
	_ = p.Close(context.Background())
	_ = st.Close()

	accepted, durable, _ := f.snapshot()
	if len(durable) != 0 {
		t.Errorf("%d records settled although every flush deferred them", len(durable))
	}
	// NOT RESENT is the half that distinguishes Deferred from Failed: the sink must have been
	// handed each record exactly once.
	seen := map[string]int{}
	for _, ln := range accepted {
		seen[ln]++
	}
	for ln, n := range seen {
		if n > 1 {
			t.Errorf("record %q was written %d times; Deferred must not cause a resend", ln, n)
		}
	}
	if n := cursorCount(t, stateDir); n != 0 {
		t.Errorf("%d cursors persisted for records that were only ever deferred", n)
	}
}

// waitForWrite blocks until the sink has been handed at least one record.
//
// SLEEPING AT A STARTUP RACE IS NOT SYNCHRONISATION. This test used to sleep 40ms and cancel,
// which assumes the run is past openSinks, recoverCheckpoint and openSources by then. It is not a
// safe assumption: those three take runCtx, so a cancellation that lands during any of them makes
// run return the open error immediately — before the terminal flush, before the drain, before
// anything. The sink is then genuinely never flushed, and the failure looks exactly like the
// regression this test is here to catch. It passed for months locally and failed in CI, where the
// package binaries run alongside each other and 40ms buys less than it looks like it does.
//
// Waiting on the sink's own state instead makes the precondition an assertion: cancellation cannot
// land before the first write, because the cancel does not happen until the first write has.
func waitForWrite(t *testing.T, f *flusher) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if accepted, _, _ := f.snapshot(); len(accepted) > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the run never reached the sink, so cancelling it proves nothing about the drain")
}

// A cancelled run is DRAINING, not finalising, and a staging sink is right to keep holding an
// undersized artifact it would seal at end of input.
func TestACancelledRunFlushesWithDrain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 200000) // large enough that cancellation lands mid-read

	f := &flusher{}
	name := registerFlusher(t, "flusher_drain", f)

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()
	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name, path),
		engine.Deps{State: st, Worker: "test", FlushInterval: 5 * time.Millisecond, GracePeriod: 5 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	waitForWrite(t, f)
	cancel()
	<-done

	_, _, reasons := f.snapshot()
	if len(reasons) == 0 {
		t.Fatal("the sink was never flushed")
	}
	if last := reasons[len(reasons)-1]; last != connector.FlushDrain {
		t.Errorf("the last flush reason was %v, want drain: a cancelled run is not an ended input", last)
	}
}
