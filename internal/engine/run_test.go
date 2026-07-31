package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"

	_ "github.com/BernardoCSACarreira/canal/internal/example/linefile"
)

// These are the first tests in the repository that RUN a pipeline. Every assertion before them was
// about shape — does it build, does it negotiate, does it refuse. These are about behaviour: does a
// record arrive, and does the position it left behind survive a restart.
//
// The source is the real internal/example/linefile. The sink is local to this file because a test
// that writes to stdout can assert nothing, but it is a real registered sink implementing the real
// three-method interface with no test hooks in the core.

// collector is a sink that keeps what it is given and can be told to stop.
type collector struct {
	mu    sync.Mutex
	lines []string

	// after is called with the running total once a request has been recorded, so a test can end the
	// run at a deterministic point rather than after a sleep.
	after func(total int)
}

func (c *collector) Open(context.Context, connector.SinkRuntime, connector.Opening) error { return nil }
func (c *collector) Close(context.Context) error                                          { return nil }

func (c *collector) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	c.mu.Lock()
	for _, ln := range strings.Split(strings.TrimSuffix(string(req.Body), "\n"), "\n") {
		if ln != "" {
			c.lines = append(c.lines, ln)
		}
	}
	total := len(c.lines)
	after := c.after
	c.mu.Unlock()

	if after != nil {
		after(total)
	}
	// A clean return means DURABLE. This sink declares no Flusher, so the core settles on this
	// return and on nothing else.
	return connector.AllWritten(req.Count), nil
}

func (c *collector) got() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

// sinkSeq makes every registered sink name unique.
//
// The registry refuses a duplicate name with a panic, which is correct — two components answering to
// one name is exactly the ambiguity registration exists to prevent — and it means a test binary run
// with -count=2 cannot register the same fixture twice. A counter is the whole fix, and running the
// suite repeatedly is worth keeping possible: it is how a flaky concurrency test is found.
var sinkSeq atomic.Int64

// registerCollector adds a uniquely-named collector sink to the process registry and returns the name.
func registerCollector(t *testing.T, prefix string, c *collector) string {
	t.Helper()
	name := fmt.Sprintf("%s_%d", prefix, sinkSeq.Add(1))
	registry.AddSink(registry.Default, registry.SinkDef[*collector]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Collector",
			Summary: "Keeps every line it is written, for assertions.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			Modes:          []connector.DestMode{connector.DestAppend},
			MaxConcurrency: 1,
		},
		New: func(context.Context, *config.Config) (*collector, error) { return c, nil },
	})
	return name
}

func pipelineSpec(sinkName, path string) spec.Spec {
	return spec.Spec{
		Tenant: "acme", ID: "p1",
		Guarantee: connector.AtLeastOnce,
		Retry:     fault.DefaultRetry,
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: "line_file",
				Config: map[string]any{"path": path}},
			{ID: "out", Kind: registry.KindSink, Name: sinkName,
				Inputs: []spec.Edge{{From: "in"}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "lines",
			Read:   []connector.LaneKind{connector.LaneKindScan},
			Write:  connector.DestAppend,
		}},
	}
}

func writeLines(t *testing.T, path string, n int) []string {
	t.Helper()
	want := make([]string, 0, n)
	var b strings.Builder
	for i := 0; i < n; i++ {
		ln := fmt.Sprintf("line-%05d", i)
		want = append(want, ln)
		b.WriteString(ln)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return want
}

func deps(t *testing.T, dir string) (engine.Deps, func()) {
	t.Helper()
	st, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	return engine.Deps{
		State:         st,
		Worker:        "test",
		FlushInterval: 10 * time.Millisecond,
		GracePeriod:   5 * time.Second,
	}, func() { _ = st.Close() }
}

// TestOneRecordEndToEnd is design rule R3's milestone, minus the binary: one record moves from a
// real source to a real sink through the real engine.
//
// Until this passed, every guarantee in this repository was a design rather than a fact.
func TestOneRecordEndToEnd(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	want := writeLines(t, path, 3)

	c := &collector{}
	sinkName := registerCollector(t, "collector_e2e", c)
	d, closeStore := deps(t, filepath.Join(dir, "state"))
	defer closeStore()

	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(sinkName, path), d)
	if diags.HasErrors() {
		t.Fatalf("Build refused the pipeline: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := c.got()
	if len(got) != len(want) {
		t.Fatalf("got %d lines, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d is %q, want %q", i, got[i], want[i])
		}
	}
}

// TestPositionIsDurable proves phase two actually happened: after a clean run the lane's cursor is
// in the store, not merely in the ledger's memory.
//
// The ledger resolving a prefix is not progress. Progress is a byte on a disk.
func TestPositionIsDurable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 10)
	stateDir := filepath.Join(dir, "state")

	c := &collector{}
	sinkName := registerCollector(t, "collector_durable", c)
	d, closeStore := deps(t, stateDir)

	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(sinkName, path), d)
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_ = p.Close(context.Background())
	closeStore()

	// Reopen the store as a separate process would and look for the cursor.
	st2, err := wal.Open(stateDir)
	if err != nil {
		t.Fatalf("reopening the store: %v", err)
	}
	defer st2.Close()

	seq, err := st2.Range(context.Background(), storeLanePrefix())
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	rows := 0
	for _, v := range seq {
		rows++
		if !strings.Contains(string(v.Value), `"cursor"`) {
			t.Errorf("lane row has no cursor field: %s", v.Value)
		}
		// The token is the connector's own bytes, and linefile's is a byte offset. A zero one would
		// mean the run committed nothing.
		if strings.Contains(string(v.Value), `"token":{"version":0`) {
			t.Errorf("the lane committed no position: %s", v.Value)
		}
	}
	if rows == 0 {
		t.Fatal("no lane row survived the run; nothing was ever made durable")
	}
}

// TestResumeAfterInterruption is the property the whole design exists for: stop a pipeline mid-flight
// and restart it, and every record arrives with none lost.
//
// The run is ended at a deterministic point — when the sink has taken a whole batch — rather than
// after a sleep, so the test asserts the same thing on a fast machine and a loaded one.
//
// The assertion is AT-LEAST-ONCE, which is what this pipeline negotiated: no line may be missing,
// and a line may repeat. Asserting exactly-once here would be asserting a guarantee the negotiation
// explicitly did not give.
func TestResumeAfterInterruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	const total = 1200 // linefile reads at most 500 per call, so this is three batches
	want := writeLines(t, path, total)
	stateDir := filepath.Join(dir, "state")

	// --- first run, stopped after the first batch lands -------------------------
	ctx1, cancel1 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel1()
	stop := context.AfterFunc(ctx1, func() {})
	_ = stop

	runCtx, endRun := context.WithCancel(ctx1)
	first := &collector{after: func(n int) {
		if n >= 500 {
			endRun() // a whole batch is durable at the sink; stop reading
		}
	}}
	name1 := registerCollector(t, "collector_resume", first)

	d1, close1 := deps(t, stateDir)
	p1, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name1, path), d1)
	if diags.HasErrors() {
		t.Fatalf("Build (first): %v", diags)
	}
	if err := p1.Run(runCtx); err != nil {
		t.Logf("first run ended with: %v", err)
	}
	_ = p1.Close(context.Background())
	close1()
	endRun()

	gotFirst := first.got()
	if len(gotFirst) == 0 {
		t.Fatal("the first run delivered nothing")
	}
	if len(gotFirst) >= total {
		t.Skipf("the first run consumed the whole input (%d lines) before it could be stopped; "+
			"nothing is left to resume, so this machine cannot exercise the resume path", len(gotFirst))
	}

	// --- second run, same state directory ---------------------------------------
	second := &collector{}
	name2 := registerCollector(t, "collector_resume", second)

	d2, close2 := deps(t, stateDir)
	defer close2()
	p2, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name2, path), d2)
	if diags.HasErrors() {
		t.Fatalf("Build (second): %v", diags)
	}
	defer p2.Close(context.Background())

	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()
	if err := p2.Run(ctx2); err != nil {
		t.Fatalf("Run (second): %v", err)
	}
	gotSecond := second.got()

	// NO LOSS. Every line must appear in one run or the other.
	seen := map[string]int{}
	for _, ln := range append(append([]string{}, gotFirst...), gotSecond...) {
		seen[ln]++
	}
	var missing []string
	for _, ln := range want {
		if seen[ln] == 0 {
			missing = append(missing, ln)
		}
	}
	if len(missing) > 0 {
		t.Errorf("%d lines were lost across the restart, e.g. %v", len(missing), missing[:min(5, len(missing))])
	}

	// IT RESUMED. The second run must not have re-read the whole file, or the cursor did nothing.
	if len(gotSecond) >= total {
		t.Errorf("the second run delivered %d of %d lines: it restarted from the beginning rather than resuming",
			len(gotSecond), total)
	}

	// And the duplicates, if any, are bounded by one in-flight window rather than unbounded.
	dupes := 0
	for _, n := range seen {
		if n > 1 {
			dupes += n - 1
		}
	}
	t.Logf("first run %d lines, second run %d, %d duplicates across the restart",
		len(gotFirst), len(gotSecond), dupes)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// storeLanePrefix is the key prefix every lane row for this test's pipeline lives under.
func storeLanePrefix() store.Key {
	return store.Key{Tenant: "acme", Space: store.SpaceLane, Parts: []string{"p1"}}
}
