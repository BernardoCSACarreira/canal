package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/internal/example/filesink"
	"github.com/BernardoCSACarreira/canal/internal/metrics"
	pushsource "github.com/BernardoCSACarreira/canal/internal/stress/push-source"
	"github.com/BernardoCSACarreira/canal/pkg/codec"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
)

// THIS IS THE TEST OF THE CLAIM THE WHOLE ARCHITECTURE RESTS ON.
//
// The README's first paragraph, and the reason the registry, the capability negotiation and the
// *Runtime interfaces exist at all, is that adding a source or a sink means implementing an
// interface and registering it — no core changes, no per-connector branches anywhere in the engine.
//
// Nothing had ever tested that. There was one real source and one real sink, written alongside the
// core by the same hand at the same time, and eight connectors in internal/stress written to be
// deliberately hostile — none of which had ever moved a record.
//
// So this runs one of the hostile ones, UNMODIFIED, through the real engine. internal/stress/
// push-source is not touched by this branch; if the core needs a change to accept it, that is the
// finding, and it is worth more than the test passing.
//
// What it exercises that nothing had before:
//
//   - A DISCRETE-ORDERED LANE. Every previous run was prefix-ordered. Discrete settles by handle
//     through Ledger.emitDiscrete rather than by advancing a prefix, and that path had no caller.
//   - AN UNBOUNDED LANE. linefile is a bounded scan that ends; this one never does, so the run
//     stops on cancellation and the drain has to work with a source that is still listening.
//   - A BLOCKING READ. linefile always has an answer. This one parks until a peer posts, which is
//     what makes "Commit must not be serialised behind Read" a real constraint rather than a note.
//   - CONNECTOR-OWNED METRICS. push-source registers its own counters and histogram through
//     rt.Metrics(), which until this branch was a noop that dropped them.
//   - A SINK WHOSE OUTPUT OUTLIVES THE PROCESS, so the assertion is about a file rather than
//     about a buffer the test harness owns.

// thirdPartyRegistry is a private registry holding exactly the catalogue this test needs.
//
// Private rather than registry.Default because AddSource panics on a duplicate name, and a stress
// connector registered into the process registry by one test would be there for every other.
func thirdPartyRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	r := registry.New()
	pushsource.Register(r)
	filesink.Register(r)
	codec.Register(r)
	return r
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestAHostileThirdPartyConnectorRunsUnmodified(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "received.ndjson")
	addr := freePort(t)

	s := spec.Spec{
		Tenant: "acme", ID: "ingress",
		Guarantee: connector.AtLeastOnce,
		Retry:     fault.DefaultRetry,
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: pushsource.Name, Config: map[string]any{
				"listen":      addr,
				"path":        "/ingest",
				"stream":      "events",
				"lane_name":   "ingress",
				"ack_timeout": "10s",
			}},
			{ID: "out", Kind: registry.KindSink, Name: filesink.Name, Config: map[string]any{
				"path":  out,
				"codec": map[string]any{"encoder": "raw", "framer": "newline"},
			}, Inputs: []spec.Edge{{From: "in"}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "events",
			Read:   []connector.LaneKind{connector.LaneKindStream},
			Write:  connector.DestAppend,
		}},
	}

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	reg := metrics.New()
	p, neg, diags := engine.Build(context.Background(), thirdPartyRegistry(t), s, engine.Deps{
		State: st, Worker: "test", Metrics: reg,
		FlushInterval: 10 * time.Millisecond, GracePeriod: 5 * time.Second,
	})
	if diags.HasErrors() {
		t.Fatalf("Build refused a pipeline made only of registered components: %v", diags)
	}
	defer p.Close(context.Background())
	t.Logf("negotiated %s: %v", neg.Guarantee, neg.Why)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(ctx) }()

	// The source binds its listener in Open, so wait for the port rather than for a fixed sleep.
	base := "http://" + addr + "/ingest"
	select {
	case err := <-runDone:
		t.Fatalf("Run returned before the source ever listened: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	waitForListener(t, addr)

	// Each POST stays open until the record has SETTLED end to end, so a 204 is an assertion by
	// itself: the record reached the file and the file was fsynced before this returned.
	const n = 40
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		codes  = map[int]int{}
		client = &http.Client{Timeout: 20 * time.Second}
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"seq":%d}`, i)
			resp, err := client.Post(base, "application/json", strings.NewReader(body))
			if err != nil {
				mu.Lock()
				codes[-1]++
				mu.Unlock()
				return
			}
			defer resp.Body.Close()
			mu.Lock()
			codes[resp.StatusCode]++
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if codes[http.StatusNoContent] != n {
		t.Errorf("got %v, want %d x 204 — a 204 is the peer being told the record is durable", codes, n)
	}

	cancel()
	if err := <-runDone; err != nil && !isCancellation(err) {
		t.Fatalf("Run: %v", err)
	}

	// The file is the assertion. It outlives the process, so this is about bytes on a disk rather
	// than about a buffer the harness happens to own.
	got := readLines(t, out)
	if len(got) != n {
		t.Fatalf("the file holds %d records, want %d", len(got), n)
	}
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		want = append(want, fmt.Sprintf(`{"seq":%d}`, i))
	}
	sort.Strings(got)
	sort.Strings(want)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %d is %q, want %q", i, got[i], want[i])
		}
	}

	// The connector's own metrics are namespaced under its component name and nothing else. Until
	// this branch rt.Metrics() was a noop that accepted every call and dropped it.
	var b strings.Builder
	if err := reg.WriteText(&b); err != nil {
		t.Fatalf("WriteText: %v", err)
	}
	if !strings.Contains(b.String(), "canal_connector_http_push_") {
		t.Errorf("the connector's own metrics are missing from the scrape:\n%s", b.String())
	}
}

func waitForListener(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the source never listened on %s", addr)
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var out []string
	for _, ln := range bytes.Split(bytes.TrimSuffix(b, []byte("\n")), []byte("\n")) {
		if len(ln) > 0 {
			out = append(out, string(ln))
		}
	}
	return out
}

func isCancellation(err error) bool {
	return strings.Contains(err.Error(), context.Canceled.Error())
}
