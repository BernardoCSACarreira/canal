package engine_test

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/internal/metrics"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// internal/metrics proves the registry works. These prove the ENGINE uses it: that running a real
// pipeline produces real samples, with the right names, of the right things.
//
// The distinction matters because the previous state of this repository was a complete metric
// vocabulary that nothing incremented. A registry with no caller is the same defect as no registry.

// scrape renders a registry the way the HTTP endpoint would.
func scrape(t *testing.T, reg *metrics.Registry) string {
	t.Helper()
	var b strings.Builder
	if err := reg.WriteText(&b); err != nil {
		t.Fatalf("WriteText: %v", err)
	}
	return b.String()
}

// seriesNames returns the distinct metric names present in a scrape, histogram suffixes folded back
// onto the base name.
func seriesNames(body string) []string {
	seen := map[string]bool{}
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, _, _ := strings.Cut(line, "{")
		name, _, _ = strings.Cut(name, " ")
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			if strings.HasSuffix(name, suffix) && !strings.HasSuffix(name, "_total") {
				name = strings.TrimSuffix(name, suffix)
				break
			}
		}
		seen[name] = true
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// value reads one series' value out of a scrape.
func value(t *testing.T, body, prefix string) float64 {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, prefix) {
			_, v, ok := strings.Cut(line, " ")
			if !ok {
				continue
			}
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatalf("value of %q is not a number: %q", prefix, v)
			}
			return f
		}
	}
	t.Fatalf("no series starting %q in:\n%s", prefix, body)
	return 0
}

func has(body, prefix string) bool {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

// runWithMetrics runs a pipeline against a fresh registry and returns the scrape.
func runWithMetrics(t *testing.T, s spec.Spec, dir string) (string, error) {
	t.Helper()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	reg := metrics.New()
	d := engine.Deps{
		State: st, Worker: "test", Metrics: reg,
		FlushInterval: 10 * time.Millisecond, GracePeriod: 5 * time.Second,
	}

	p, _, diags := engine.Build(context.Background(), registry.Default, s, d)
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	runErr := p.Run(ctx)
	return scrape(t, reg), runErr
}

// TestAHealthyRunEmitsItsSeries is the golden-file test pkg/telemetry/metrics.go has always
// promised: "the closed metric-name set, pinned by a golden-file test against real /metrics output".
//
// The NAMES are pinned rather than the numbers, because a throughput figure is a fact about a
// machine and a name is a contract with a dashboard. A name appearing or disappearing is a breaking
// change for whoever is alerting on it, and this test is what makes that require an edit here.
func TestAHealthyRunEmitsItsSeries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	want := writeLines(t, path, 25)

	c := &collector{}
	sinkName := registerCollector(t, "collector_metrics", c)

	body, err := runWithMetrics(t, pipelineSpec(sinkName, path), dir)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := seriesNames(body)
	expect := []string{
		"canal_commit_latency_seconds",
		"canal_metrics_series_dropped_total",
		"canal_records_committed_total",
		"canal_records_read_total",
		"canal_records_written_total",
		"canal_state_persist_staleness_seconds",
	}
	if strings.Join(got, "\n") != strings.Join(expect, "\n") {
		t.Errorf("the series a healthy run emits have changed.\n got: %v\nwant: %v\n"+
			"\nIf this is intended, update the list — a name appearing or disappearing is a breaking "+
			"change for anything alerting on it.", got, expect)
	}

	// Every emitted name must be one the closed set agreed to.
	for _, n := range got {
		if n == "canal_metrics_series_dropped_total" {
			continue // the registry's own meta-series
		}
		if telemetry.MetricHelp[n] == "" {
			t.Errorf("%s is exported but is not in telemetry.MetricNames", n)
		}
	}

	// THE PER-LANE GAUGES ARE ABSENT ON PURPOSE, and it is the most surprising line in this list.
	// This pipeline is bounded: its one lane finished, so its cursor is final and its in-flight count
	// is permanently zero. A checkpoint age that kept climbing for it would page somebody about a
	// batch job that completed exactly as intended, so the series are forgotten rather than frozen.
	// TestCheckpointAgeClimbsWhileNothingHappens covers the live case, which is the one that alarms.
	for _, gauge := range []string{
		telemetry.MCheckpointAge, telemetry.MInFlight,
		telemetry.MInFlightBudget, telemetry.MReplayRecords,
	} {
		if has(body, gauge) {
			t.Errorf("%s is still reported for a lane that finished:\n%s", gauge, body)
		}
	}

	// The counts have to be right, not merely present.
	if v := value(t, body, "canal_records_read_total"); v != float64(len(want)) {
		t.Errorf("canal_records_read_total is %v, want %d", v, len(want))
	}
	if v := value(t, body, "canal_records_written_total"); v != float64(len(want)) {
		t.Errorf("canal_records_written_total is %v, want %d", v, len(want))
	}
}

// The two counters pkg/telemetry says MUST STAY ZERO. Under omit-don't-zero, staying zero means
// being ABSENT from the scrape — which is a far stronger assertion than reading a zero, because a
// zero can also mean "nothing ever incremented this".
func TestTheMustBeZeroCountersAreAbsentOnAHealthyRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 15)

	c := &collector{}
	sinkName := registerCollector(t, "collector_zero", c)
	body, err := runWithMetrics(t, pipelineSpec(sinkName, path), dir)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, name := range []string{
		telemetry.MUnclassified,
		telemetry.MLedgerLeaks,
		telemetry.MRecordsAbandoned,
		telemetry.MFaults,
		telemetry.MAbandonedPluginCalls,
	} {
		if has(body, name) {
			t.Errorf("%s appears on a healthy run:\n%s", name, body)
		}
	}
}

// canal_checkpoint_age_seconds is documented as THE primary alert signal, and the property that
// makes it one is that it climbs WITHOUT anything happening. A checkpoint age updated only when a
// checkpoint is written freezes at its last good value exactly when the pipeline stalls.
func TestCheckpointAgeClimbsWhileNothingHappens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 5)

	// A sink that blocks forever, so the pipeline makes no progress at all after the first batch.
	release := make(chan struct{})
	stuck := &flaky{answer: func(int, []record.Ref) (connector.WriteResult, error) {
		<-release
		return connector.WriteResult{}, nil
	}}
	name := registerFlaky(t, "stuck_sink", stuck, false)

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	reg := metrics.New()
	p, _, diags := engine.Build(context.Background(), registry.Default,
		routeSpec(name, path, fault.DefaultRetry, ""),
		engine.Deps{State: st, Worker: "test", Metrics: reg,
			FlushInterval: 5 * time.Millisecond, GracePeriod: time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Wait for the gauge to appear, then watch it grow with the pipeline entirely stalled.
	var first, second float64
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if body := scrape(t, reg); has(body, telemetry.MCheckpointAge) {
			first = value(t, body, telemetry.MCheckpointAge)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if first == 0 {
		t.Fatal("canal_checkpoint_age_seconds never appeared; a stalled pipeline emits no samples at all")
	}
	time.Sleep(150 * time.Millisecond)
	second = value(t, scrape(t, reg), telemetry.MCheckpointAge)

	if second <= first {
		t.Errorf("checkpoint age went %v -> %v while the pipeline was stalled; it must climb", first, second)
	}

	close(release)
	cancel()
	<-done
}

// A fault has to be COUNTED as well as logged, with the class and the blame that make it
// actionable. Every routing decision was log-only before this.
func TestFaultsAreCountedByClassAndBlame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 6)

	f := &flaky{answer: func(call int, _ []record.Ref) (connector.WriteResult, error) {
		if call == 1 {
			return connector.WriteResult{}, fault.Throttle(fault.OpWrite, 5*time.Millisecond, errors.New("429"))
		}
		return connector.WriteResult{}, nil
	}}
	name := registerFlaky(t, "throttling_sink", f, false)

	body, err := runWithMetrics(t, routeSpec(name, path, retryFast(4, fault.TerminalStop, fault.IndeterminateStall), ""), dir)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !has(body, `canal_faults_total{pipeline="p1",node="out",op="write",class="throttled",blame="upstream"}`) {
		t.Errorf("the throttle was not counted with its class and blame:\n%s", body)
	}
	// Cumulative SECONDS, not attempts: a retry count says retries happened, retry seconds says how
	// much of the pipeline's life is spent waiting.
	if !has(body, `canal_backoff_seconds_total{pipeline="p1",node="out",class="throttled"}`) {
		t.Errorf("the wait was not counted:\n%s", body)
	}
	if v := value(t, body, `canal_backoff_seconds_total`); v <= 0 {
		t.Errorf("backoff seconds is %v, want the time actually spent waiting", v)
	}
}

// An abandoned record must be counted with the reason it was abandoned, because "we dropped 400
// records" and "we dropped 400 records because their retries ran out" are different incidents.
func TestAbandonedRecordsAreCountedWithTheirReason(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 8)

	var poison record.RecordID
	f := &flaky{}
	f.answer = func(_ int, recs []record.Ref) (connector.WriteResult, error) {
		var out connector.WriteResult
		for i, ref := range recs {
			if poison == 0 && i == 3 {
				poison = ref.ID
			}
			if ref.ID == poison {
				out.Failed = append(out.Failed, fault.RecordFault{
					Record: ref.ID, Class: fault.PermanentMapping, Op: fault.OpWrite, User: "unrepresentable",
				})
			}
		}
		return out, nil
	}
	name := registerFlaky(t, "dropping_sink", f, false)

	body, err := runWithMetrics(t, routeSpec(name, path, retryFast(4, fault.TerminalDrop, fault.IndeterminateStall), ""), dir)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !has(body, `canal_records_abandoned_total{pipeline="p1",lane=`) {
		t.Fatalf("no abandoned counter at all:\n%s", body)
	}
	if !strings.Contains(body, `reason="terminal_fault"} 1`) {
		t.Errorf("the abandonment was not attributed to the terminal class:\n%s", body)
	}
}
