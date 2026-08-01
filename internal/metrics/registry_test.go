package metrics

import (
	"strings"
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

func render(t *testing.T, r *Registry) string {
	t.Helper()
	var b strings.Builder
	if err := r.WriteText(&b); err != nil {
		t.Fatalf("WriteText: %v", err)
	}
	return b.String()
}

// OMIT, DON'T ZERO is the rule the whole metric surface rests on, and it is the one most easily
// broken by a well-meaning exporter that emits every registered metric at zero.
//
// A stalled pipeline reporting canal_checkpoint_age_seconds = 0 says its checkpoint is perfectly
// fresh. Reporting nothing makes `absent()` fire. The second is the design; this test is what keeps
// it true.
func TestAnUnmeasuredMetricProducesNoSeries(t *testing.T) {
	r := New()
	if _, err := r.Gauge(telemetry.MCheckpointAge, telemetry.LabelPipeline, telemetry.LabelLane); err != nil {
		t.Fatalf("Gauge: %v", err)
	}
	if _, err := r.Counter(telemetry.MRecordsRead, telemetry.LabelPipeline); err != nil {
		t.Fatalf("Counter: %v", err)
	}

	out := render(t, r)
	if strings.Contains(out, telemetry.MCheckpointAge) {
		t.Errorf("a registered but never-set gauge appeared in the scrape:\n%s", out)
	}
	if strings.Contains(out, telemetry.MRecordsRead) {
		t.Errorf("a registered but never-incremented counter appeared in the scrape:\n%s", out)
	}
	// Not even the HELP line, because a HELP with no series is still a claim that the metric exists.
	if strings.Contains(out, "# HELP "+telemetry.MCheckpointAge) {
		t.Error("the HELP line was emitted for a metric with no samples")
	}
}

// The other half of the rule: a quantity that STOPS being measurable stops being reported. A lane
// that no longer exists must not keep exporting its last checkpoint age forever.
func TestForgetRemovesASeries(t *testing.T) {
	r := New()
	g, _ := r.Gauge(telemetry.MCheckpointAge, telemetry.LabelPipeline, telemetry.LabelLane)
	g.Set(12, "p1", "7")
	g.Set(3, "p1", "8")

	if out := render(t, r); !strings.Contains(out, `lane="7"`) || !strings.Contains(out, `lane="8"`) {
		t.Fatalf("both lanes should be present:\n%s", out)
	}
	g.Forget("p1", "7")

	out := render(t, r)
	if strings.Contains(out, `lane="7"`) {
		t.Errorf("a forgotten lane is still being reported:\n%s", out)
	}
	if !strings.Contains(out, `lane="8"`) {
		t.Errorf("Forget removed the wrong series:\n%s", out)
	}
}

func TestLabelVocabularyIsClosedAtRegistration(t *testing.T) {
	r := New()
	if _, err := r.Counter(telemetry.MFaults, telemetry.LabelPipeline, "customer_email"); err == nil {
		t.Error("a label outside the vocabulary was accepted; that is a cardinality explosion with a user's data in it")
	}
	if _, err := r.Counter(telemetry.MFaults, telemetry.LabelNode, telemetry.LabelNode); err == nil {
		t.Error("a duplicated label was accepted")
	}
	if _, err := r.Counter(telemetry.MFaults, telemetry.LabelPipeline, telemetry.LabelClass); err != nil {
		t.Errorf("a permitted label set was refused: %v", err)
	}
}

// Two registrations of one name must agree. Two nodes registering the same core metric is normal;
// two registering it with different labels would produce one series family whose rows mean
// different things.
func TestRedefinitionMustAgree(t *testing.T) {
	r := New()
	a, err := r.Counter(telemetry.MRecordsRead, telemetry.LabelPipeline, telemetry.LabelLane)
	if err != nil {
		t.Fatalf("first registration: %v", err)
	}
	b, err := r.Counter(telemetry.MRecordsRead, telemetry.LabelPipeline, telemetry.LabelLane)
	if err != nil {
		t.Fatalf("an identical re-registration must be allowed: %v", err)
	}
	if a.m != b.m {
		t.Error("an identical re-registration returned a different instrument")
	}
	if _, err := r.Counter(telemetry.MRecordsRead, telemetry.LabelPipeline); err == nil {
		t.Error("the same name was accepted with a different label set")
	}
	if _, err := r.Gauge(telemetry.MRecordsRead, telemetry.LabelPipeline, telemetry.LabelLane); err == nil {
		t.Error("the same name was accepted as a different type")
	}
}

// Label VALUES are not closed — a lane id is a number, a node id is operator-chosen — so the series
// map is exactly the unbounded growth design rule R6 forbids. It must refuse and count, not grow.
func TestSeriesAreCappedAndTheTruncationIsVisible(t *testing.T) {
	r := New()
	c, _ := r.Counter(telemetry.MRecordsRead, telemetry.LabelPipeline, telemetry.LabelLane)
	for i := 0; i < maxSeriesPerMetric+50; i++ {
		c.Add(1, "p1", string(rune('a'+i%26))+strings.Repeat("x", i/26))
	}

	c.m.mu.Lock()
	got := len(c.m.series)
	c.m.mu.Unlock()
	if got > maxSeriesPerMetric {
		t.Errorf("the metric holds %d series, above the cap of %d", got, maxSeriesPerMetric)
	}

	// A truncated metric surface must be visible IN the metric surface. Silently dropping series is
	// a lie about the pipeline's shape.
	out := render(t, r)
	if !strings.Contains(out, "canal_metrics_series_dropped_total") {
		t.Fatalf("the drop counter is missing entirely:\n%s", out)
	}
	if strings.Contains(out, "canal_metrics_series_dropped_total 0\n") {
		t.Error("series were dropped but the counter says zero")
	}
}

// A mismatched tuple cannot be attributed to anything. Guessing which labels were meant would put
// wrong values on a real series, so it is dropped and counted.
func TestAWrongLengthLabelTupleIsDroppedNotGuessed(t *testing.T) {
	r := New()
	c, _ := r.Counter(telemetry.MRecordsRead, telemetry.LabelPipeline, telemetry.LabelLane)
	c.Add(1, "p1") // one value for two labels

	out := render(t, r)
	if strings.Contains(out, telemetry.MRecordsRead) {
		t.Errorf("a mismatched tuple produced a series:\n%s", out)
	}
	if strings.Contains(out, "canal_metrics_series_dropped_total 0\n") {
		t.Error("the drop went uncounted")
	}
}

// A connector can never name a core metric, shadow one, or export under a name an operator's
// dashboard already means something by.
func TestConnectorMetricsAreNamespaced(t *testing.T) {
	r := New()
	m := r.ForConnector("postgres_cdc")

	c, err := m.Counter("rows_seen", telemetry.LabelPipeline)
	if err != nil {
		t.Fatalf("Counter: %v", err)
	}
	c.Add(3, "p1")

	out := render(t, r)
	if !strings.Contains(out, "canal_connector_postgres_cdc_rows_seen{pipeline=\"p1\"} 3") {
		t.Errorf("the namespaced series is missing:\n%s", out)
	}

	// A connector trying to be canal_records_read_total gets its own namespaced name instead.
	c2, _ := m.Counter(telemetry.MRecordsRead, telemetry.LabelPipeline)
	c2.Add(99, "p1")
	out = render(t, r)
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, telemetry.MRecordsRead+"{") {
			t.Errorf("a connector wrote to the core's own metric: %s", line)
		}
	}
}

// A connector-supplied name reaching the exposition unchecked lets one broken component produce a
// body no scraper can parse, taking every other metric in the process down with it.
func TestConnectorNamesAreSanitised(t *testing.T) {
	r := New()
	c, err := r.ForConnector("we{ird}").Counter("has spaces\nand\"quotes\"")
	if err != nil {
		t.Fatalf("Counter: %v", err)
	}
	c.Add(1)

	out := render(t, r)
	for _, bad := range []string{"{ird", " spaces", "\"quotes"} {
		if strings.Contains(out, bad) {
			t.Errorf("unsanitised text %q reached the exposition:\n%s", bad, out)
		}
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	r := New()
	c, _ := r.Counter(telemetry.MRecordsRead, telemetry.LabelPipeline)
	c.Add(1, `a"b\c`+"\n"+"d")

	out := render(t, r)
	if !strings.Contains(out, `pipeline="a\"b\\c\nd"`) {
		t.Errorf("a label value with a quote, a backslash and a newline was not escaped:\n%s", out)
	}
}

// A counter that can go down is not a counter, and silently accepting a negative delta produces a
// rate() that reads as a spike on the next scrape.
func TestCounterRefusesToGoBackwards(t *testing.T) {
	r := New()
	c, _ := r.Counter(telemetry.MRecordsRead, telemetry.LabelPipeline)
	c.Add(5, "p1")
	c.Add(-3, "p1")
	if !strings.Contains(render(t, r), telemetry.MRecordsRead+`{pipeline="p1"} 5`) {
		t.Errorf("a negative delta changed a counter:\n%s", render(t, r))
	}
}

func TestHistogramExposesCumulativeBuckets(t *testing.T) {
	r := New()
	h, err := r.Histogram(telemetry.MCommitLatency, []float64{0.1, 1, 10}, telemetry.LabelPhase)
	if err != nil {
		t.Fatalf("Histogram: %v", err)
	}
	h.Observe(0.05, "persist")
	h.Observe(0.5, "persist")
	h.Observe(50, "persist")

	out := render(t, r)
	for _, want := range []string{
		`canal_commit_latency_seconds_bucket{phase="persist",le="0.1"} 1`,
		`canal_commit_latency_seconds_bucket{phase="persist",le="1"} 2`,
		`canal_commit_latency_seconds_bucket{phase="persist",le="10"} 2`,
		`canal_commit_latency_seconds_bucket{phase="persist",le="+Inf"} 3`,
		`canal_commit_latency_seconds_count{phase="persist"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if !strings.Contains(out, `canal_commit_latency_seconds_sum{phase="persist"} 50.55`) {
		t.Errorf("the sum is wrong or missing:\n%s", out)
	}
	if _, err := r.Histogram("canal_x", []float64{10, 1}); err == nil {
		t.Error("descending buckets were accepted")
	}
}

// Two scrapes of an unchanged registry must be byte-identical, or a diff of /metrics is noise and
// the golden-file test pkg/telemetry promises cannot exist.
func TestScrapeIsDeterministic(t *testing.T) {
	r := New()
	c, _ := r.Counter(telemetry.MRecordsRead, telemetry.LabelPipeline, telemetry.LabelLane)
	g, _ := r.Gauge(telemetry.MInFlight, telemetry.LabelPipeline, telemetry.LabelLane)
	for i := 0; i < 20; i++ {
		c.Add(float64(i), "p1", string(rune('a'+i)))
		g.Set(float64(i), "p1", string(rune('a'+i)))
	}
	first := render(t, r)
	for i := 0; i < 10; i++ {
		if got := render(t, r); got != first {
			t.Fatalf("scrape %d differs from the first", i)
		}
	}
}

// Every name in the closed set needs a description. A metric whose meaning nobody wrote down is a
// metric somebody will alert on and misread.
func TestEveryMetricHasHelp(t *testing.T) {
	for _, name := range telemetry.MetricNames {
		if telemetry.MetricHelp[name] == "" {
			t.Errorf("%s has no entry in telemetry.MetricHelp", name)
		}
	}
	for name := range telemetry.MetricHelp {
		if !contains(telemetry.MetricNames, name) {
			t.Errorf("MetricHelp describes %s, which is not in MetricNames", name)
		}
	}
}

func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}
