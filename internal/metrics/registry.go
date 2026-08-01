// Package metrics is canal's in-process metric registry and its Prometheus exposition.
//
// It exists because pkg/telemetry has always declared twenty-six metric names, a closed label
// vocabulary and the rule that governs both — and nothing recorded a single sample. The engine's
// runtime handed connectors a noopMetrics that accepted every call and dropped it. Everything a
// pipeline knew about itself was in a log line.
//
// # Three rules, all from pkg/telemetry, all enforced here
//
//   - THE CORE OWNS NAMING. A connector cannot name a core metric or invent a label. Its metrics are
//     namespaced canal_connector_<component>_<metric> automatically, by [Registry.ForConnector], and
//     a name outside the core's list is refused rather than exported under a name nobody agreed to.
//
//   - THE LABEL VOCABULARY IS CLOSED, and checked at registration rather than at scrape. A label
//     that can grow with the data is a cardinality explosion waiting for a busy Tuesday.
//
//   - AN UNMEASURABLE QUANTITY IS OMITTED, NEVER ZERO. A gauge that has never been set exports no
//     series at all, and [Gauge.Forget] removes one that has stopped being measurable. This is the
//     rule that makes canal_checkpoint_age_seconds trustworthy as the primary alert: a stalled
//     pipeline that reports zero lag is worse than one that reports nothing.
//
// # Bounded by construction
//
// Label VALUES are not closed — a lane id is a number, a node id is operator-chosen — so a series
// map keyed on them is exactly the unbounded growth design rule R6 forbids. Every metric caps its
// series count and REFUSES beyond it, counting the refusals, rather than growing until the process
// dies.
package metrics

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// maxSeriesPerMetric bounds the label-value combinations one metric may hold.
//
// A pipeline's topology is small — nodes and lanes are tens, not thousands — so this is far above
// any legitimate use and far below the point at which a runaway label value costs the process its
// memory. Crossing it is a defect somewhere, so it is counted and logged rather than absorbed.
const maxSeriesPerMetric = 4096

// Kind is a metric's exposition type.
type Kind uint8

const (
	KindCounter Kind = iota
	KindGauge
	KindHistogram
)

func (k Kind) String() string {
	switch k {
	case KindCounter:
		return "counter"
	case KindGauge:
		return "gauge"
	default:
		return "histogram"
	}
}

// Registry holds every metric this process exports.
type Registry struct {
	mu      sync.Mutex
	metrics map[string]*metric

	// dropped counts series refused by the cap, across every metric. It is exported as its own
	// series so that a truncated metric surface is visible IN the metric surface, rather than being
	// a silent lie about a pipeline's shape.
	dropped uint64
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{metrics: map[string]*metric{}}
}

// metric is one named series family.
type metric struct {
	name    string
	kind    Kind
	labels  []string
	buckets []float64

	mu     sync.Mutex
	series map[string]*sample
}

// sample is one label-value combination.
type sample struct {
	values []string

	// count and sum serve counters and gauges (count alone) and histograms (both).
	count   float64
	sum     float64
	buckets []uint64
}

// register creates or returns a metric, refusing a redefinition that disagrees.
func (r *Registry) register(name string, kind Kind, buckets []float64, labels []string) (*metric, error) {
	if err := checkLabels(labels); err != nil {
		return nil, fmt.Errorf("metrics: %s: %w", name, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if m, ok := r.metrics[name]; ok {
		// Two registrations of one name must agree, or a scrape produces a series family whose rows
		// have different meanings under one name. Returning the existing instrument is deliberate:
		// two nodes registering the same core metric is normal and correct.
		if m.kind != kind || !slices.Equal(m.labels, labels) {
			return nil, fmt.Errorf(
				"metrics: %s is already registered as a %s with labels %v; cannot re-register as a %s with %v",
				name, m.kind, m.labels, kind, labels)
		}
		return m, nil
	}

	m := &metric{
		name: name, kind: kind, labels: slices.Clone(labels),
		buckets: slices.Clone(buckets), series: map[string]*sample{},
	}
	r.metrics[name] = m
	return m, nil
}

// checkLabels enforces the closed vocabulary.
func checkLabels(labels []string) error {
	seen := map[string]bool{}
	for _, l := range labels {
		if !telemetry.LabelPermitted(l) {
			return fmt.Errorf("label %q is not in the closed vocabulary %v", l, telemetry.Labels)
		}
		if seen[l] {
			return fmt.Errorf("label %q is declared twice", l)
		}
		seen[l] = true
	}
	return nil
}

// find locates or creates the series for a label-value tuple, honouring the cap.
func (m *metric) find(r *Registry, values []string) *sample {
	if len(values) != len(m.labels) {
		// A mismatched tuple cannot be attributed to anything, and guessing which labels were meant
		// would put wrong values on a real series. Dropping it is the only honest answer.
		r.countDrop()
		return nil
	}
	key := strings.Join(values, "\x00")

	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.series[key]; ok {
		return s
	}
	if len(m.series) >= maxSeriesPerMetric {
		r.countDrop()
		return nil
	}
	s := &sample{values: slices.Clone(values)}
	if m.kind == KindHistogram {
		s.buckets = make([]uint64, len(m.buckets))
	}
	m.series[key] = s
	return s
}

func (r *Registry) countDrop() {
	r.mu.Lock()
	r.dropped++
	r.mu.Unlock()
}

// Counter is a monotonically increasing series.
type Counter struct {
	r *Registry
	m *metric
}

// Counter registers or returns a counter.
func (r *Registry) Counter(name string, labels ...string) (*Counter, error) {
	m, err := r.register(name, KindCounter, nil, labels)
	if err != nil {
		return nil, err
	}
	return &Counter{r: r, m: m}, nil
}

// Add increases the series named by labelValues.
//
// A negative delta is ignored rather than applied: a counter that can go down is not a counter, and
// silently accepting one produces a rate() that reads as a spike on the next scrape.
func (c *Counter) Add(delta float64, labelValues ...string) {
	if c == nil || delta < 0 {
		return
	}
	s := c.m.find(c.r, labelValues)
	if s == nil {
		return
	}
	c.m.mu.Lock()
	s.count += delta
	c.m.mu.Unlock()
}

// Gauge is a point-in-time value that may be absent.
type Gauge struct {
	r *Registry
	m *metric
}

// Gauge registers or returns a gauge.
func (r *Registry) Gauge(name string, labels ...string) (*Gauge, error) {
	m, err := r.register(name, KindGauge, nil, labels)
	if err != nil {
		return nil, err
	}
	return &Gauge{r: r, m: m}, nil
}

// Set records the current value.
func (g *Gauge) Set(v float64, labelValues ...string) {
	if g == nil {
		return
	}
	s := g.m.find(g.r, labelValues)
	if s == nil {
		return
	}
	g.m.mu.Lock()
	s.count = v
	g.m.mu.Unlock()
}

// Forget removes a series, so a quantity that has STOPPED being measurable stops being reported.
//
// This is the other half of omit-don't-zero, and it is why the core's gauge type is richer than
// connector.Gauge: a lane that no longer exists must not keep exporting its last checkpoint age
// forever, and setting it to zero would say the checkpoint is perfectly fresh.
func (g *Gauge) Forget(labelValues ...string) {
	if g == nil || len(labelValues) != len(g.m.labels) {
		return
	}
	key := strings.Join(labelValues, "\x00")
	g.m.mu.Lock()
	delete(g.m.series, key)
	g.m.mu.Unlock()
}

// Histogram is a bucketed distribution.
type Histogram struct {
	r *Registry
	m *metric
}

// DefaultBuckets spans a millisecond to a minute, which is the range a commit, a flush and a
// restart all fall in.
var DefaultBuckets = []float64{
	.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60,
}

// Histogram registers or returns a histogram. Buckets are upper bounds, ascending.
func (r *Registry) Histogram(name string, buckets []float64, labels ...string) (*Histogram, error) {
	if len(buckets) == 0 {
		buckets = DefaultBuckets
	}
	if !sort.Float64sAreSorted(buckets) {
		return nil, fmt.Errorf("metrics: %s: buckets must ascend, got %v", name, buckets)
	}
	m, err := r.register(name, KindHistogram, buckets, labels)
	if err != nil {
		return nil, err
	}
	return &Histogram{r: r, m: m}, nil
}

// Observe records one measurement.
func (h *Histogram) Observe(v float64, labelValues ...string) {
	if h == nil {
		return
	}
	s := h.m.find(h.r, labelValues)
	if s == nil {
		return
	}
	// The bucket counts are stored NON-cumulatively and accumulated at exposition. Storing them
	// cumulatively here and accumulating again in writeInto counted every observation once per
	// bucket it fell under, so three observations reported five in the top bucket.
	i := sort.SearchFloat64s(h.m.buckets, v)
	h.m.mu.Lock()
	s.count++
	s.sum += v
	if i < len(s.buckets) {
		s.buckets[i]++
	}
	h.m.mu.Unlock()
}

// ObserveSince is the common case: how long something took.
func (h *Histogram) ObserveSince(start time.Time, labelValues ...string) {
	h.Observe(time.Since(start).Seconds(), labelValues...)
}

// --- the connector-facing view --------------------------------------------------------------

// ForConnector returns a [connector.Metrics] whose every name is namespaced to one component.
//
// A connector can therefore never collide with a core metric, never shadow one, and never export
// under a name an operator's dashboard already means something by. The component name is sanitised
// rather than trusted: a registered component name reaches this from a registry entry, but a
// metric name is a string a connector author types.
func (r *Registry) ForConnector(component string) connector.Metrics {
	return connectorView{r: r, prefix: "canal_connector_" + sanitise(component) + "_"}
}

type connectorView struct {
	r      *Registry
	prefix string
}

func (v connectorView) Counter(name string, labels ...string) (connector.Counter, error) {
	return v.r.Counter(v.prefix+sanitise(name), labels...)
}

func (v connectorView) Gauge(name string, labels ...string) (connector.Gauge, error) {
	return v.r.Gauge(v.prefix+sanitise(name), labels...)
}

func (v connectorView) Histogram(name string, buckets []float64, labels ...string) (connector.Histogram, error) {
	return v.r.Histogram(v.prefix+sanitise(name), buckets, labels...)
}

// sanitise reduces a string to the characters a Prometheus name allows.
//
// A connector-supplied name reaching the exposition unchecked would let one broken component
// produce a body no scraper can parse, taking every other metric in the process down with it.
func sanitise(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" {
		return "unnamed"
	}
	if out[0] >= '0' && out[0] <= '9' {
		return "_" + out
	}
	return out
}

// --- exposition ------------------------------------------------------------------------------

// WriteText renders the registry in the Prometheus text exposition format.
//
// Metrics and series are both sorted, so two scrapes of an unchanged registry are byte-identical.
// That is what lets the golden-file test pkg/telemetry has always promised actually exist.
func (r *Registry) WriteText(w io.Writer) error {
	r.mu.Lock()
	names := slices.Sorted(maps.Keys(r.metrics))
	ms := make([]*metric, 0, len(names))
	for _, n := range names {
		ms = append(ms, r.metrics[n])
	}
	dropped := r.dropped
	r.mu.Unlock()

	var b strings.Builder
	for _, m := range ms {
		m.writeInto(&b)
	}

	// Always emitted, including at zero: this one is a statement about the metric surface itself,
	// and "no series were dropped" is a measurement rather than an absence.
	fmt.Fprintf(&b, "# HELP canal_metrics_series_dropped_total Series refused because a metric hit its cardinality cap.\n")
	fmt.Fprintf(&b, "# TYPE canal_metrics_series_dropped_total counter\n")
	fmt.Fprintf(&b, "canal_metrics_series_dropped_total %s\n", num(float64(dropped)))

	_, err := io.WriteString(w, b.String())
	return err
}

func (m *metric) writeInto(b *strings.Builder) {
	// SNAPSHOT BY VALUE, not by pointer. Copying the *sample pointers under the lock and then
	// reading their fields outside it is a data race against every concurrent Add and Set — the
	// race detector caught exactly that here, between the flush loop refreshing gauges and a
	// scrape rendering them. A scrape must never be able to tear a counter it is reading.
	m.mu.Lock()
	keys := slices.Sorted(maps.Keys(m.series))
	rows := make([]sample, 0, len(keys))
	for _, k := range keys {
		src := m.series[k]
		rows = append(rows, sample{
			values:  src.values, // written once at creation, never mutated
			count:   src.count,
			sum:     src.sum,
			buckets: slices.Clone(src.buckets),
		})
	}
	m.mu.Unlock()

	// OMIT, DON'T ZERO. A metric with no samples writes nothing at all — not even its HELP — so a
	// quantity nothing has measured is absent from the scrape rather than present and wrong.
	if len(rows) == 0 {
		return
	}

	if help := telemetry.MetricHelp[m.name]; help != "" {
		fmt.Fprintf(b, "# HELP %s %s\n", m.name, help)
	}
	fmt.Fprintf(b, "# TYPE %s %s\n", m.name, m.kind)

	for _, s := range rows {
		if m.kind != KindHistogram {
			fmt.Fprintf(b, "%s%s %s\n", m.name, labelSet(m.labels, s.values, "", ""), num(s.count))
			continue
		}
		var cumulative uint64
		for i, ub := range m.buckets {
			cumulative += s.buckets[i]
			fmt.Fprintf(b, "%s_bucket%s %d\n", m.name,
				labelSet(m.labels, s.values, "le", num(ub)), cumulative)
		}
		fmt.Fprintf(b, "%s_bucket%s %d\n", m.name, labelSet(m.labels, s.values, "le", "+Inf"), uint64(s.count))
		fmt.Fprintf(b, "%s_sum%s %s\n", m.name, labelSet(m.labels, s.values, "", ""), num(s.sum))
		fmt.Fprintf(b, "%s_count%s %s\n", m.name, labelSet(m.labels, s.values, "", ""), num(s.count))
	}
}

// labelSet renders {a="1",b="2"}, optionally with one extra pair appended for a histogram's le.
func labelSet(names, values []string, extraName, extraValue string) string {
	if len(names) == 0 && extraName == "" {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		v := ""
		if i < len(values) {
			v = values[i]
		}
		fmt.Fprintf(&b, "%s=\"%s\"", n, escape(v))
	}
	if extraName != "" {
		if len(names) > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=\"%s\"", extraName, escape(extraValue))
	}
	b.WriteByte('}')
	return b.String()
}

// escape applies the three substitutions the text format requires of a label value.
//
// Not %q: Go's quoting escapes far more than Prometheus asks for, and would turn a legitimate UTF-8
// label value into a run of \uXXXX. Exactly three characters are special here.
func escape(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(v)
}

// num renders a float the way the text format expects, without an exponent for ordinary values and
// without a trailing ".0" on an integer.
func num(f float64) string {
	if f == float64(int64(f)) && f < 1e15 && f > -1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
