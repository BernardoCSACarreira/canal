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

// WHEN_FULL WAS OFFERED IN TWO PLACES AND READ IN NEITHER.
//
// spec.Spec.WhenFull is pipeline-wide policy and pkg/registry adds when_full to EVERY node's
// stage-standard config form, so it appears in the generated JSON Schema and on the operator's
// submit screen. Nothing in the engine read either one: whatever an operator chose, admission
// blocked. That is the same defect as Deps.FlushRecords, which was declared, never read, and cost a
// thirty-second test run before anyone noticed.
//
// Blocking is the default and the only policy that never loses data. The others SHED, and a shed is
// a configured loss: counted, logged at ERROR, and reported to the source through Ack.Abandoned so a
// destructive-commit source can refuse to advance.

// floodSource fills a lane far faster than a blocked sink can drain it.
type floodSource struct {
	mu       sync.Mutex
	produced int
	perBatch int
	release  chan struct{}
	seq      uint64
}

func (s *floodSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil || len(as) > 0 {
		return err
	}
	_, err = rt.Lanes().Announce(ctx, connector.LaneSpec{
		Name: "flood", Stream: "lines", Kind: connector.LaneKindStream,
		Ordering: connector.OrderingPrefix, Boundedness: connector.Unbounded, Group: "tail",
	})
	return err
}

func (s *floodSource) Read(ctx context.Context, dst *record.Batch) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return fault.ErrEndOfInput
	default:
	}
	s.mu.Lock()
	s.seq++
	seq := s.seq
	s.produced += s.perBatch
	s.mu.Unlock()

	dst.Reset()
	for i := 0; i < s.perBatch; i++ {
		if r := dst.Add(); r != nil {
			r.Payload = record.BytesPayload([]byte(fmt.Sprintf("row-%d-%d", seq, i)))
		}
	}
	var b [8]byte
	for i := 0; i < 8; i++ {
		b[7-i] = byte(seq >> (8 * i))
	}
	dst.Position = record.Position{Token: record.Blob{Version: 1, Bytes: b[:]}, Order: b[:],
		Safe: true, At: time.Now(), Label: fmt.Sprintf("batch %d", seq)}
	return nil
}

func (s *floodSource) Commit(context.Context, connector.Ack) error { return nil }
func (s *floodSource) Close(context.Context) error                 { return nil }

func (s *floodSource) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.produced
}

func registerFloodSource(t *testing.T, s *floodSource) string {
	t.Helper()
	name := fmt.Sprintf("flood_source_%d", controlSeq.Add(1))
	registry.AddSource(registry.Default, registry.SourceDef[*floodSource]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Flood source",
			Summary: "Produces faster than any sink can drain.",
			Notes:   "Origin.Key is the batch and row index, stable across re-reads.",
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
		New: func(context.Context, *config.Config) (*floodSource, error) { return s, nil },
	})
	return name
}

// floodSpec wires the flood source into a sink that never returns, with a tiny lane budget so the
// lane fills within a few batches.
func floodSpec(srcName, sinkName string, w connector.WhenFull, nodeOverride string) spec.Spec {
	s := spec.Spec{
		Tenant: "acme", ID: "wf",
		Retry:      fault.DefaultRetry,
		WhenFull:   w,
		LaneBudget: 8,
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: srcName},
			{ID: "out", Kind: registry.KindSink, Name: sinkName,
				Config: map[string]any{"codec": map[string]any{"encoder": "raw", "framer": "newline"}},
				Inputs: []spec.Edge{{From: "in"}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "lines",
			Read:   []connector.LaneKind{connector.LaneKindStream},
			Write:  connector.DestAppend,
		}},
	}
	if nodeOverride != "" {
		s.Graph[0].Config = map[string]any{"when_full": nodeOverride}
	}
	return s
}

// A BLOCKING POLICY BLOCKS, which is the behaviour every other case is measured against. The source
// is stopped at the budget and not one record is dropped.
func TestTheDefaultPolicyBlocksAndDropsNothing(t *testing.T) {
	got := runFlood(t, connector.WhenFullBlock, "")
	if got.dropped != 0 {
		t.Errorf("block dropped %d records; it is the one policy that never loses data", got.dropped)
	}
	if got.produced > 64 {
		t.Errorf("the source produced %d records against a budget of 8 without being stopped; "+
			"admission is not applying backpressure at all", got.produced)
	}
}

// REJECT SHEDS, and the shed is counted rather than silent. Design rule R6 asks for a rejection path
// that is expressible; before this, choosing it got you blocking with no indication that the choice
// had been ignored.
func TestRejectShedsAndCountsWhatItDropped(t *testing.T) {
	got := runFlood(t, connector.WhenFullReject, "")
	if got.dropped == 0 {
		t.Fatalf("reject dropped nothing over %d produced records; the policy is not being read",
			got.produced)
	}
	// The metric is the operator's only evidence that a configured loss is happening.
	if got.metric != float64(got.dropped) {
		t.Errorf("canal_records_abandoned_total{reason=buffer_full} is %v but %d records were dropped",
			got.metric, got.dropped)
	}
	// AND THE SOURCE KEEPS READING. A shed that stopped the pipeline would be a noisier way of
	// blocking, which is the opposite of what the operator asked for.
	if got.produced < 64 {
		t.Errorf("only %d records were produced; a shedding lane must not stall the reader", got.produced)
	}
}

// DROP_NEWEST AND REJECT COINCIDE AT THE SOURCE EDGE TODAY, and saying so is better than implying a
// distinction that does not exist. Both refuse the incoming batch, advance past it and count it; the
// two diverge once a buffer node can sit between them and decide WHICH end of the queue is dropped.
// The policy is still read separately, so the day that matters the wiring is already right.
func TestDropNewestShedsTheSameWayRejectDoes(t *testing.T) {
	got := runFlood(t, connector.WhenFullDropNewest, "")
	if got.dropped == 0 {
		t.Fatalf("drop_newest dropped nothing over %d produced records", got.produced)
	}
	if got.metric != float64(got.dropped) {
		t.Errorf("the metric is %v and %d records were dropped", got.metric, got.dropped)
	}
}

// SPECIFIC BEATS GENERAL, the precedence spec.StreamFor already uses. The registry offers when_full
// on every node's form, so a node that sets it must win over the pipeline-wide value — otherwise the
// field on the form is decoration.
func TestANodesOwnWhenFullBeatsThePipelineWideOne(t *testing.T) {
	got := runFlood(t, connector.WhenFullBlock, "reject")
	if got.dropped == 0 {
		t.Errorf("the pipeline says block and the source node says reject, and nothing was dropped: "+
			"the node's own stage-standard field is not being read (%d produced)", got.produced)
	}
}

// OVERFLOW IS REFUSED AT BUILD. It means "spill to the next buffer in the graph" and no buffer node
// type exists, so honouring it is impossible and treating it as a shed would drop data the operator
// asked to have moved somewhere else.
func TestOverflowIsRefusedBecauseThereIsNowhereToOverflowTo(t *testing.T) {
	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	src := &floodSource{perBatch: 4, release: make(chan struct{})}
	srcName := registerFloodSource(t, src)
	sinkName := registerProbe(t, "overflow_sink", &probeSink{})

	_, _, diags := engine.Build(context.Background(), registry.Default,
		floodSpec(srcName, sinkName, connector.WhenFullOverflow, ""),
		engine.Deps{State: st, Worker: "test", GracePeriod: time.Second})
	if !diags.HasErrors() {
		t.Fatal("a pipeline asking to overflow into a buffer that cannot exist was accepted")
	}
	var named bool
	for _, d := range diags {
		if strings.Contains(d.Message, "overflow") && strings.Contains(d.Message, "buffer") {
			named = true
		}
	}
	if !named {
		t.Errorf("the refusal does not explain itself: %v", diags)
	}

	// The same refusal for a NODE that asks for it, because a gate that only checks one of the two
	// places it can be set is a gate with a hole in it.
	_, _, nodeDiags := engine.Build(context.Background(), registry.Default,
		floodSpec(srcName, sinkName, connector.WhenFullBlock, "overflow"),
		engine.Deps{State: st, Worker: "test", GracePeriod: time.Second})
	if !nodeDiags.HasErrors() {
		t.Error("a NODE asking to overflow was accepted although the pipeline-wide value was refused")
	}
}

// connector.Runtime.Config returned (nil, nil) to every connector that ever asked, because Build
// validated each node's config, handed it to New and then dropped it.
func TestARuntimeHandsBackTheConfigItWasBuiltWith(t *testing.T) {
	var seen *config.Config
	var found bool
	sinkName := registerProbe(t, "config_sink", &probeSink{
		onOpen: func(rt connector.SinkRuntime) {
			seen, _ = rt.Config(context.Background())
			found = true
		},
	})

	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 3)
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(sinkName, path),
		engine.Deps{State: st, Worker: "test", FlushInterval: 5 * time.Millisecond,
			GracePeriod: 2 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !found {
		t.Fatal("the sink never opened, so this test proves nothing")
	}
	if seen == nil {
		t.Fatal("Runtime.Config returned nil to a connector whose config Build had just validated")
	}
	// The stage-standard fields are on it, which is what makes reading when_full from here possible.
	if !seen.Has("codec") {
		t.Error("the config handed back does not carry the codec this node was configured with")
	}
}

// --- harness ------------------------------------------------------------------------------------

type floodResult struct {
	produced int
	dropped  int
	metric   float64
}

// runFlood drives a flooding source into a wedged sink until the lane is well past its budget.
func runFlood(t *testing.T, w connector.WhenFull, nodeOverride string) floodResult {
	t.Helper()
	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	src := &floodSource{perBatch: 4, release: make(chan struct{})}
	srcName := registerFloodSource(t, src)

	// A DEFERRING FLUSHER, not a wedged sink, and the difference is the whole test.
	//
	// readLoop is sequential — read, admit, deliver — so a sink that never returns blocks the READER
	// rather than filling the lane, and admission is never reached at all. A Flusher accepts
	// immediately and makes nothing durable, which is exactly the shape durability.go describes:
	// with a Flusher every un-durable record counts against the lane budget. The budget fills, the
	// reader keeps arriving, and admission is the thing that has to decide.
	f := &flusher{deferAll: true}
	sinkName := registerFlusher(t, "flood_sink", f)

	reg := metrics.New()
	p, _, diags := engine.Build(context.Background(), registry.Default,
		floodSpec(srcName, sinkName, w, nodeOverride),
		engine.Deps{State: st, Worker: "test", Metrics: reg,
			FlushInterval: 5 * time.Millisecond, GracePeriod: time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Run until the lane has been full for a while. Gated on observed production rather than on the
	// clock alone, so a slow machine waits rather than measuring nothing.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		s := p.Status(telemetry.StatusQuery{})
		if len(s.Lanes) == 1 && s.Lanes[0].RecordsAbandoned > 0 {
			break
		}
		if src.count() >= 400 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)

	// BOTH COUNTERS ARE READ AFTER THE RUN ENDS, not during it. This pipeline sheds twenty thousand
	// records a second, so a status snapshot and a scrape taken a few microseconds apart disagree by
	// however many batches landed between them — which is a property of reading a moving counter
	// twice, not a defect, and asserting on it would be asserting on the scheduler.
	cancel()
	<-done
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("the run did not finish inside its timeout")
	}

	s := p.Status(telemetry.StatusQuery{})
	res := floodResult{produced: src.count()}
	if len(s.Lanes) == 1 {
		res.dropped = int(s.Lanes[0].RecordsAbandoned)
	}
	res.metric = sumAbandonedFor(t, scrape(t, reg), telemetry.ReasonBufferFull)
	return res
}

// sumAbandonedFor totals canal_records_abandoned_total across the series carrying one reason.
//
// The shared value() helper reads the FIRST matching series, and this counter has several: the
// shutdown leak report adds a zero-valued drain_timeout series whose empty lane label sorts it to
// the front, so the naive read returned 0 for a run that shed twenty thousand records. Selecting by
// reason is the whole point of the label existing.
func sumAbandonedFor(t *testing.T, body, reason string) float64 {
	t.Helper()
	var total float64
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, telemetry.MRecordsAbandoned+"{") {
			continue
		}
		if !strings.Contains(line, `reason="`+reason+`"`) {
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
