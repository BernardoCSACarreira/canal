package engine_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/internal/example/memstore"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// THE FIXTURE THE REVOCATION FENCES NEEDED, AND DID NOT HAVE.
//
// Three rules protect a fenced worker from advancing an upstream it has lost — the ledger's own
// fence, the commit pump's check, and the refusal to re-claim a lane this run was fenced off — and
// not one of them could be demonstrated. Deleting all three left every test passing.
//
// The reason was always the same, and it was the FIXTURE. Every earlier attempt revoked a lane by
// taking it away, and taking the lane away is also what stops the pipeline: Assigned stops offering
// it, the source stops producing, the run winds down, and the acknowledgement that should have been
// refused is never generated. There is nothing for a fence to stop.
//
// This one holds TWO LANES on one source and revokes only the first, so the run stays alive instead
// of winding down — and both lanes' records are held in the sink across the revocation and settle
// after it. That is the state every one of those rules exists for and none of them had ever been in.
//
// It does not get there yet. See the skip on the test below for exactly where it stops and why.

// twoLaneSource holds two lanes and produces on both, so revoking one leaves the pipeline running.
type twoLaneSource struct {
	mu      sync.Mutex
	acks    []connector.Ack
	seq     map[record.LaneID]uint64
	started chan struct{}
	once    sync.Once
}

func newTwoLaneSource() *twoLaneSource {
	return &twoLaneSource{seq: map[record.LaneID]uint64{}, started: make(chan struct{})}
}

func (s *twoLaneSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil {
		return err
	}
	if len(as) == 0 {
		if _, err := rt.Lanes().AnnounceMany(ctx, []connector.LaneSpec{
			{Name: "a", Stream: "lines", Kind: connector.LaneKindStream,
				Ordering: connector.OrderingPrefix, Boundedness: connector.Unbounded},
			{Name: "b", Stream: "lines", Kind: connector.LaneKindStream,
				Ordering: connector.OrderingPrefix, Boundedness: connector.Unbounded},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *twoLaneSource) Close(context.Context) error { return nil }

// Read and ReadLanes are the SAME BODY over one batch or several.
//
// Both are implemented because which one the engine calls is its choice — LaneReader is an optional
// interface resolved from declared caps — and a fixture that only filled one of them looked exactly
// like a pipeline that had claimed its lanes and then produced nothing. That was twenty minutes of
// this fixture's life.
func (s *twoLaneSource) Read(ctx context.Context, dst *record.Batch) error {
	return s.ReadLanes(ctx, []*record.Batch{dst})
}

func (s *twoLaneSource) ReadLanes(ctx context.Context, dst []*record.Batch) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Millisecond):
	}
	s.once.Do(func() { close(s.started) })

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range dst {
		s.seq[b.Lane]++
		n := s.seq[b.Lane]
		if r := b.Add(); r != nil {
			// The payload names its lane, which was how the sink was meant to hold one lane and pass
			// the other. That does not work — see holdingSink — but the naming stays: it is what
			// makes a held batch identifiable when this fixture is finished, and a Request carries
			// bytes rather than a lane.
			r.Payload = record.BytesPayload([]byte(fmt.Sprintf("%s-%d", laneTag(b.Lane), n)))
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], n)
		b.Position = record.Position{
			Token: record.Blob{Version: 1, Bytes: buf[:]}, Order: buf[:],
			Safe: true, At: time.Now(), Label: fmt.Sprintf("row %d", n),
		}
	}
	return nil
}

func (s *twoLaneSource) Commit(_ context.Context, a connector.Ack) error {
	s.mu.Lock()
	s.acks = append(s.acks, a)
	s.mu.Unlock()
	return nil
}

// acksFor counts the acknowledgements delivered for one lane.
func (s *twoLaneSource) acksFor(tag string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for _, a := range s.acks {
		if laneTag(a.Lane) == tag {
			n++
		}
	}
	return n
}

// inFlightByLane renders the per-lane in-flight counts, for a diagnostic.
func inFlightByLane(d telemetry.PipelineStatus) map[string]uint64 {
	out := map[string]uint64{}
	for _, l := range d.Lanes {
		out[laneTag(l.ID)] = l.InFlight
	}
	return out
}

// laneTag is the trailing segment of a lane id, which is the name the source announced.
func laneTag(id record.LaneID) string {
	s := string(id)
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return s[i+1:]
		}
	}
	return s
}

// holdingSink parks in Write until it is released.
//
// It HOLDS EVERYTHING, and the matcher is a formality kept only because the next version of this
// fixture will not need it either. Holding one lane selectively was the first design and cannot
// work: the engine writes serially per sink node, so a blocked write for one lane stalls every other
// lane behind it — which is the whole reason this test is skipped.
type holdingSink struct {
	hold    []byte
	release chan struct{}
}

func (h *holdingSink) Open(context.Context, connector.SinkRuntime, connector.Opening) error {
	return nil
}
func (h *holdingSink) Close(context.Context) error { return nil }

func (h *holdingSink) Write(ctx context.Context, req *connector.Request) (connector.WriteResult, error) {
	if bytes.Contains(req.Body, h.hold) {
		select {
		case <-h.release:
		case <-ctx.Done():
			return connector.WriteResult{}, ctx.Err()
		}
	}
	return connector.AllWritten(req.Count), nil
}

func registerTwoLaneSource(t *testing.T, s *twoLaneSource) string {
	t.Helper()
	name := fmt.Sprintf("two_lane_source_%d", sinkSeq.Add(1))
	registry.AddSource(registry.Default, registry.SourceDef[*twoLaneSource]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Two-lane source",
			Summary: "Holds two lanes and produces on both, so revoking one leaves the run alive.",
			Notes:   "Origin.Key is the per-lane sequence, stable across re-reads.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SourceCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering:   connector.OrderingPrefix,
			Boundedness:       []connector.Boundedness{connector.Unbounded},
			LaneKinds:         []connector.LaneKind{connector.LaneKindStream},
			MaxLanes:          2,
			ReadsLanes:        true,
			ReadConcurrency:   1,
			StableKeys:        true,
			Replayable:        true,
			UpstreamRetention: connector.RetentionUnbounded,
		},
		New: func(context.Context, *config.Config) (*twoLaneSource, error) { return s, nil },
	})
	return name
}

func registerHoldingSink(t *testing.T, h *holdingSink) string {
	t.Helper()
	name := fmt.Sprintf("holding_sink_%d", sinkSeq.Add(1))
	registry.AddSink(registry.Default, registry.SinkDef[*holdingSink]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Holding sink",
			Summary: "Parks in Write for one lane's records and accepts the rest.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			Modes:          []connector.DestMode{connector.DestAppend},
			MaxConcurrency: 1,
		},
		New: func(context.Context, *config.Config) (*holdingSink, error) { return h, nil },
	})
	return name
}

// revocationFixture is a running two-lane pipeline behind a coordinator whose clock a test drives.
type revocationFixture struct {
	p     *engine.Pipeline
	coord *memstore.Coordinator
	src   *twoLaneSource
	sink  *holdingSink
	now   time.Time
	stop  func()
}

func startRevocation(t *testing.T) *revocationFixture {
	t.Helper()
	dir := t.TempDir()

	f := &revocationFixture{
		coord: memstore.NewCoordinator(),
		src:   newTwoLaneSource(),
		sink:  &holdingSink{hold: []byte("-"), release: make(chan struct{})},
		now:   time.Now(),
	}
	f.coord.Now = func() time.Time { return f.now }

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the state store: %v", err)
	}

	spec := pipelineSpec(registerHoldingSink(t, f.sink), filepath.Join(dir, "unused.txt"))
	spec.Graph[0].Name, spec.Graph[0].Config = registerTwoLaneSource(t, f.src), map[string]any{}
	spec.Streams[0].Read = []connector.LaneKind{connector.LaneKindStream}
	spec.Parallelism = 2

	p, _, diags := engine.Build(context.Background(), registry.Default, spec, engine.Deps{
		State: st, Coordinator: f.coord, Worker: "w1",
		FlushInterval: 5 * time.Millisecond, GracePeriod: time.Second,
	})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	f.p = p

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	var once sync.Once
	f.stop = func() {
		once.Do(func() {
			close(f.sink.release)
			cancel()
			<-done
			p.Close(context.Background())
			st.Close()
		})
	}
	t.Cleanup(f.stop)

	select {
	case <-f.src.started:
	case err := <-done:
		t.Fatalf("the run ended before the source read anything: %v", err)
	case <-time.After(20 * time.Second):
		d := p.Status(telemetry.StatusQuery{})
		as, _ := f.coord.Assignments(context.Background(), d.Tenant, d.Pipeline)
		held := 0
		for _, a := range as {
			if a.Worker == "w1" {
				held++
			}
		}
		t.Fatalf("the source never started reading; phase=%s lanes=%d assignments=%d heldByW1=%d",
			d.Phase, d.LaneCount, len(as), held)
	}
	return f
}

// assignmentOf returns the coordinator's row for the lane whose announced name is tag.
func (f *revocationFixture) assignmentOf(t *testing.T, tag string) store.Assignment {
	t.Helper()
	s := f.p.Status(telemetry.StatusQuery{})
	as, err := f.coord.Assignments(context.Background(), s.Tenant, s.Pipeline)
	if err != nil {
		t.Fatalf("Assignments: %v", err)
	}
	for _, a := range as {
		if laneTag(a.Lane.ID) == tag {
			return a
		}
	}
	t.Fatalf("no assignment for lane %q among %d rows", tag, len(as))
	return store.Assignment{}
}

// awaitAcks blocks until a lane has been acknowledged at least n times.
func (f *revocationFixture) awaitAcks(t *testing.T, tag string, n int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if f.src.acksFor(tag) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("lane %s was acknowledged %d times, want at least %d", tag, f.src.acksFor(tag), n)
}

// THE ASSERTION EVERY REVOCATION RULE EXISTS FOR, and the first fixture that can make it.
//
// ONE RELEASE, TWO OUTCOMES. Both lanes' records are held in the sink across the revocation; when the
// hold is released they all settle at once and reach the commit pump together, with lane a already
// lost and lane b still held. Lane b being acknowledged is the positive control — it is what proves
// the ack path ran at all — and lane a not being acknowledged in the same moment is the fence.
//
// Holding only one lane was the first attempt and does not work: the engine writes serially per sink
// node, so a blocked write for lane a stalls lane b behind it and nothing is acknowledged either way.
func TestRecordsSettlingAfterARevocationNeverReachTheUpstream(t *testing.T) {
	t.Skip(`unfinished: the sink holds by BLOCKING Write, and the engine reads and writes serially
per sink node — so the first held record starves the other lane and the fixture reaches
inflight=map[a:1 b:0] instead of records in flight on both. Everything up to that point works: two
lanes are announced, both are claimed, the run stays alive, and the revocation is noticed.

The fix is to hold by DEFERRING DURABILITY rather than by blocking: a sink that declares Flusher
returns from Write immediately, so nothing on the read path stalls, and its records settle only when
Flush is allowed to return. Releasing the flush then settles both lanes at once, which is the shape
this test's assertion already expects.

Checked in skipped rather than dropped, because the fixture is most of the way there and the
diagnosis is the expensive part.`)

	f := startRevocation(t)
	ctx := context.Background()

	// Gated on records genuinely being in flight for both lanes, so the release below has something
	// to settle on each.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if d := f.p.Status(telemetry.StatusQuery{}); len(d.Lanes) == 2 &&
			d.Lanes[0].InFlight > 0 && d.Lanes[1].InFlight > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	d := f.p.Status(telemetry.StatusQuery{})
	if len(d.Lanes) != 2 || d.Lanes[0].InFlight == 0 || d.Lanes[1].InFlight == 0 {
		t.Fatalf("both lanes need records in flight before the revocation; lanes=%d inflight=%v",
			len(d.Lanes), inFlightByLane(d))
	}
	if got := f.src.acksFor("a") + f.src.acksFor("b"); got != 0 {
		t.Fatalf("%d acknowledgements arrived while every record is held in the sink; the hold is "+
			"not working and this test isolates nothing", got)
	}

	// Lane a is taken away. Lane b is untouched.
	a := f.assignmentOf(t, "a")
	f.now = f.now.Add(store.DefaultLeaseTTL + store.DefaultReassignmentDelay + time.Second)
	if _, err := f.coord.Claim(ctx, a.ID, "w2", store.DefaultLeaseTTL); err != nil {
		t.Fatalf("the other worker could not take lane a: %v", err)
	}

	revoked := time.Now().Add(2*store.DefaultRenewInterval + 10*time.Second)
	var seen bool
	for time.Now().Before(revoked) && !seen {
		for _, e := range f.p.Status(telemetry.StatusQuery{}).RecentEvents {
			if e.Kind == "lane_revoked" {
				seen = true
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !seen {
		t.Fatal("this worker never noticed it lost lane a, so nothing is being fenced")
	}

	// NOW every held record settles, with lane a already lost and lane b still held.
	close(f.sink.release)
	f.awaitAcks(t, "b", 1)

	if got := f.src.acksFor("a"); got != 0 {
		t.Errorf("lane a was acknowledged %d times after this worker lost it, in the same moment "+
			"lane b was acknowledged %d times.\n"+
			"  Every one of those lets the upstream discard records the new holder has not "+
			"delivered, and no epoch undoes it because by then the data is gone from the source",
			got, f.src.acksFor("b"))
	}
}
