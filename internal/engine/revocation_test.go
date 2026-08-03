package engine_test

import (
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

// revocationFixture is a running two-lane pipeline behind a coordinator whose clock a test drives.
type revocationFixture struct {
	p     *engine.Pipeline
	coord *memstore.Coordinator
	src   *twoLaneSource
	sink  *flusher
	stop  func()

	// now is the coordinator's clock, and it is GUARDED because the test writes it while the
	// engine's lease goroutine reads it through coord.Now on every renew. The plain field this
	// replaced was a data race that only the skip was hiding — nothing ran this file under -race.
	mu  sync.Mutex
	now time.Time
}

// clock is what the coordinator asks the time of.
func (f *revocationFixture) clock() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *revocationFixture) setClock(t time.Time) {
	f.mu.Lock()
	f.now = t
	f.mu.Unlock()
}

func startRevocation(t *testing.T) *revocationFixture {
	t.Helper()
	dir := t.TempDir()

	// THE SINK HOLDS BY DEFERRING DURABILITY, NOT BY BLOCKING, and that is the whole difference
	// between this fixture and the version that could not work.
	//
	// The first one held records by parking inside Write. The engine writes serially per sink node,
	// so a blocked write for lane a stalled lane b behind it: the fixture reached one record in
	// flight on a and none on b, which is the exact state the assertion needs NOT to be in. There
	// was nothing to fence, because nothing was ever going to be acknowledged either way.
	//
	// A Flusher returns from Write immediately — accepted, not durable — so neither lane stalls and
	// both accumulate in flight. Nothing settles until Flush is allowed to cover them, and letting
	// it cover everything at once is what puts both lanes' records on the commit pump in the same
	// moment, with one lane lost and the other held. That moment is the only one in which these
	// three rules are distinguishable from their absence.
	f := &revocationFixture{
		coord: memstore.NewCoordinator(),
		src:   newTwoLaneSource(),
		sink:  &flusher{deferAll: true},
		now:   time.Now(),
	}
	f.coord.Now = f.clock

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the state store: %v", err)
	}

	spec := pipelineSpec(registerFlusher(t, "revocation_sink", f.sink), filepath.Join(dir, "unused.txt"))
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
			// Let everything become durable before shutdown, so a failed test tears down through the
			// ordinary drain rather than through the grace period expiring on held records.
			f.release()
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

// release lets the next flush make every accepted record durable, settling both lanes at once.
func (f *revocationFixture) release() {
	f.sink.set(func(s *flusher) { s.deferAll = false })
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
// See startRevocation for how the sink holds instead.
func TestRecordsSettlingAfterARevocationNeverReachTheUpstream(t *testing.T) {
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

	// LANE A IS TAKEN AWAY AND LANE B IS NOT, which needs one move worth spelling out.
	//
	// Whether a lease has lapsed is not stored state — it is Now() compared against the row's
	// LeaseExpires — and both lanes were claimed together, so any clock far enough forward to make
	// lane a claimable makes lane b lapse in the same instant. The first version did exactly that
	// and destroyed its own positive control: both leases went, nothing was acknowledged at all,
	// and the fence had nothing left to prove.
	//
	// So the clock goes forward only long enough for the other worker to take lane a — a Claim
	// MUTATES that row, giving it a new holder, a new epoch and a fresh expiry — and then goes
	// straight back. Lane b's row is untouched by the round trip, so its stored expiry is in the
	// future again and this worker keeps renewing it.
	//
	// The state left behind is the ordinary one a planner produces when it moves a single lane
	// between workers: one row reassigned, the rest held. The clock is only how a deterministic
	// test reaches it without waiting out a two-and-a-half-minute reassignment delay.
	held := f.clock()
	a := f.assignmentOf(t, "a")
	f.setClock(held.Add(store.DefaultLeaseTTL + store.DefaultReassignmentDelay + time.Second))
	_, claimErr := f.coord.Claim(ctx, a.ID, "w2", store.DefaultLeaseTTL)
	f.setClock(held)
	if claimErr != nil {
		t.Fatalf("the other worker could not take lane a: %v", claimErr)
	}

	// THE POSITIVE CONTROL IS CHECKED HERE, WHILE IT IS STILL CHEAP TO EXPLAIN.
	//
	// The clock is forward for the duration of one Claim, and the engine's lease goroutine reads it
	// on every renew — so a renew landing inside that window sees both leases lapsed and this worker
	// loses lane b as well. It is a narrow window against a ten-second renew interval, but the whole
	// test rests on lane b surviving, and without this the symptom is "lane b was acknowledged 0
	// times" thirty seconds later, which reads as a broken fence rather than a lost control.
	//
	// EXPIRY IS THE DISCRIMINATOR, NOT Worker, which store.Assignment says in its own doc and which
	// the first version of this check got wrong: a lapsed row goes on naming whoever held it last,
	// because that identity is what the reassignment delay reserves it for. Comparing Worker alone
	// reports "still ours" for precisely the lane that was just lost.
	b := f.assignmentOf(t, "b")
	if lease := (store.Lease{Expires: b.LeaseExpires}); b.Worker != "w1" || !lease.Valid(f.clock()) {
		t.Fatalf("lane b is not held by this worker any more (worker=%q expires=%v now=%v): the "+
			"clock window was hit and the positive control is gone, so a green run proves nothing",
			b.Worker, b.LeaseExpires, f.clock())
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
	f.release()
	f.awaitAcks(t, "b", 1)

	if got := f.src.acksFor("a"); got != 0 {
		t.Errorf("lane a was acknowledged %d times after this worker lost it, in the same moment "+
			"lane b was acknowledged %d times.\n"+
			"  Every one of those lets the upstream discard records the new holder has not "+
			"delivered, and no epoch undoes it because by then the data is gone from the source",
			got, f.src.acksFor("b"))
	}
}
