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

// THE WORKER HOLDS LEASES NOW, and the one rule that matters is that it stops when it loses one.
//
// Deps.Coordinator was declared and unread: laneCtl.Revoked returned false truthfully, because one
// process could not lose a lane to anybody. These drive a real pipeline against a real coordinator
// and take its lane away underneath it.

// leaseFixture is a running pipeline with a coordinator behind it.
type leaseFixture struct {
	p     *engine.Pipeline
	coord *memstore.Coordinator
	src   *leaseSource
	now   time.Time
	stop  func()
}

func startLeased(t *testing.T, hold ...chan struct{}) *leaseFixture {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 3)

	// The sink accepts everything, so records SETTLE and phase three actually runs — which is the
	// only way a test can see whether an upstream was told anything. A caller that passes a hold
	// channel gets a sink that parks in Write until it closes, which is how records are kept in
	// flight ACROSS a revocation.
	sink := &collector{}
	if len(hold) > 0 {
		sink.after = func(int) { <-hold[0] }
	}
	name := registerCollector(t, "lease_sink", sink)

	f := &leaseFixture{coord: memstore.NewCoordinator(), src: &leaseSource{}, now: time.Now()}
	f.coord.Now = func() time.Time { return f.now }

	// THE EPOCH COUNTER IS ADVANCED PAST 1 BEFORE THE PIPELINE STARTS. A fresh coordinator grants
	// epoch 1 to its first claim, and singleWorkerEpoch is also 1 — so a test whose pipeline takes
	// the very first lease cannot tell the lease's epoch from the constant, and one that asserted it
	// passed with epochFor hard-coded to the constant. Burning two epochs on a throwaway assignment
	// costs nothing and makes the numbers distinguishable.
	burn := []store.LaneRow{{ID: "burn", Name: "burn"}}
	if err := f.coord.Plan(context.Background(), "burn", "burn", 0, burn); err != nil {
		t.Fatalf("seeding the coordinator: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := f.coord.Claim(context.Background(),
			store.AssignmentIDFor("burn", "burn", 0, "burn"), "burner", store.DefaultLeaseTTL); err != nil {
			t.Fatalf("seeding the coordinator: %v", err)
		}
	}

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the state store: %v", err)
	}

	spec := pipelineSpec(name, path)
	spec.Graph[0].Name, spec.Graph[0].Config = registerLeaseSource(t, f.src), map[string]any{}
	spec.Streams[0].Read = []connector.LaneKind{connector.LaneKindStream}
	p, _, diags := engine.Build(context.Background(), registry.Default, spec,
		engine.Deps{
			State: st, Coordinator: f.coord, Worker: "w1",
			FlushInterval: 5 * time.Millisecond, GracePeriod: time.Second,
		})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	f.p = p

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	f.stop = func() {
		cancel()
		<-done
		p.Close(context.Background())
		st.Close()
	}
	t.Cleanup(f.stop)

	f.awaitPhase(t, telemetry.PhaseRunning)
	return f
}

func (f *leaseFixture) awaitPhase(t *testing.T, want telemetry.Phase) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last telemetry.Phase
	for time.Now().Before(deadline) {
		if last = f.p.Status(telemetry.StatusQuery{}).Phase; last == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("the pipeline is %s and never reached %s", last, want)
}

// A WORKER THAT CLAIMS ITS LANES REPORTS THE LEASE IT HOLDS. WorkerStatus.LeaseExpires has been a
// declared field with no producer since the read model was written, for the reason status.go named:
// there were no leases.
func TestAWorkerWithACoordinatorHoldsAndReportsItsLeases(t *testing.T) {
	f := startLeased(t)

	s := f.p.Status(telemetry.StatusQuery{})
	if len(s.Workers) != 1 {
		t.Fatalf("the document reports %d workers, want 1", len(s.Workers))
	}
	w := s.Workers[0]
	if w.LeaseExpires == nil {
		t.Fatal("the worker reports no lease expiry although it claimed its lanes through a " +
			"coordinator; the field is nil only when nothing granted a lease")
	}
	if !w.LeaseExpires.After(time.Now()) {
		t.Errorf("the reported lease expired at %s, which is in the past", w.LeaseExpires)
	}

	// And the assignment really is in the coordinator, held by this worker.
	as, err := f.coord.Assignments(context.Background(), s.Tenant, s.Pipeline)
	if err != nil {
		t.Fatalf("Assignments: %v", err)
	}
	if len(as) == 0 {
		t.Fatal("the coordinator holds no assignments, so nothing was planned")
	}
	for _, a := range as {
		if a.Worker != "w1" {
			t.Errorf("assignment %s is held by %q, want w1", a.ID, a.Worker)
		}
		if a.Epoch == 0 {
			t.Errorf("assignment %s was granted epoch 0, which is the value that means unclaimed", a.ID)
		}
	}

	// AND THE EPOCH THE ENGINE HANDED THE SOURCE IS THE LEASE'S, not the single-worker constant.
	//
	// The lane is re-claimed first so the granted epoch is past 1. With one lane in a fresh
	// coordinator the first claim gets epoch 1, which is singleWorkerEpoch — so a version of this
	// that asserted against the first claim passed with the lease epoch hard-coded to the constant.
	// Asserting it on the coordinator's row only proves the coordinator works; this is the number the
	// connector sees and the number a durable write would be fenced with.
	epochs := f.src.seenEpochs()
	if as[0].Epoch == 1 {
		t.Fatalf("the lease epoch is %d, which is singleWorkerEpoch; this assertion cannot tell the "+
			"lease's epoch from the constant", as[0].Epoch)
	}
	if len(epochs) == 0 {
		t.Fatal("the source was never assigned a lane")
	}
	for _, e := range epochs {
		if e != as[0].Epoch {
			t.Errorf("the source was handed epoch %d for its lane; the coordinator granted %d. "+
				"A connector fencing its own writes with this would be fencing with a constant",
				e, as[0].Epoch)
		}
	}
}

// A WORKER WITH NO COORDINATOR REPORTS NO LEASE, and nil says so rather than a zero time. This is the
// standalone deployment, and it must be untouched by any of the above.
func TestAWorkerWithNoCoordinatorReportsNoLease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 2)
	c := &collector{}
	name := registerCollector(t, "lease_none", c)

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the state store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name, path),
		engine.Deps{State: st, Worker: "single", FlushInterval: 5 * time.Millisecond,
			GracePeriod: time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := len(c.got()); got != 2 {
		t.Errorf("the standalone pipeline delivered %d of 2 records", got)
	}
	if w := p.Status(telemetry.StatusQuery{}).Workers[0]; w.LeaseExpires != nil {
		t.Errorf("a worker with no coordinator reports a lease expiry of %s; nil is what says there "+
			"is no lease, and a time here would read as one that is about to lapse", w.LeaseExpires)
	}
}

// THE FENCE WITH TEETH. A worker that has lost a lane must not tell that lane's upstream it may
// forget anything: the new holder re-reads from the last DURABLE cursor, and an acknowledgement from
// the old holder discards records nobody has delivered.
func TestALaneTakenAwayIsNotAcknowledgedAndIsReportedRevoked(t *testing.T) {
	f := startLeased(t)
	ctx := context.Background()

	s := f.p.Status(telemetry.StatusQuery{})
	as, err := f.coord.Assignments(ctx, s.Tenant, s.Pipeline)
	if err != nil || len(as) == 0 {
		t.Fatalf("no assignments to take away (%v)", err)
	}
	lane := as[0].Lane.ID

	// Gated on phase three having actually run, so "no commits after revocation" is a statement
	// about the fence and not about a pipeline that never committed anything.
	deadline := time.Now().Add(10 * time.Second)
	for f.src.committed() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if f.src.committed() == 0 {
		t.Fatal("the source was never told to advance its upstream, so this test cannot tell a fence " +
			"from a pipeline that commits nothing")
	}

	// SOMEBODY ELSE TAKES THE LANE. Past the TTL and the reassignment delay, which is exactly the
	// state a worker that stopped renewing ends up in.
	f.now = f.now.Add(store.DefaultLeaseTTL + store.DefaultReassignmentDelay + time.Second)
	if _, err := f.coord.Claim(ctx, as[0].ID, "w2", store.DefaultLeaseTTL); err != nil {
		t.Fatalf("the other worker could not take the lane: %v", err)
	}

	// The renew loop notices on its next tick, and the renew interval is what bounds that.
	deadline = time.Now().Add(2*store.DefaultRenewInterval + 5*time.Second)
	var revoked bool
	for time.Now().Before(deadline) && !revoked {
		for _, e := range f.p.Status(telemetry.StatusQuery{}).RecentEvents {
			if e.Kind == "lane_revoked" {
				revoked = true
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !revoked {
		t.Fatalf("the pipeline never reported lane %s revoked after another worker took it; a "+
			"worker that does not notice keeps reading and keeps acknowledging", lane)
	}

	// THE ASSERTION THIS WHOLE CHANGE EXISTS FOR. The upstream must not be told it may forget
	// anything for a lane this worker no longer holds: the new holder re-reads from the last DURABLE
	// cursor, so an acknowledgement from the old holder discards records nobody has delivered.
	settled := f.src.committed()
	time.Sleep(200 * time.Millisecond)
	if got := f.src.committed(); got != settled {
		t.Errorf("the source was told to advance %d more times after its lane was taken away.\n"+
			"  Every one of those acknowledgements lets an upstream discard records the new holder "+
			"has not delivered, and no epoch undoes it", got-settled)
	}

	// AND IT REPORTS NO LEASE AT ALL, rather than a stale one. nil is what says this worker holds
	// nothing; a time in the past would read as a lease that is merely about to lapse.
	if got := f.p.Status(telemetry.StatusQuery{}).Workers[0].LeaseExpires; got != nil {
		t.Errorf("the worker still reports a lease expiring at %s after losing every lane", got)
	}
}

// leaseSource records what the engine tells it: the epoch on every lane it is assigned, and every
// lane whose upstream it is told to advance.
//
// It exists because the first version of these tests asserted the SIDE EFFECT — that a lane-revoked
// event appeared — and not the PROPERTY the whole change is for. Deleting the commit fence in
// commitPump left them passing, which is the same defect this repo hunts in production code: a thing
// that is declared and observed by nothing.
type leaseSource struct {
	mu      sync.Mutex
	commits []record.LaneID
	epochs  []uint64
	seq     uint64
}

func (s *leaseSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil {
		return err
	}
	if len(as) == 0 {
		if _, err := rt.Lanes().Announce(ctx, connector.LaneSpec{
			Name: "tail", Stream: "lines", Kind: connector.LaneKindStream,
			Ordering: connector.OrderingPrefix, Boundedness: connector.Unbounded,
		}); err != nil {
			return err
		}
		if as, err = rt.Lanes().Assigned(ctx); err != nil {
			return err
		}
	}
	s.mu.Lock()
	for _, a := range as {
		s.epochs = append(s.epochs, a.Epoch)
	}
	s.mu.Unlock()
	return nil
}

func (s *leaseSource) Close(context.Context) error { return nil }

func (s *leaseSource) Read(ctx context.Context, dst *record.Batch) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
	}
	s.mu.Lock()
	s.seq++
	seq := s.seq
	s.mu.Unlock()

	dst.Reset()
	if r := dst.Add(); r != nil {
		r.Payload = record.BytesPayload([]byte(fmt.Sprintf("row-%d", seq)))
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seq)
	dst.Position = record.Position{
		Token: record.Blob{Version: 1, Bytes: b[:]}, Order: b[:],
		Safe: true, At: time.Now(), Label: fmt.Sprintf("row %d", seq),
	}
	return nil
}

func (s *leaseSource) Commit(_ context.Context, a connector.Ack) error {
	s.mu.Lock()
	s.commits = append(s.commits, a.Lane)
	s.mu.Unlock()
	return nil
}

func (s *leaseSource) committed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.commits)
}

func (s *leaseSource) seenEpochs() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.epochs...)
}

func registerLeaseSource(t *testing.T, s *leaseSource) string {
	t.Helper()
	name := fmt.Sprintf("lease_source_%d", sinkSeq.Add(1))
	registry.AddSource(registry.Default, registry.SourceDef[*leaseSource]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Lease source",
			Summary: "Records the epochs it is assigned and the lanes it is told to advance.",
			Notes:   "Origin.Key is the row sequence, stable across re-reads.",
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
		New: func(context.Context, *config.Config) (*leaseSource, error) { return s, nil },
	})
	return name
}

// THE COMMIT FENCE, ISOLATED. The test above cannot see it: by the time a lane is revoked there the
// read fence has already stopped the source, so nothing new settles and no acknowledgement was going
// to be attempted anyway. Deleting the fence in commitPump left it passing.
//
// Here the sink parks in Write, so records are IN FLIGHT across the revocation and settle only after
// it. Those settlements reach the commit pump with the lane already lost, which is the one moment
// the fence is the only thing standing between a fenced worker and an upstream it must not advance.
func TestRecordsSettlingAfterARevocationDoNotAdvanceTheUpstream(t *testing.T) {
	hold := make(chan struct{})
	f := startLeased(t, hold)
	ctx := context.Background()

	s := f.p.Status(telemetry.StatusQuery{})
	as, err := f.coord.Assignments(ctx, s.Tenant, s.Pipeline)
	if err != nil || len(as) == 0 {
		t.Fatalf("no assignments to take away (%v)", err)
	}

	// Gated on records genuinely being in flight, so releasing the sink below has something to settle.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if d := f.p.Status(telemetry.StatusQuery{}); len(d.Lanes) > 0 && d.Lanes[0].InFlight > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if d := f.p.Status(telemetry.StatusQuery{}); len(d.Lanes) == 0 || d.Lanes[0].InFlight == 0 {
		t.Fatal("no records are in flight, so releasing the sink settles nothing and this test " +
			"cannot reach the commit pump at all")
	}

	f.now = f.now.Add(store.DefaultLeaseTTL + store.DefaultReassignmentDelay + time.Second)
	if _, err := f.coord.Claim(ctx, as[0].ID, "w2", store.DefaultLeaseTTL); err != nil {
		t.Fatalf("the other worker could not take the lane: %v", err)
	}

	revoked := time.Now().Add(2*store.DefaultRenewInterval + 5*time.Second)
	for time.Now().Before(revoked) {
		if f.p.Status(telemetry.StatusQuery{}).Workers[0].LeaseExpires == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if f.p.Status(telemetry.StatusQuery{}).Workers[0].LeaseExpires != nil {
		t.Fatal("the worker never noticed it lost the lane, so nothing is being fenced")
	}

	// NOW the held records settle, and every one of them reaches the commit pump for a lane this
	// worker no longer holds.
	close(hold)
	time.Sleep(500 * time.Millisecond)

	if got := f.src.committed(); got != 0 {
		t.Errorf("the source was told to advance its upstream %d times for a lane this worker had "+
			"already lost.\n  Each one lets the upstream discard records the new holder has not "+
			"delivered, and no epoch undoes it because by then the data is gone from the source", got)
	}
}
