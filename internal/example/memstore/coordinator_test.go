package memstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/example/memstore"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// THE PLACEMENT PROTOCOL, PINNED BEFORE ANYTHING READS IT.
//
// store.Coordinator has been a declared interface with no implementation since it was written, and
// the engine is about to be written against it. These are the rules the engine will assume — the
// epoch fences, the lease expires, the delay reserves, the gate holds — asserted here where they can
// be driven deliberately rather than through a pipeline where a lease expiring is a coincidence.
//
// The clock is a field for that reason. A test that waited out a real thirty-second TTL would be a
// thirty-second test, and one that shortened the TTL to a millisecond would be testing a lease
// nobody deploys.

// fixture is a coordinator with a controllable clock and a planned pipeline.
type fixture struct {
	c   *memstore.Coordinator
	now time.Time
}

const (
	tenant = record.TenantID("acme")
	pipe   = record.PipelineID("p1")
	gen    = uint64(7)
)

func newFixture(t *testing.T, rows ...store.LaneRow) *fixture {
	t.Helper()
	f := &fixture{c: memstore.NewCoordinator(), now: time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)}
	f.c.Now = func() time.Time { return f.now }
	if len(rows) == 0 {
		rows = []store.LaneRow{{ID: "lane-a", Name: "a"}, {ID: "lane-b", Name: "b"}}
	}
	if err := f.c.Plan(context.Background(), tenant, pipe, gen, rows); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	return f
}

func (f *fixture) advance(d time.Duration) { f.now = f.now.Add(d) }

func (f *fixture) id(lane record.LaneID) store.AssignmentID {
	return memstore.AssignmentIDFor(tenant, pipe, gen, lane)
}

func (f *fixture) claim(t *testing.T, lane record.LaneID, w store.WorkerID) store.Lease {
	t.Helper()
	l, err := f.c.Claim(context.Background(), f.id(lane), w, store.DefaultLeaseTTL)
	if err != nil {
		t.Fatalf("claiming %s for %s: %v", lane, w, err)
	}
	return l
}

// THE EPOCH IS THE FENCE, and every claim takes a new one. That is what makes the loser of a race
// hold a number the store refuses, rather than two workers both believing they are current.
func TestEveryClaimTakesANewEpochAndFencesThePreviousHolder(t *testing.T) {
	f := newFixture(t)

	first := f.claim(t, "lane-a", "w1")
	if first.Epoch == 0 {
		t.Fatal("a claim was granted epoch 0; zero is the value that means unclaimed")
	}

	// A FAST RESTART IS THE SAME WORKER ID AND A DIFFERENT PROCESS. It must be able to take its own
	// lane back while the old lease is still live, and the epoch it gets must fence whatever the old
	// process still has in flight.
	again, err := f.c.Claim(context.Background(), f.id("lane-a"), "w1", store.DefaultLeaseTTL)
	if err != nil {
		t.Fatalf("a worker could not reclaim its own live lease, so a restart cannot recover: %v", err)
	}
	if again.Epoch <= first.Epoch {
		t.Errorf("the reclaim got epoch %d, not past the previous %d; the old process is not fenced",
			again.Epoch, first.Epoch)
	}

	// And the fenced lease is now good for nothing.
	if _, err := f.c.Renew(context.Background(), first); !errors.Is(err, fault.ErrFenced) {
		t.Errorf("renewing the fenced lease gave %v, want fault.ErrFenced", err)
	}
	if err := f.c.Release(context.Background(), first); !errors.Is(err, fault.ErrFenced) {
		t.Errorf("releasing the fenced lease gave %v, want fault.ErrFenced", err)
	}

	// Epochs are store-wide, so a claim on another lane does not repeat one.
	other := f.claim(t, "lane-b", "w2")
	if other.Epoch == again.Epoch {
		t.Errorf("two lanes were granted the same epoch %d; the counter is per row, not store-wide",
			other.Epoch)
	}
}

// A LIVE LEASE IS NOT TAKEABLE. This is the whole point of the lease and the one thing that stops
// two workers advancing one upstream.
func TestALiveLeaseCannotBeTakenByAnotherWorker(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "lane-a", "w1")

	_, err := f.c.Claim(context.Background(), f.id("lane-a"), "w2", store.DefaultLeaseTTL)
	if !errors.Is(err, fault.ErrFenced) {
		t.Fatalf("a second worker claimed a live lease (%v); two workers now read one lane", err)
	}
	if err != nil && !containsAll(err.Error(), "w1") {
		t.Errorf("the refusal %q does not name who holds it; an operator debugging a stuck lane "+
			"needs to know where it went", err)
	}
}

// THE REASSIGNMENT DELAY RESERVES A LAPSED LANE FOR ITS OWN WORKER. A restart, a GC pause and a
// rolling deploy all look like a lease briefly lapsing, and handing those lanes away immediately
// means every deploy is a cluster-wide reshuffle that every other worker then absorbs.
func TestALapsedLeaseIsReservedForItsPreviousHolder(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "lane-a", "w1")

	// Past the TTL, inside the delay.
	f.advance(store.DefaultLeaseTTL + time.Second)
	if _, err := f.c.Claim(context.Background(), f.id("lane-a"), "w2", store.DefaultLeaseTTL); !errors.Is(err, fault.ErrFenced) {
		t.Fatalf("another worker took a lane %v after its lease lapsed (%v); the reassignment delay "+
			"is not being applied", time.Second, err)
	}

	// Its own worker comes back, which is the case the delay exists for.
	back, err := f.c.Claim(context.Background(), f.id("lane-a"), "w1", store.DefaultLeaseTTL)
	if err != nil {
		t.Fatalf("the previous holder could not reclaim its own lapsed lane: %v", err)
	}
	if !back.Valid(f.now) {
		t.Error("the reclaimed lease is already invalid")
	}

	// And once the delay is genuinely past, the lane does move — a reservation that never expires is
	// a lane nobody can ever read again.
	f.claim(t, "lane-b", "w1")
	f.advance(store.DefaultLeaseTTL + store.DefaultReassignmentDelay + time.Second)
	if _, err := f.c.Claim(context.Background(), f.id("lane-b"), "w2", store.DefaultLeaseTTL); err != nil {
		t.Fatalf("a lane whose holder is long gone could not be taken: %v", err)
	}
}

// THE DELAY IS MEASURED FROM THE LEASE'S OWN EXPIRY, not from whenever somebody first asked. A delay
// that started on the first claim attempt would be extended by every attempt, so a busy cluster
// would never reassign anything.
func TestTheReassignmentDelayRunsFromExpiryNotFromTheFirstAttempt(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "lane-a", "w1")

	f.advance(store.DefaultLeaseTTL + time.Second)
	// Somebody asks early and is refused. That must not restart the clock.
	if _, err := f.c.Claim(context.Background(), f.id("lane-a"), "w2", store.DefaultLeaseTTL); err == nil {
		t.Fatal("the lane was taken inside the delay, so this test proves nothing")
	}

	// Now just past the delay, measured from expiry. If the refusal above restarted it, this fails.
	f.advance(store.DefaultReassignmentDelay)
	if _, err := f.c.Claim(context.Background(), f.id("lane-a"), "w2", store.DefaultLeaseTTL); err != nil {
		t.Fatalf("the lane was still reserved %v after its lease expired, which is past the %v delay: %v",
			store.DefaultLeaseTTL+store.DefaultReassignmentDelay+time.Second,
			store.DefaultReassignmentDelay, err)
	}
}

// A RELEASED LANE IS FREE AT ONCE. The delay is for a worker that vanished; one that hands a lane
// back has said it is not coming, and making a clean shutdown wait two minutes would make every
// rolling deploy a stall.
func TestAReleasedLaneIsImmediatelyAvailable(t *testing.T) {
	f := newFixture(t)
	l := f.claim(t, "lane-a", "w1")

	if err := f.c.Release(context.Background(), l); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := f.c.Claim(context.Background(), f.id("lane-a"), "w2", store.DefaultLeaseTTL); err != nil {
		t.Fatalf("a released lane was still reserved: %v", err)
	}
}

// RENEWING KEEPS THE EPOCH. A renewal that took a new one would fence the holder's own in-flight
// writes every ten seconds, which is the opposite of what renewing is for.
func TestRenewExtendsTheLeaseAndKeepsTheEpoch(t *testing.T) {
	f := newFixture(t)
	l := f.claim(t, "lane-a", "w1")

	f.advance(store.DefaultRenewInterval)
	got, err := f.c.Renew(context.Background(), l)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if got.Epoch != l.Epoch {
		t.Errorf("Renew changed the epoch from %d to %d; a renewal is the same holding continuing",
			l.Epoch, got.Epoch)
	}
	if !got.Expires.After(l.Expires) {
		t.Errorf("Renew left the expiry at %s; it extended nothing", got.Expires)
	}

	// A LEASE THAT HAS ALREADY LAPSED IS NOT RENEWED. The row may still name this worker — the delay
	// is holding it — but the window in which somebody else could legally have taken it has been
	// open, so extending it now would be re-taking the lane without the fence a claim carries.
	f.advance(store.DefaultLeaseTTL + time.Second)
	if _, err := f.c.Renew(context.Background(), got); !errors.Is(err, fault.ErrFenced) {
		t.Errorf("an expired lease renewed successfully (%v); the holder would never learn it had "+
			"been out of contact, which is exactly when it must stop", err)
	}
}

// A GATED LANE IS NOT OFFERED TO ANYBODY. This is the snapshot-to-stream handoff, and it holds
// cluster-wide because it is data in the plan rather than a per-connector convention.
func TestAGatedLaneIsNotClaimableUntilItsPredecessorIsDurablyFinished(t *testing.T) {
	scan := store.LaneRow{ID: "scan-1", Name: "scan", Group: "scan"}
	tail := store.LaneRow{ID: "tail-1", Name: "tail", After: []record.LaneGroup{"scan"}}
	f := newFixture(t, scan, tail)

	if _, err := f.c.Claim(context.Background(), f.id("tail-1"), "w1", store.DefaultLeaseTTL); !errors.Is(err, fault.ErrFenced) {
		t.Fatalf("a gated lane was claimed (%v); the tail would read while the snapshot is still "+
			"running and the handoff is lost", err)
	}
	// The ungated one is claimable throughout.
	f.claim(t, "scan-1", "w1")

	// DURABLY finished, not merely finished: a gate that fires on a fact that did not survive a crash
	// is a gate that can open twice.
	scan.Finished = true
	if err := f.c.Plan(context.Background(), tenant, pipe, gen, []store.LaneRow{scan, tail}); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if _, err := f.c.Claim(context.Background(), f.id("tail-1"), "w1", store.DefaultLeaseTTL); !errors.Is(err, fault.ErrFenced) {
		t.Errorf("the gate opened on a finish with no FinishedAt (%v); that is a finish nobody has "+
			"confirmed survived a crash", err)
	}

	scan.FinishedAt = f.now
	if err := f.c.Plan(context.Background(), tenant, pipe, gen, []store.LaneRow{scan, tail}); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if _, err := f.c.Claim(context.Background(), f.id("tail-1"), "w1", store.DefaultLeaseTTL); err != nil {
		t.Errorf("the gate never opened although its predecessor is durably finished: %v", err)
	}
}

// A GATE IS A STATEMENT ABOUT THE WHOLE PLAN, so it has to be evaluated after every row is in. A
// coordinator that gated each row as it wrote it would answer from a half-applied plan — and would
// open a gate on a predecessor the very same call was about to add.
func TestAGateIsEvaluatedAgainstTheWholePlanNotTheRowsSoFar(t *testing.T) {
	f := newFixture(t, store.LaneRow{ID: "tail-1", Name: "tail", After: []record.LaneGroup{"scan"}})

	// With no scan row at all the tail is ungated: nothing is outstanding.
	if _, err := f.c.Claim(context.Background(), f.id("tail-1"), "w1", store.DefaultLeaseTTL); err != nil {
		t.Fatalf("a lane waiting on a group with no members was gated: %v", err)
	}

	// Now the scan arrives in the same plan, AFTER the tail in the slice. If gating were computed
	// per row as it was written, the tail would be evaluated before the scan existed.
	rows := []store.LaneRow{
		{ID: "tail-1", Name: "tail", After: []record.LaneGroup{"scan"}},
		{ID: "scan-1", Name: "scan", Group: "scan"},
	}
	if err := f.c.Plan(context.Background(), tenant, pipe, gen, rows); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	as := assignmentsByID(t, f)
	if !as[f.id("tail-1")].Gated {
		t.Error("the tail is ungated although an unfinished scan is in the same plan; the gate was " +
			"computed before the plan was complete")
	}
}

// PLANNING ON A TIMER MUST NOT MOVE ANYTHING. A re-plan updates lane data and re-evaluates gates and
// leaves placement alone; a planner that dropped claims each pass would move every lane in the
// cluster every time anybody finished anything.
func TestReplanningPreservesPlacementAndDropsWithdrawnLanes(t *testing.T) {
	f := newFixture(t)
	l := f.claim(t, "lane-a", "w1")

	rows := []store.LaneRow{
		{ID: "lane-a", Name: "a", Weight: 99},
		{ID: "lane-c", Name: "c"},
	}
	if err := f.c.Plan(context.Background(), tenant, pipe, gen, rows); err != nil {
		t.Fatalf("Plan: %v", err)
	}

	as := assignmentsByID(t, f)
	kept := as[f.id("lane-a")]
	if kept.Worker != "w1" || kept.Epoch != l.Epoch {
		t.Errorf("re-planning moved a held lane: it is now %s at epoch %d, was w1 at %d",
			kept.Worker, kept.Epoch, l.Epoch)
	}
	if kept.Lane.Weight != 99 {
		t.Error("re-planning did not update the lane's own data, so a plan can never change anything")
	}
	if _, err := f.c.Renew(context.Background(), l); err != nil {
		t.Errorf("the lease did not survive a re-plan: %v", err)
	}

	// A lane the source stopped announcing stops being offered.
	if _, ok := as[f.id("lane-b")]; ok {
		t.Error("a lane that is no longer in the plan is still assignable")
	}
	if _, ok := as[f.id("lane-c")]; !ok {
		t.Error("a lane added by the re-plan was not written")
	}
}

// A GENERATION IS A DIFFERENT ROW. "An assignment from an older generation is not claimed" is
// enforceable only because the id differs, so the old rows are not silently reused under a spec that
// may have changed underneath them.
func TestAssignmentsAreDistinctPerGenerationAndStableWithinOne(t *testing.T) {
	a := memstore.AssignmentIDFor(tenant, pipe, 7, "lane-a")
	if again := memstore.AssignmentIDFor(tenant, pipe, 7, "lane-a"); again != a {
		t.Errorf("the same lane in the same generation derived two ids, %s and %s; a re-plan would "+
			"drop every claim", a, again)
	}
	if next := memstore.AssignmentIDFor(tenant, pipe, 8, "lane-a"); next == a {
		t.Error("a new generation reused the old assignment id, so an assignment planned against " +
			"the previous spec would keep its lease")
	}
	// Escaped, so a tenant containing a separator cannot address another tenant's assignment.
	if memstore.AssignmentIDFor("a/b", pipe, 7, "x") == memstore.AssignmentIDFor("a", "b/"+pipe, 7, "x") {
		t.Error("two different tenants derived one assignment id; the separator is not escaped")
	}
}

// A NEW GENERATION SUPERSEDES THE OLD ONE COMPLETELY. Leaving the old rows in place would make
// Assignments answer with a mixture of two specs, with nothing that ever cleans up the older — and
// the worker holding a superseded lease would never be told to stop.
func TestPlanningANewGenerationRetiresTheOldRowsAndRefusesToGoBackwards(t *testing.T) {
	f := newFixture(t)
	old := f.claim(t, "lane-a", "w1")

	next := gen + 1
	rows := []store.LaneRow{{ID: "lane-a", Name: "a"}}
	if err := f.c.Plan(context.Background(), tenant, pipe, next, rows); err != nil {
		t.Fatalf("planning the next generation: %v", err)
	}

	as := assignmentsByID(t, f)
	if len(as) != 1 {
		t.Errorf("Assignments returned %d rows across generations; a superseded plan is still there "+
			"and a reader cannot tell which spec it is looking at", len(as))
	}
	if _, ok := as[memstore.AssignmentIDFor(tenant, pipe, next, "lane-a")]; !ok {
		t.Error("the new generation's row is missing")
	}

	// The holder of a superseded lease finds out the way it finds out about everything else.
	if _, err := f.c.Renew(context.Background(), old); !errors.Is(err, fault.ErrFenced) {
		t.Errorf("a lease from a retired generation renewed successfully (%v); that worker would "+
			"keep reading under a spec nobody is running", err)
	}

	// AND PLANNING BACKWARDS IS REFUSED. A planner that has just been fenced can still hold an old
	// generation in a local variable; applying it would delete the current plan and hand every lane
	// back for re-claiming under a spec nobody is running.
	if err := f.c.Plan(context.Background(), tenant, pipe, gen, rows); err == nil {
		t.Error("an older generation was planned over a newer one")
	}
	if got := assignmentsByID(t, f); len(got) != 1 {
		t.Errorf("the refused plan still changed the table: %d rows", len(got))
	}
}

// Membership is a live view, and its Changes channel is what a worker selects on rather than polling.
func TestMembershipReportsTheWorkerSetAndSignalsChanges(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	m1, err := f.c.Join(ctx, store.WorkerInfo{ID: "w1", Tenant: tenant, Version: "1.0.0"})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	changed := m1.Changes()

	if _, err := f.c.Join(ctx, store.WorkerInfo{ID: "w2", Tenant: tenant, Version: "1.0.0"}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	select {
	case <-changed:
	default:
		t.Fatal("the worker set changed and Changes was not signalled; a worker would have to poll")
	}

	ws, err := m1.Workers(ctx)
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if len(ws) != 2 || ws[0].ID != "w1" || ws[1].ID != "w2" {
		t.Fatalf("the worker set is %v, want w1 and w2 in order", ws)
	}

	// LEAVING DOES NOT RELEASE LEASES, which is not an omission: membership says who is here and the
	// lease says who holds what. Dropping a leaving worker's lanes would be a much weaker fence than
	// the epoch, and it would fire on a worker that is merely re-registering.
	l := f.claim(t, "lane-a", "w1")
	if err := m1.Leave(ctx); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	if _, err := f.c.Renew(ctx, l); err != nil {
		t.Errorf("leaving the worker set invalidated a lease: %v", err)
	}
	if _, err := f.c.Claim(ctx, f.id("lane-a"), "w2", store.DefaultLeaseTTL); !errors.Is(err, fault.ErrFenced) {
		t.Errorf("a departed worker's live lease was taken (%v); the lease is the fence, not "+
			"membership", err)
	}
}

// Leadership is ADVISORY and the second campaigner is told so immediately rather than blocking or
// erroring — store.Leadership says a caller must treat planning as safe to lose.
func TestLeadershipIsAdvisoryAndTheLoserKnowsAtOnce(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	first, err := f.c.Campaign(ctx)
	if err != nil {
		t.Fatalf("Campaign: %v", err)
	}
	if !first.IsLeader() {
		t.Fatal("the first campaigner is not the leader")
	}

	second, err := f.c.Campaign(ctx)
	if err != nil {
		t.Fatalf("a second campaign returned an error; losing an election is not a failure: %v", err)
	}
	if second.IsLeader() {
		t.Error("two campaigners both believe they are the leader")
	}
	select {
	case <-second.Lost():
	default:
		t.Error("the loser's Lost channel is open, so it would plan until some timeout it has to invent")
	}

	// AND A LOST ELECTION CHANGES NOTHING ABOUT PLACEMENT. Planning is the only thing leadership
	// gates, and the fence is the epoch.
	if _, err := f.c.Claim(ctx, f.id("lane-a"), "w2", store.DefaultLeaseTTL); err != nil {
		t.Errorf("a non-leader could not claim a lane: %v", err)
	}

	if err := first.Resign(ctx); err != nil {
		t.Fatalf("Resign: %v", err)
	}
	select {
	case <-first.Lost():
	default:
		t.Error("a resigned leader's Lost channel is still open")
	}
	third, _ := f.c.Campaign(ctx)
	if !third.IsLeader() {
		t.Error("nobody could become leader after the incumbent resigned")
	}
}

// A LAPSED ROW GOES ON NAMING ITS PREVIOUS HOLDER, and the expiry is the discriminator. That
// identity is what the reassignment delay reserves the lane for, so it cannot be cleared — which
// means a reader that treats Worker as "the current holder" is wrong in exactly the situation an
// operator is most likely to be looking at: a worker that just died.
func TestALapsedRowStillNamesItsPreviousHolderAndSaysSoThroughTheExpiry(t *testing.T) {
	f := newFixture(t)
	f.claim(t, "lane-a", "w1")
	f.advance(store.DefaultLeaseTTL + time.Second)

	row := assignmentsByID(t, f)[f.id("lane-a")]
	if row.Worker != "w1" {
		t.Errorf("the lapsed row names %q; the previous holder's identity is what the reassignment "+
			"delay reserves the lane for and it cannot be cleared", row.Worker)
	}
	if (store.Lease{Expires: row.LeaseExpires}).Valid(f.now) {
		t.Error("the row reports a valid lease after its TTL; the expiry is the only thing that " +
			"tells a reader the named worker is the previous one")
	}

	// And after a clean release there is no holder at all, which is the state Worker being empty
	// actually means.
	l := f.claim(t, "lane-b", "w1")
	if err := f.c.Release(context.Background(), l); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := assignmentsByID(t, f)[f.id("lane-b")]; got.Worker != "" || got.Epoch != 0 {
		t.Errorf("a released row still names %s at epoch %d", got.Worker, got.Epoch)
	}
}

// A finished lane is not claimable: there is nothing left to read, and handing it out would have a
// worker open a source for a lane that is already done.
func TestAFinishedLaneIsNotClaimable(t *testing.T) {
	done := store.LaneRow{ID: "lane-a", Name: "a", Finished: true, FinishedAt: time.Now()}
	f := newFixture(t, done)

	if _, err := f.c.Claim(context.Background(), f.id("lane-a"), "w1", store.DefaultLeaseTTL); !errors.Is(err, fault.ErrFenced) {
		t.Errorf("a finished lane was claimed: %v", err)
	}
}

func assignmentsByID(t *testing.T, f *fixture) map[store.AssignmentID]store.Assignment {
	t.Helper()
	as, err := f.c.Assignments(context.Background(), tenant, pipe)
	if err != nil {
		t.Fatalf("Assignments: %v", err)
	}
	out := make(map[store.AssignmentID]store.Assignment, len(as))
	for _, a := range as {
		out[a.ID] = a
	}
	return out
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
