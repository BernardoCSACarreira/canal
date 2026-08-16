// Package coordtest is the conformance suite for [store.Coordinator], on the same reasoning as
// pkg/storetest: a contract two implementations each prove separately gets proved wrong twice, and
// the placement protocol is the contract the whole multi-worker design leans on.
//
// THE RULES HERE WERE PINNED BEFORE THE ENGINE READ THEM — they began as the in-memory
// coordinator's own suite, written when store.Coordinator had no implementation at all — and they
// are the rules the engine's lease table now assumes: the epoch fences, the lease expires, the
// delay reserves, the gate holds. An implementation that passes this suite can be handed to
// internal/engine/lease.go; one that does not has no business under a pipeline.
//
// THE CLOCK IS THE SUBJECT'S TO WIRE. Lease expiry is the one behaviour a test cannot drive any
// other way: waiting out a real thirty-second TTL is a thirty-second test, and shortening the TTL
// to a millisecond tests a lease nobody deploys. New receives a [Clock]; the implementation must
// read its time through it. An implementation that cannot inject a clock cannot run this suite,
// and that is a finding about the implementation.
package coordtest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// Clock is the controllable time source a Subject wires into its coordinator.
type Clock struct {
	mu sync.Mutex
	t  time.Time
}

// NewClock starts at a fixed instant, deliberately: two runs of the suite see the same times, so a
// failure reproduces rather than depending on when the test ran.
func NewClock() *Clock {
	return &Clock{t: time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)}
}

// Now is the coordinator's time source.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance moves the clock forward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// Subject is one implementation under test.
type Subject struct {
	Name string

	// New returns a coordinator over a FRESH, EMPTY coordination domain whose clock is clk. Every
	// case calls it once, so state cannot leak between cases.
	New func(t *testing.T, clk *Clock) store.Coordinator

	// Attach returns a SECOND handle onto the same domain as c — a separate instance the way a
	// second worker process would hold one — or nil when the implementation has no such thing (an
	// in-memory coordinator's domain IS its instance; returning c itself is the honest answer
	// there). Cases that need two instances skip when Attach is nil.
	Attach func(t *testing.T, clk *Clock, c store.Coordinator) store.Coordinator
}

// Run executes the conformance suite against one subject.
func Run(t *testing.T, s Subject) {
	t.Run(s.Name+"/every_claim_takes_a_new_epoch", func(t *testing.T) { testClaimEpochs(t, s) })
	t.Run(s.Name+"/a_live_lease_cannot_be_taken", func(t *testing.T) { testLiveLeaseHeld(t, s) })
	t.Run(s.Name+"/a_lapsed_lease_is_reserved_for_its_holder", func(t *testing.T) { testReassignmentDelay(t, s) })
	t.Run(s.Name+"/the_delay_runs_from_expiry", func(t *testing.T) { testDelayFromExpiry(t, s) })
	t.Run(s.Name+"/a_released_lane_is_immediately_available", func(t *testing.T) { testReleaseFrees(t, s) })
	t.Run(s.Name+"/renew_extends_and_keeps_the_epoch", func(t *testing.T) { testRenew(t, s) })
	t.Run(s.Name+"/a_gated_lane_waits_for_a_durable_finish", func(t *testing.T) { testGates(t, s) })
	t.Run(s.Name+"/gates_are_evaluated_against_the_whole_plan", func(t *testing.T) { testGateWholePlan(t, s) })
	t.Run(s.Name+"/replanning_preserves_placement", func(t *testing.T) { testReplan(t, s) })
	t.Run(s.Name+"/assignment_ids_are_stable_and_generation_scoped", func(t *testing.T) { testAssignmentIDs(t, s) })
	t.Run(s.Name+"/a_new_generation_supersedes_completely", func(t *testing.T) { testGenerations(t, s) })
	t.Run(s.Name+"/membership_reports_and_signals", func(t *testing.T) { testMembership(t, s) })
	t.Run(s.Name+"/leadership_is_advisory", func(t *testing.T) { testLeadership(t, s) })
	t.Run(s.Name+"/a_lapsed_row_names_its_previous_holder", func(t *testing.T) { testLapsedRow(t, s) })
	t.Run(s.Name+"/a_rejected_plan_changes_nothing", func(t *testing.T) { testRejectedPlan(t, s) })
	t.Run(s.Name+"/worker_labels_are_copied", func(t *testing.T) { testLabelCopies(t, s) })
	t.Run(s.Name+"/a_finished_lane_is_not_claimable", func(t *testing.T) { testFinishedLane(t, s) })
	t.Run(s.Name+"/two_instances_share_one_domain", func(t *testing.T) { testTwoInstances(t, s) })
}

const (
	tenant = record.TenantID("acme")
	pipe   = record.PipelineID("p1")
	gen    = uint64(7)
)

// fixture is a coordinator with a controllable clock and a planned pipeline.
type fixture struct {
	c   store.Coordinator
	clk *Clock
}

func newFixture(t *testing.T, s Subject, rows ...store.LaneRow) *fixture {
	t.Helper()
	clk := NewClock()
	f := &fixture{c: s.New(t, clk), clk: clk}
	if len(rows) == 0 {
		rows = []store.LaneRow{{ID: "lane-a", Name: "a"}, {ID: "lane-b", Name: "b"}}
	}
	if err := f.c.Plan(context.Background(), tenant, pipe, gen, rows); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	return f
}

func (f *fixture) id(lane record.LaneID) store.AssignmentID {
	return store.AssignmentIDFor(tenant, pipe, gen, lane)
}

func (f *fixture) claim(t *testing.T, lane record.LaneID, w store.WorkerID) store.Lease {
	t.Helper()
	l, err := f.c.Claim(context.Background(), f.id(lane), w, store.DefaultLeaseTTL)
	if err != nil {
		t.Fatalf("claiming %s for %s: %v", lane, w, err)
	}
	return l
}

func (f *fixture) assignments(t *testing.T) map[store.AssignmentID]store.Assignment {
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

// THE EPOCH IS THE FENCE, and every claim takes a new one. That is what makes the loser of a race
// hold a number the store refuses, rather than two workers both believing they are current.
func testClaimEpochs(t *testing.T, s Subject) {
	f := newFixture(t, s)

	first := f.claim(t, "lane-a", "w1")
	if first.Epoch == 0 {
		t.Fatal("a claim was granted epoch 0; zero is the value that means unclaimed")
	}

	// A FAST RESTART IS THE SAME WORKER ID AND A DIFFERENT PROCESS. It must be able to take its own
	// lane back while the old lease is still live, and the epoch it gets must fence whatever the
	// old process still has in flight.
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
func testLiveLeaseHeld(t *testing.T, s Subject) {
	f := newFixture(t, s)
	f.claim(t, "lane-a", "w1")

	_, err := f.c.Claim(context.Background(), f.id("lane-a"), "w2", store.DefaultLeaseTTL)
	if !errors.Is(err, fault.ErrFenced) {
		t.Fatalf("a second worker claimed a live lease (%v); two workers now read one lane", err)
	}
	if err != nil && !strings.Contains(err.Error(), "w1") {
		t.Errorf("the refusal %q does not name who holds it; an operator debugging a stuck lane "+
			"needs to know where it went", err)
	}
}

// THE REASSIGNMENT DELAY RESERVES A LAPSED LANE FOR ITS OWN WORKER. A restart, a GC pause and a
// rolling deploy all look like a lease briefly lapsing, and handing those lanes away immediately
// means every deploy is a cluster-wide reshuffle that every other worker then absorbs.
func testReassignmentDelay(t *testing.T, s Subject) {
	f := newFixture(t, s)
	f.claim(t, "lane-a", "w1")

	// Past the TTL, inside the delay.
	f.clk.Advance(store.DefaultLeaseTTL + time.Second)
	if _, err := f.c.Claim(context.Background(), f.id("lane-a"), "w2", store.DefaultLeaseTTL); !errors.Is(err, fault.ErrFenced) {
		t.Fatalf("another worker took a lane %v after its lease lapsed (%v); the reassignment delay "+
			"is not being applied", time.Second, err)
	}

	// Its own worker comes back, which is the case the delay exists for.
	back, err := f.c.Claim(context.Background(), f.id("lane-a"), "w1", store.DefaultLeaseTTL)
	if err != nil {
		t.Fatalf("the previous holder could not reclaim its own lapsed lane: %v", err)
	}
	if !back.Valid(f.clk.Now()) {
		t.Error("the reclaimed lease is already invalid")
	}

	// And once the delay is genuinely past, the lane does move — a reservation that never expires
	// is a lane nobody can ever read again.
	f.claim(t, "lane-b", "w1")
	f.clk.Advance(store.DefaultLeaseTTL + store.DefaultReassignmentDelay + time.Second)
	if _, err := f.c.Claim(context.Background(), f.id("lane-b"), "w2", store.DefaultLeaseTTL); err != nil {
		t.Fatalf("a lane whose holder is long gone could not be taken: %v", err)
	}
}

// THE DELAY IS MEASURED FROM THE LEASE'S OWN EXPIRY, not from whenever somebody first asked. A
// delay that started on the first claim attempt would be extended by every attempt, so a busy
// cluster would never reassign anything.
func testDelayFromExpiry(t *testing.T, s Subject) {
	f := newFixture(t, s)
	f.claim(t, "lane-a", "w1")

	f.clk.Advance(store.DefaultLeaseTTL + time.Second)
	// Somebody asks early and is refused. That must not restart the clock.
	if _, err := f.c.Claim(context.Background(), f.id("lane-a"), "w2", store.DefaultLeaseTTL); err == nil {
		t.Fatal("the lane was taken inside the delay, so this test proves nothing")
	}

	// Now just past the delay, measured from expiry. If the refusal above restarted it, this fails.
	f.clk.Advance(store.DefaultReassignmentDelay)
	if _, err := f.c.Claim(context.Background(), f.id("lane-a"), "w2", store.DefaultLeaseTTL); err != nil {
		t.Fatalf("the lane was still reserved %v after its lease expired, which is past the %v delay: %v",
			store.DefaultLeaseTTL+store.DefaultReassignmentDelay+time.Second,
			store.DefaultReassignmentDelay, err)
	}
}

// A RELEASED LANE IS FREE AT ONCE. The delay is for a worker that vanished; one that hands a lane
// back has said it is not coming, and making a clean shutdown wait two minutes would make every
// rolling deploy a stall.
func testReleaseFrees(t *testing.T, s Subject) {
	f := newFixture(t, s)
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
func testRenew(t *testing.T, s Subject) {
	f := newFixture(t, s)
	l := f.claim(t, "lane-a", "w1")

	f.clk.Advance(store.DefaultRenewInterval)
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

	// A LEASE THAT HAS ALREADY LAPSED IS NOT RENEWED. The row may still name this worker — the
	// delay is holding it — but the window in which somebody else could legally have taken it has
	// been open, so extending it now would be re-taking the lane without the fence a claim carries.
	f.clk.Advance(store.DefaultLeaseTTL + time.Second)
	if _, err := f.c.Renew(context.Background(), got); !errors.Is(err, fault.ErrFenced) {
		t.Errorf("an expired lease renewed successfully (%v); the holder would never learn it had "+
			"been out of contact, which is exactly when it must stop", err)
	}
}

// A GATED LANE IS NOT OFFERED TO ANYBODY. This is the snapshot-to-stream handoff, and it holds
// cluster-wide because it is data in the plan rather than a per-connector convention.
func testGates(t *testing.T, s Subject) {
	scan := store.LaneRow{ID: "scan-1", Name: "scan", Group: "scan"}
	tail := store.LaneRow{ID: "tail-1", Name: "tail", After: []record.LaneGroup{"scan"}}
	f := newFixture(t, s, scan, tail)

	if _, err := f.c.Claim(context.Background(), f.id("tail-1"), "w1", store.DefaultLeaseTTL); !errors.Is(err, fault.ErrFenced) {
		t.Fatalf("a gated lane was claimed (%v); the tail would read while the snapshot is still "+
			"running and the handoff is lost", err)
	}
	// The ungated one is claimable throughout.
	f.claim(t, "scan-1", "w1")

	// DURABLY finished, not merely finished: a gate that fires on a fact that did not survive a
	// crash is a gate that can open twice.
	scan.Finished = true
	if err := f.c.Plan(context.Background(), tenant, pipe, gen, []store.LaneRow{scan, tail}); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if _, err := f.c.Claim(context.Background(), f.id("tail-1"), "w1", store.DefaultLeaseTTL); !errors.Is(err, fault.ErrFenced) {
		t.Errorf("the gate opened on a finish with no FinishedAt (%v); that is a finish nobody has "+
			"confirmed survived a crash", err)
	}

	scan.FinishedAt = f.clk.Now()
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
func testGateWholePlan(t *testing.T, s Subject) {
	f := newFixture(t, s, store.LaneRow{ID: "tail-1", Name: "tail", After: []record.LaneGroup{"scan"}})

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
	if !f.assignments(t)[f.id("tail-1")].Gated {
		t.Error("the tail is ungated although an unfinished scan is in the same plan; the gate was " +
			"computed before the plan was complete")
	}
}

// PLANNING ON A TIMER MUST NOT MOVE ANYTHING. A re-plan updates lane data and re-evaluates gates
// and leaves placement alone; a planner that dropped claims each pass would move every lane in the
// cluster every time anybody finished anything.
func testReplan(t *testing.T, s Subject) {
	f := newFixture(t, s)
	l := f.claim(t, "lane-a", "w1")

	rows := []store.LaneRow{
		{ID: "lane-a", Name: "a", Weight: 99},
		{ID: "lane-c", Name: "c"},
	}
	if err := f.c.Plan(context.Background(), tenant, pipe, gen, rows); err != nil {
		t.Fatalf("Plan: %v", err)
	}

	as := f.assignments(t)
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
// enforceable only because the id differs, so the old rows are not silently reused under a spec
// that may have changed underneath them. This one holds for every implementation at once, because
// store.AssignmentIDFor is one function used by all of them — a second derivation is the bug it
// exists to prevent.
func testAssignmentIDs(t *testing.T, _ Subject) {
	a := store.AssignmentIDFor(tenant, pipe, 7, "lane-a")
	if again := store.AssignmentIDFor(tenant, pipe, 7, "lane-a"); again != a {
		t.Errorf("the same lane in the same generation derived two ids, %s and %s; a re-plan would "+
			"drop every claim", a, again)
	}
	if next := store.AssignmentIDFor(tenant, pipe, 8, "lane-a"); next == a {
		t.Error("a new generation reused the old assignment id, so an assignment planned against " +
			"the previous spec would keep its lease")
	}
	// Escaped, so a tenant containing a separator cannot address another tenant's assignment.
	if store.AssignmentIDFor("a/b", pipe, 7, "x") == store.AssignmentIDFor("a", record.PipelineID("b/"+string(pipe)), 7, "x") {
		t.Error("two different tenants derived one assignment id; the separator is not escaped")
	}
}

// A NEW GENERATION SUPERSEDES THE OLD ONE COMPLETELY. Leaving the old rows in place would make
// Assignments answer with a mixture of two specs, with nothing that ever cleans up the older — and
// the worker holding a superseded lease would never be told to stop.
func testGenerations(t *testing.T, s Subject) {
	f := newFixture(t, s)
	old := f.claim(t, "lane-a", "w1")

	next := gen + 1
	rows := []store.LaneRow{{ID: "lane-a", Name: "a"}}
	if err := f.c.Plan(context.Background(), tenant, pipe, next, rows); err != nil {
		t.Fatalf("planning the next generation: %v", err)
	}

	as := f.assignments(t)
	if len(as) != 1 {
		t.Errorf("Assignments returned %d rows across generations; a superseded plan is still there "+
			"and a reader cannot tell which spec it is looking at", len(as))
	}
	if _, ok := as[store.AssignmentIDFor(tenant, pipe, next, "lane-a")]; !ok {
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
	if got := f.assignments(t); len(got) != 1 {
		t.Errorf("the refused plan still changed the table: %d rows", len(got))
	}
}

// Membership is a live view, and its Changes channel is what a worker selects on rather than
// polling.
func testMembership(t *testing.T, s Subject) {
	f := newFixture(t, s)
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

	// LEAVING DOES NOT RELEASE LEASES, which is not an omission: membership says who is here and
	// the lease says who holds what. Dropping a leaving worker's lanes would be a much weaker fence
	// than the epoch, and it would fire on a worker that is merely re-registering.
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
func testLeadership(t *testing.T, s Subject) {
	f := newFixture(t, s)
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
	third, err := f.c.Campaign(ctx)
	if err != nil {
		t.Fatalf("Campaign after a resignation: %v", err)
	}
	if !third.IsLeader() {
		t.Error("nobody could become leader after the incumbent resigned")
	}
}

// A LAPSED ROW GOES ON NAMING ITS PREVIOUS HOLDER, and the expiry is the discriminator. That
// identity is what the reassignment delay reserves the lane for, so it cannot be cleared — which
// means a reader that treats Worker as "the current holder" is wrong in exactly the situation an
// operator is most likely to be looking at: a worker that just died.
func testLapsedRow(t *testing.T, s Subject) {
	f := newFixture(t, s)
	f.claim(t, "lane-a", "w1")
	f.clk.Advance(store.DefaultLeaseTTL + time.Second)

	row := f.assignments(t)[f.id("lane-a")]
	if row.Worker != "w1" {
		t.Errorf("the lapsed row names %q; the previous holder's identity is what the reassignment "+
			"delay reserves the lane for and it cannot be cleared", row.Worker)
	}
	if (store.Lease{Expires: row.LeaseExpires}).Valid(f.clk.Now()) {
		t.Error("the row reports a valid lease after its TTL; the expiry is the only thing that " +
			"tells a reader the named worker is the previous one")
	}

	// And after a clean release there is no holder at all, which is the state Worker being empty
	// actually means.
	l := f.claim(t, "lane-b", "w1")
	if err := f.c.Release(context.Background(), l); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := f.assignments(t)[f.id("lane-b")]; got.Worker != "" || got.Epoch != 0 {
		t.Errorf("a released row still names %s at epoch %d", got.Worker, got.Epoch)
	}
}

// A REJECTED PLAN LEAVES NO PARTIAL STATE, the same rule StateStore.Set states. Validating inside
// the apply loop means a plan whose fifth row is malformed has already written the first four and
// has not yet deleted anything, so the table holds half of one plan and half of the last.
func testRejectedPlan(t *testing.T, s Subject) {
	f := newFixture(t, s)
	before := f.assignments(t)

	bad := []store.LaneRow{
		{ID: "lane-a", Name: "a", Weight: 42},
		{ID: "lane-c", Name: "c"},
		{ID: "", Name: "nameless"},
	}
	if err := f.c.Plan(context.Background(), tenant, pipe, gen, bad); err == nil {
		t.Fatal("a plan containing a row with no id was accepted")
	}

	after := f.assignments(t)
	if len(after) != len(before) {
		t.Errorf("the rejected plan left %d rows, was %d; it was applied up to the bad row",
			len(after), len(before))
	}
	if _, ok := after[f.id("lane-c")]; ok {
		t.Error("a row from the rejected plan was written")
	}
	if got := after[f.id("lane-a")].Lane.Weight; got != 0 {
		t.Errorf("lane-a carries weight %d from the rejected plan; the rows before the bad one were "+
			"applied", got)
	}
	if _, ok := after[f.id("lane-b")]; !ok {
		t.Error("lane-b is gone although the plan that would have withdrawn it was rejected")
	}

	// TWO ROWS FOR ONE LANE IS A PLANNER BUG. Taking the last silently makes it a bug that surfaces
	// later as a lane whose gate or weight keeps changing for no reason anybody can trace.
	dup := []store.LaneRow{{ID: "lane-a", Name: "a"}, {ID: "lane-a", Name: "a-again"}}
	if err := f.c.Plan(context.Background(), tenant, pipe, gen, dup); err == nil {
		t.Error("a plan naming one lane twice was accepted")
	}
}

// A store that hands out its own maps is a store a caller can edit by accident.
func testLabelCopies(t *testing.T, s Subject) {
	f := newFixture(t, s)
	ctx := context.Background()

	labels := map[string]string{"zone": "eu-west-1a"}
	m, err := f.c.Join(ctx, store.WorkerInfo{ID: "w1", Version: "1.0.0", Labels: labels})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}

	labels["zone"] = "edited-after-joining"
	got, err := m.Workers(ctx)
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if got[0].Labels["zone"] != "eu-west-1a" {
		t.Errorf("the store's copy became %q when the caller edited the map it passed to Join",
			got[0].Labels["zone"])
	}

	got[0].Labels["zone"] = "edited-after-reading"
	again, err := m.Workers(ctx)
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if again[0].Labels["zone"] != "eu-west-1a" {
		t.Errorf("the store's copy became %q when a caller edited what Workers returned",
			again[0].Labels["zone"])
	}
}

// A finished lane is not claimable: there is nothing left to read, and handing it out would have a
// worker open a source for a lane that is already done.
func testFinishedLane(t *testing.T, s Subject) {
	done := store.LaneRow{ID: "lane-a", Name: "a", Finished: true,
		FinishedAt: time.Date(2026, 8, 1, 11, 0, 0, 0, time.UTC)}
	f := newFixture(t, s, done)

	if _, err := f.c.Claim(context.Background(), f.id("lane-a"), "w1", store.DefaultLeaseTTL); !errors.Is(err, fault.ErrFenced) {
		t.Errorf("a finished lane was claimed: %v", err)
	}
}

// TWO INSTANCES OVER ONE DOMAIN ARE ONE COORDINATOR — the case a durable implementation exists
// for, and the one an in-memory implementation honestly cannot have (its Attach returns the same
// instance, which still pins the semantics: what matters is the DOMAIN, never the handle).
func testTwoInstances(t *testing.T, s Subject) {
	if s.Attach == nil {
		t.Skipf("%s has no second-instance story; see Subject.Attach for why that can be legitimate", s.Name)
	}
	f := newFixture(t, s)
	other := s.Attach(t, f.clk, f.c)

	// A claim through one handle fences a claim through the other.
	l := f.claim(t, "lane-a", "w1")
	if _, err := other.Claim(context.Background(), f.id("lane-a"), "w2", store.DefaultLeaseTTL); !errors.Is(err, fault.ErrFenced) {
		t.Fatalf("a lane held through one instance was claimed through another (%v); the domain is "+
			"not shared and two workers would both read it", err)
	}

	// A renewal through the handle that claimed keeps working.
	if _, err := f.c.Renew(context.Background(), l); err != nil {
		t.Fatalf("Renew through the claiming instance: %v", err)
	}

	// And the plan is visible from both sides.
	as, err := other.Assignments(context.Background(), tenant, pipe)
	if err != nil {
		t.Fatalf("Assignments through the second instance: %v", err)
	}
	if len(as) != 2 {
		t.Fatalf("the second instance sees %d assignments, want 2; the plan did not cross", len(as))
	}
}
