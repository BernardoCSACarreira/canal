package memstore

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// Coordinator is an in-memory [store.Coordinator]: membership, an advisory leader claim, and
// assignment leases.
//
// SCAFFOLDING, like the rest of this package (design rule R10). One process, no durability, no
// network. What it is for is the PROTOCOL: leases that expire, epochs that fence, a plan that gates
// on unfinished predecessors, and a reassignment delay that lets a bouncing worker reclaim its own
// lanes. Every one of those is a rule the engine has to be written against, and none of them can be
// written against an interface with no implementation.
//
// It is deliberately NOT a stand-in for the real thing. A single-process coordinator cannot exercise
// the case the design is most careful about — two processes that both believe they are leader — and
// [store.Leadership] says in its own doc that nothing may depend on single-leadership anyway. What
// this does provide is the fencing that DOES carry the correctness: the epoch, and the store
// refusing a stale one.
type Coordinator struct {
	mu sync.Mutex

	// Now is the clock, and it is a field because lease expiry is the one behaviour here that a test
	// cannot drive any other way. A test that waited out a real thirty-second TTL would either be a
	// thirty-second test or a lie about the TTL.
	//
	// It is read under the lock; set it once, before the coordinator is used.
	Now func() time.Time

	workers map[store.WorkerID]store.WorkerInfo

	// changes is closed-and-replaced when the worker set moves, so every live Membership sees it.
	changes chan struct{}

	// leader is the current advisory claim, or nil. ADVISORY: see the type's doc. Nothing in the
	// assignment protocol below consults it.
	leader *leadership

	rows map[store.AssignmentID]*assignmentRow

	// epoch is store-wide and monotonic. Every CLAIM takes the next one, which is what makes the
	// loser of a race hold a number the store will refuse; a Renew keeps the epoch it was granted,
	// because renewing is the same holding continuing rather than a new one.
	epoch uint64
}

// assignmentRow is one lane's placement plus the lease bookkeeping the interface does not expose.
type assignmentRow struct {
	a store.Assignment

	// ttl is remembered because Renew does not take one: renewing extends the lease the claim asked
	// for, not some default the store picked.
	//
	// There is deliberately no second field for WHEN the lease lapsed. The reassignment delay runs
	// from the lease's own expiry, and that is already on the row — a separate copy could only ever
	// hold the same value or a wrong one, and stamping it took a mutation inside a function whose
	// name promised a query.
	ttl time.Duration
}

// NewCoordinator returns an empty coordinator.
func NewCoordinator() *Coordinator {
	return &Coordinator{
		Now:     time.Now,
		workers: map[store.WorkerID]store.WorkerInfo{},
		changes: make(chan struct{}),
		rows:    map[store.AssignmentID]*assignmentRow{},
	}
}

// AssignmentIDFor is store.AssignmentIDFor, re-exported so this package's own tests can name it.
//
// It is deliberately NOT a second derivation. A planner and a worker that derived assignment ids
// differently would plan rows nobody could claim, so there is one function and every caller uses it.
func AssignmentIDFor(t record.TenantID, p record.PipelineID, gen uint64, lane record.LaneID) store.AssignmentID {
	return store.AssignmentIDFor(t, p, gen, lane)
}

// --- membership ----------------------------------------------------------------------------------

// Join publishes this worker and returns a live view of the set.
func (c *Coordinator) Join(_ context.Context, w store.WorkerInfo) (store.Membership, error) {
	if w.ID == "" {
		return nil, fault.Contract(fault.OpOpen,
			fmt.Errorf("memstore: a worker needs an id to join"))
	}
	// LABELS ARE COPIED IN AND OUT. They are the one map on the interface, and a store that hands out
	// its own is a store a caller can edit by accident — the deployment's zone tag changing because
	// somebody ranged over what Workers returned and normalised it in place.
	w.Labels = maps.Clone(w.Labels)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers[w.ID] = w
	c.notifyLocked()
	return &membership{c: c, id: w.ID}, nil
}

// notifyLocked wakes everything watching the worker set. It runs under c.mu.
func (c *Coordinator) notifyLocked() {
	close(c.changes)
	c.changes = make(chan struct{})
}

type membership struct {
	c  *Coordinator
	id store.WorkerID
}

func (m *membership) Workers(context.Context) ([]store.WorkerInfo, error) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	out := make([]store.WorkerInfo, 0, len(m.c.workers))
	for _, w := range m.c.workers {
		w.Labels = maps.Clone(w.Labels)
		out = append(out, w)
	}
	// Ordered, because a caller rendering a worker list must not have it reshuffle on every read.
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *membership) Changes() <-chan struct{} {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	return m.c.changes
}

func (m *membership) Leave(context.Context) error {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if _, ok := m.c.workers[m.id]; !ok {
		return nil
	}
	delete(m.c.workers, m.id)
	// LEAVING DOES NOT RELEASE THIS WORKER'S LEASES, and that is not an omission. A lease is released
	// by Release or by expiring; dropping them here would hand a worker's lanes away the instant it
	// withdrew from membership, which is a different and much weaker fence than the one the epoch
	// gives. Membership says who is here; the lease says who holds what.
	m.c.notifyLocked()
	return nil
}

// --- leadership ----------------------------------------------------------------------------------

// Campaign claims the planner role, which is ADVISORY and never trusted for correctness.
//
// A second campaigner gets a Leadership that reports false rather than an error: [store.Leadership]
// says IsLeader is a local belief, and a caller is required to treat planning as safe to lose. The
// fence is the compare-and-set in Claim, not this.
func (c *Coordinator) Campaign(context.Context) (store.Leadership, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := &leadership{c: c, lost: make(chan struct{})}
	if c.leader == nil {
		c.leader = l
		l.held = true
		return l, nil
	}
	// Not the leader, and it already knows: Lost is closed from the start so a caller selecting on it
	// stops planning immediately rather than after a timeout it would have to invent.
	close(l.lost)
	return l, nil
}

type leadership struct {
	c    *Coordinator
	held bool
	lost chan struct{}
}

func (l *leadership) IsLeader() bool {
	l.c.mu.Lock()
	defer l.c.mu.Unlock()
	return l.held
}

func (l *leadership) Lost() <-chan struct{} { return l.lost }

func (l *leadership) Resign(context.Context) error {
	l.c.mu.Lock()
	defer l.c.mu.Unlock()
	if !l.held {
		return nil
	}
	l.held = false
	l.c.leader = nil
	close(l.lost)
	return nil
}

// --- the plan ------------------------------------------------------------------------------------

// Plan writes the assignment rows for a pipeline's announced lanes.
//
// IDEMPOTENT, AND THAT IS THE WHOLE REASON IT CAN RUN ON A TIMER. A re-plan updates each row's lane
// data and re-evaluates its gate, and leaves the placement — worker, epoch, lease — exactly as it
// was. A planner that dropped claims on every pass would move every lane in the cluster every time
// anybody finished anything.
//
// Rows for this generation that are no longer in the plan are DELETED, which is how a lane that the
// source stopped announcing stops being offered. So are rows for SUPERSEDED generations: a plan is
// the complete statement of what should be running, and store.Assignment.Generation already says an
// assignment from an older generation is not claimed — leaving those rows in place would mean
// Assignments answers with a mixture of two specs and nothing ever cleans up the older one. A worker
// still holding a superseded lease learns through a fenced Renew, which is the mechanism it already
// has to handle.
func (c *Coordinator) Plan(_ context.Context, t record.TenantID, id record.PipelineID,
	gen uint64, rows []store.LaneRow,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// PLANNING BACKWARDS IS REFUSED RATHER THAN OBEYED. A planner that has just been fenced can still
	// be holding an old generation in a local variable, and applying it would delete the current
	// plan and hand every lane back for re-claiming under a spec nobody is running.
	for _, row := range c.rows {
		if row.a.Tenant == t && row.a.Pipeline == id && row.a.Generation > gen {
			return fault.Contract(fault.OpPersist, fmt.Errorf(
				"memstore: %s/%s is planned at generation %d; refusing to plan the older %d",
				t, id, row.a.Generation, gen))
		}
	}

	// CHECKED IN FULL BEFORE ANYTHING IS WRITTEN, which is the same two-pass shape StateStore.Set
	// uses and states the reason for: a rejected write must leave no partial state. Validating inside
	// the apply loop meant a plan whose fifth row was malformed had already written the first four
	// and had not yet deleted anything, so the table held half of one plan and half of the last.
	keep := make(map[store.AssignmentID]bool, len(rows))
	for _, lr := range rows {
		if lr.ID == "" {
			return fault.Contract(fault.OpPersist,
				fmt.Errorf("memstore: a lane row needs an id to be planned"))
		}
		aid := AssignmentIDFor(t, id, gen, lr.ID)
		if keep[aid] {
			// TWO ROWS FOR ONE LANE IS A PLANNER BUG, and taking the last one silently would make it
			// a bug that shows up later as a lane whose gate or weight keeps changing for no reason.
			return fault.Contract(fault.OpPersist,
				fmt.Errorf("memstore: lane %s appears twice in one plan", lr.ID))
		}
		keep[aid] = true
	}

	for _, lr := range rows {
		aid := AssignmentIDFor(t, id, gen, lr.ID)
		row, ok := c.rows[aid]
		if !ok {
			row = &assignmentRow{a: store.Assignment{
				ID: aid, Tenant: t, Pipeline: id, Generation: gen,
			}}
			c.rows[aid] = row
		}
		row.a.Lane = lr
	}

	for aid, row := range c.rows {
		if row.a.Tenant != t || row.a.Pipeline != id {
			continue
		}
		if row.a.Generation < gen || !keep[aid] {
			delete(c.rows, aid)
		}
	}

	// GATES ARE RE-EVALUATED AFTER EVERY ROW IS IN, never as each one is written. A gate is a
	// statement about the OTHER rows in the plan, so computing it mid-loop would answer from a
	// half-applied plan and open a gate on a predecessor this very call was about to add.
	c.regateLocked(t, id, gen)
	return nil
}

// regateLocked recomputes Gated for one pipeline generation. It runs under c.mu.
//
// A lane is gated while any group it names in After still has a member that is not DURABLY finished.
// Durably, not merely finished: store.LaneRow.FinishedAt exists because a gate that fires on a fact
// that did not survive a crash is a gate that can open twice.
func (c *Coordinator) regateLocked(t record.TenantID, id record.PipelineID, gen uint64) {
	mine := make([]*assignmentRow, 0, len(c.rows))
	for _, row := range c.rows {
		if row.a.Tenant == t && row.a.Pipeline == id && row.a.Generation == gen {
			mine = append(mine, row)
		}
	}
	for _, row := range mine {
		row.a.Gated = false
		for _, want := range row.a.Lane.After {
			for _, other := range mine {
				if other.a.Lane.Group != want {
					continue
				}
				if !other.a.Lane.Finished || other.a.Lane.FinishedAt.IsZero() {
					row.a.Gated = true
					break
				}
			}
			if row.a.Gated {
				break
			}
		}
		// A GATE THAT CLOSES ON A HELD LANE DOES NOT SNATCH IT BACK. Nothing here clears the lease,
		// and yanking a row from underneath a worker that is mid-batch is the one thing the epoch
		// cannot make safe.
		//
		// SO THE HOLDER LEARNS FROM Assignments AND NOT FROM Renew. Renew deliberately checks only
		// that the row is still this worker's at this epoch, because the two things that genuinely
		// invalidate a holding — the row being withdrawn from the plan, and the generation being
		// superseded — both delete the row and are caught by that check. A gate re-closing inside one
		// generation means the planner reported a finished predecessor as unfinished, which is a
		// planner bug rather than a placement change, and fencing a mid-batch worker over it would
		// turn that bug into lost progress.
	}
}

// Assignments returns the current rows for a pipeline, in id order.
func (c *Coordinator) Assignments(_ context.Context, t record.TenantID, id record.PipelineID) ([]store.Assignment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]store.Assignment, 0, len(c.rows))
	for _, row := range c.rows {
		if row.a.Tenant == t && row.a.Pipeline == id {
			out = append(out, row.a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// --- the placement protocol ----------------------------------------------------------------------

// Claim takes a lane, fencing whoever held it before.
//
// EVERY CLAIM ISSUES A NEW EPOCH, including a claim by the worker that already holds the row. That is
// what makes a fast restart work: the same WorkerID coming back before its old lease expired is a
// NEW process, and the epoch it gets fences any write the old one still has in flight. A worker that
// holds a live lease and wants to keep it calls Renew, which keeps the epoch it was granted.
func (c *Coordinator) Claim(_ context.Context, a store.AssignmentID, w store.WorkerID,
	ttl time.Duration,
) (store.Lease, error) {
	if w == "" {
		return store.Lease{}, fault.Contract(fault.OpPersist,
			fmt.Errorf("memstore: a claim needs a worker id"))
	}
	if ttl <= 0 {
		ttl = store.DefaultLeaseTTL
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.Now()

	row, ok := c.rows[a]
	if !ok {
		// A row that is gone is a row somebody re-planned. To the caller that is the same answer as
		// losing it to another worker — you do not get this lane — which is what Fenced means.
		return store.Lease{}, fenced("assignment %s is not in the plan", a)
	}
	switch {
	case row.a.Gated:
		return store.Lease{}, fenced("assignment %s is gated on %v", a, row.a.Lane.After)
	case row.a.Lane.Finished:
		return store.Lease{}, fenced("assignment %s is finished", a)
	}

	if held, until := c.holderLocked(row, now); held != "" && held != w {
		return store.Lease{}, fenced(
			"assignment %s is held by %s until %s", a, held, until.Format(time.RFC3339Nano))
	}

	c.epoch++
	row.a.Worker, row.a.Epoch = w, c.epoch
	row.a.LeaseExpires = now.Add(ttl)
	row.ttl = ttl
	return leaseOf(row), nil
}

// holderLocked reports who, if anyone, this row may not be taken from, and until when.
//
// THE REASSIGNMENT DELAY IS THE POINT. A lapsed lease does not become free the instant it expires:
// it is reserved for its previous holder for [store.DefaultReassignmentDelay] afterwards, so a
// worker that bounced — a restart, a GC pause, a rolling deploy — comes back to its own lanes instead
// of triggering a cluster-wide reshuffle that every other worker then has to absorb.
//
// The delay costs availability for exactly that window, and that is the trade the design names: a
// lane whose worker is genuinely gone is unread for the TTL plus the delay. Making it shorter makes
// a deploy louder; making it longer makes a real failure slower. It runs under c.mu.
func (c *Coordinator) holderLocked(row *assignmentRow, now time.Time) (store.WorkerID, time.Time) {
	if row.a.Worker == "" {
		return "", time.Time{}
	}
	if now.Before(row.a.LeaseExpires) {
		return row.a.Worker, row.a.LeaseExpires
	}
	// Expired, and the delay runs from the LEASE'S OWN EXPIRY rather than from whenever a claimant
	// happened to ask — a delay restarted by each attempt would be extended by every attempt, so a
	// busy cluster would never reassign anything.
	if until := row.a.LeaseExpires.Add(store.DefaultReassignmentDelay); now.Before(until) {
		return row.a.Worker, until
	}
	return "", time.Time{}
}

// Renew extends a lease this worker still holds, keeping its epoch.
//
// It returns [fault.ErrFenced] when the row has moved on, and store.Coordinator's own doc says what
// the caller must do with that: treat every lane in the lease as revoked. Renewing is deliberately
// not a claim — a renewal that quietly re-took a lane somebody else had would make the epoch mean
// nothing.
func (c *Coordinator) Renew(_ context.Context, l store.Lease) (store.Lease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	row, ok := c.rows[l.Assignment]
	if !ok {
		return store.Lease{}, fenced("assignment %s is not in the plan", l.Assignment)
	}
	if row.a.Worker != l.Worker || row.a.Epoch != l.Epoch {
		return store.Lease{}, fenced("assignment %s is now held by %s at epoch %d, not %s at %d",
			l.Assignment, row.a.Worker, row.a.Epoch, l.Worker, l.Epoch)
	}

	// A LEASE THAT HAS ALREADY LAPSED IS NOT RENEWED. The row still names this worker — the
	// reassignment delay is holding it — but the window in which another worker could legally have
	// taken it has been open, so extending it now would be claiming the lane back without the fence
	// a claim carries. The caller has to Claim, and take a new epoch with it.
	now := c.Now()
	if !now.Before(row.a.LeaseExpires) {
		return store.Lease{}, fenced("the lease on %s expired at %s",
			l.Assignment, row.a.LeaseExpires.Format(time.RFC3339Nano))
	}

	row.a.LeaseExpires = now.Add(row.ttl)
	return leaseOf(row), nil
}

// Release gives up a lease. A lease that has already been fenced releases nothing, and says so.
func (c *Coordinator) Release(_ context.Context, l store.Lease) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	row, ok := c.rows[l.Assignment]
	if !ok {
		return nil
	}
	if row.a.Worker != l.Worker || row.a.Epoch != l.Epoch {
		return fenced("assignment %s is now held by %s at epoch %d, not %s at %d",
			l.Assignment, row.a.Worker, row.a.Epoch, l.Worker, l.Epoch)
	}

	// RELEASED IS FREE IMMEDIATELY, with no reassignment delay. The delay exists for a worker that
	// vanished; one that hands a lane back has said it is not coming for it, and making the cluster
	// wait two minutes on a clean shutdown would make every rolling deploy a two-minute stall.
	row.a.Worker, row.a.Epoch = "", 0
	row.a.LeaseExpires = time.Time{}
	return nil
}

func leaseOf(row *assignmentRow) store.Lease {
	return store.Lease{
		Assignment: row.a.ID, Worker: row.a.Worker,
		Epoch: row.a.Epoch, Expires: row.a.LeaseExpires,
	}
}

// fenced builds the one error a caller branches on: you do not hold this lane.
//
// The MESSAGE is what says which of the several ways that happened it was — taken, expired, gated,
// re-planned — and the CLASS is what a caller matches with errors.Is. Separate reasons would mean a
// caller has to enumerate them to discover it has lost a lane, and one it forgot is a worker that
// keeps reading a lane somebody else now owns.
func fenced(format string, args ...any) error {
	return fault.New(fault.Fenced, fault.OpPersist, fmt.Errorf("memstore: "+format, args...))
}
