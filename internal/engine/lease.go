package engine

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// LEASES, AND WHAT THEY ARE ALLOWED TO CHANGE.
//
// Deps.Coordinator has been declared and unread since Deps was written, and runtime.go said so in as
// many words: "Lane assignment is local: this process announces a lane, holds it, and nothing can
// take it away, so the lease epoch is a constant." laneCtl.Revoked returned false TRUTHFULLY,
// because in one process nothing could revoke anything.
//
// This file is the other case. A worker with a store.Coordinator joins the worker set, campaigns for
// the planner role, plans the lanes its sources announced, and CLAIMS each one before reading it. The
// epoch on the resulting lease replaces singleWorkerEpoch on that lane's durable writes, which is
// what makes the state store refuse a write from a worker whose lease has moved on.
//
// THE ONE RULE THAT MATTERS. A worker that has lost a lane must not advance that lane's upstream.
// The three-phase commit exists so an upstream is told only about records that are durable HERE, and
// telling it about a lane somebody else now owns discards data the new holder has not delivered. So
// commitPump asks before it acknowledges, and the check is on the path rather than in a comment.
//
// A WORKER WITH NO COORDINATOR IS UNCHANGED. Every function here returns the single-worker answer
// when deps.Coordinator is nil: lanes are held from Announce, the epoch is singleWorkerEpoch, and
// nothing is ever revoked. That is not a fallback — it is the standalone deployment the architecture
// describes, and it stays exactly what it was.

// leaseTable is this worker's claims, keyed by lane.
//
// It is separate from laneCtl because a lane's DURABLE ROW and a lane's PLACEMENT have different
// owners and different lifetimes: the row is this pipeline's state and outlives every worker, while
// the claim is this process's and dies with it. Folding them together would make Announce
// responsible for coordination, which is how a single-worker path acquires a dependency on a
// coordinator it does not have.
type leaseTable struct {
	mu   sync.Mutex
	held map[record.LaneID]store.Lease

	coord store.Coordinator
	gen   uint64

	tenant   record.TenantID
	pipeline record.PipelineID
	worker   store.WorkerID
}

func newLeaseTable(d Deps, s specRefFields, gen uint64) *leaseTable {
	return &leaseTable{
		held:  map[record.LaneID]store.Lease{},
		coord: d.Coordinator, gen: gen,
		tenant: s.tenant, pipeline: s.pipeline, worker: d.Worker,
	}
}

// coordinated reports whether there is anything to coordinate with.
func (lt *leaseTable) coordinated() bool { return lt != nil && lt.coord != nil }

// epochFor returns the fencing token for a lane's durable writes.
//
// SINGLE-WORKER RETURNS THE CONSTANT, which is the whole reason singleWorkerEpoch is 1 rather than 0:
// a coordinated deployment hands out increasing epochs from 1, so a store written by a standalone run
// is not full of rows fenced at a number a coordinator would then have to reason about.
func (lt *leaseTable) epochFor(id record.LaneID) uint64 {
	if !lt.coordinated() {
		return singleWorkerEpoch
	}
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if l, ok := lt.held[id]; ok {
		return l.Epoch
	}
	// A LANE THIS WORKER DOES NOT HOLD REPORTS ZERO, which store.Assignment uses for "unclaimed".
	//
	// This is a REPORTING value and nothing else: Assigned and Table are the only callers, and no
	// durable write is fenced with it — runtime.go still batches at singleWorkerEpoch and says so.
	// An earlier version of this comment reasoned about how a zero would fare against the state
	// store's epoch check, which described a path that does not exist.
	return 0
}

// holds reports whether this worker currently holds a live lease on a lane.
func (lt *leaseTable) holds(id record.LaneID, now time.Time) bool {
	if !lt.coordinated() {
		return true
	}
	lt.mu.Lock()
	defer lt.mu.Unlock()
	l, ok := lt.held[id]
	return ok && l.Valid(now)
}

// soonestExpiry is when this worker first stops being able to write, or nil.
//
// SOONEST RATHER THAN LATEST, because the field answers "when does this worker's authority start
// lapsing" and the first lease to go is the first thing that stops. Nil when there is no coordinator,
// which is the document's rule: an unknown is a nil pointer and never a zero.
func (lt *leaseTable) soonestExpiry() *time.Time {
	if !lt.coordinated() {
		return nil
	}
	lt.mu.Lock()
	defer lt.mu.Unlock()
	var soonest time.Time
	for _, l := range lt.held {
		if soonest.IsZero() || l.Expires.Before(soonest) {
			soonest = l.Expires
		}
	}
	if soonest.IsZero() {
		return nil
	}
	return &soonest
}

// claim takes the lanes this worker does not already hold.
//
// Called from the lane table the moment lanes appear as well as from the renew loop, because a source
// that announces a lane inside Open and then calls Assigned — which is what every source in this
// module does — would otherwise be told it has nothing to read until the next tick.
func (lt *leaseTable) claim(ctx context.Context, log *slog.Logger, rows []store.LaneRow) {
	if !lt.coordinated() {
		return
	}
	now := time.Now()
	for _, row := range rows {
		lt.mu.Lock()
		l, ok := lt.held[row.ID]
		lt.mu.Unlock()
		if ok && l.Valid(now) {
			continue
		}

		got, err := lt.coord.Claim(ctx, store.AssignmentIDFor(lt.tenant, lt.pipeline, lt.gen, row.ID),
			lt.worker, store.DefaultLeaseTTL)
		if err != nil {
			// NOT AN ERROR THIS WORKER ACTS ON. A lane it cannot claim is one somebody else is
			// reading, or one whose gate has not opened; either way the answer is not to read it, and
			// Assigned already will not offer it. At debug because in a cluster this is the normal
			// outcome for most lanes on most workers.
			log.Debug("could not claim a lane", "lane", row.ID, "error", err)
			continue
		}
		lt.mu.Lock()
		lt.held[row.ID] = got
		lt.mu.Unlock()
	}
}

// renew extends every held lease and returns the lanes that were lost.
//
// A FAILED RENEW REVOKES ONE LANE, NOT ONE WORKER. store.Coordinator says a worker whose Renew fails
// treats every lane in that lease as revoked — and the lease is per lane here, so the blast radius is
// one lane. That is why the epoch fences per lane rather than per process.
func (lt *leaseTable) renew(ctx context.Context, log *slog.Logger) []record.LaneID {
	if !lt.coordinated() {
		return nil
	}
	lt.mu.Lock()
	current := make(map[record.LaneID]store.Lease, len(lt.held))
	for lane, l := range lt.held {
		current[lane] = l
	}
	lt.mu.Unlock()

	var lost []record.LaneID
	for lane, l := range current {
		got, err := lt.coord.Renew(ctx, l)
		if err == nil {
			lt.mu.Lock()
			lt.held[lane] = got
			lt.mu.Unlock()
			continue
		}
		if ctx.Err() != nil {
			// SHUTTING DOWN IS NOT A FENCING EVENT. A renewal that failed because the process is
			// stopping has lost nothing, and reporting it as revoked would make every clean shutdown
			// emit a lane-revoked event and a re-delivery warning for every lane it held.
			return lost
		}
		log.Warn("lost a lane lease; this worker stops reading it and will not advance its upstream",
			"lane", lane, "epoch", l.Epoch, "error", err)
		lt.mu.Lock()
		delete(lt.held, lane)
		lt.mu.Unlock()
		lost = append(lost, lane)
	}
	return lost
}

// releaseAll hands every lease back, so a clean shutdown does not make the cluster wait out the
// reassignment delay for lanes nobody is reading.
func (lt *leaseTable) releaseAll(ctx context.Context, log *slog.Logger) {
	if !lt.coordinated() {
		return
	}
	lt.mu.Lock()
	current := make([]store.Lease, 0, len(lt.held))
	for _, l := range lt.held {
		current = append(current, l)
	}
	clear(lt.held)
	lt.mu.Unlock()

	for _, l := range current {
		// A FENCED RELEASE IS NOT WORTH REPORTING: it means the lane had already moved on, which is
		// the state releasing was trying to reach.
		if err := lt.coord.Release(ctx, l); err != nil && !errors.Is(err, fault.ErrFenced) {
			log.Warn("releasing a lease", "lane", l.Assignment, "error", err)
		}
	}
}

// --- the runner's side ---------------------------------------------------------------------------

// joinCluster registers this worker and campaigns for the planner role.
//
// NEITHER FAILURE STOPS THE PIPELINE. Membership is a directory and leadership is advisory, so a
// coordinator that cannot answer either question costs this worker a row in a status document and
// the right to plan — not the right to read lanes it can still claim.
func (r *runner) joinCluster(ctx context.Context) {
	if !r.leases.coordinated() {
		return
	}
	m, err := r.leases.coord.Join(ctx, store.WorkerInfo{
		ID: r.deps.Worker, Tenant: r.p.spec.Tenant, Version: r.deps.Version, Started: r.started,
	})
	if err != nil {
		r.deps.Log.Warn("could not join the worker set; this worker still reads what it can claim",
			"worker", r.deps.Worker, "error", err)
	}

	l, cerr := r.leases.coord.Campaign(ctx)
	if cerr != nil {
		r.deps.Log.Warn("could not campaign for the planner role", "error", cerr)
	}

	// PUBLISHED UNDER THE LOCK, because Status reads them and Status runs on whatever goroutine asked.
	// This is the same race newRunner exists to prevent: the runner is published before run() starts,
	// so anything run() assigns afterwards is assigned while a concurrent Status can be reading it.
	// Join and Campaign do I/O, so they cannot move into newRunner — the lock is the answer instead.
	r.mu.Lock()
	r.membership, r.leadership = m, l
	r.mu.Unlock()
}

// cluster returns this worker's membership and planner claim, or nils.
func (r *runner) cluster() (store.Membership, store.Leadership) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.membership, r.leadership
}

// isLeader reports what this process believes about the planner role, which is advisory by
// construction and true for a standalone run that is the only planner there is.
func (r *runner) isLeader() bool {
	_, l := r.cluster()
	return l == nil || l.IsLeader()
}

// leaveCluster releases every lease and withdraws, in that order.
//
// LEASES FIRST. Withdrawing from membership does not release anything — store.Coordinator's
// membership and its leases are deliberately separate fences — so leaving first would just mean the
// lanes sat out the reassignment delay while this process was already gone.
func (r *runner) leaveCluster(ctx context.Context) {
	if !r.leases.coordinated() {
		return
	}
	r.leases.releaseAll(ctx, r.deps.Log)
	m, l := r.cluster()
	if l != nil {
		if err := l.Resign(ctx); err != nil {
			r.deps.Log.Warn("resigning the planner role", "error", err)
		}
	}
	if m != nil {
		if err := m.Leave(ctx); err != nil {
			r.deps.Log.Warn("leaving the worker set", "error", err)
		}
	}
}

// leaseLoop renews this worker's leases and re-claims what it can, until the run stops.
//
// It takes ctx rather than the read context, so leases keep being renewed while the pipeline DRAINS.
// A worker that stopped renewing the moment it stopped reading would have its lanes reassigned
// underneath a drain that is still settling records for them.
func (r *runner) leaseLoop(ctx context.Context) {
	if !r.leases.coordinated() {
		return
	}
	t := time.NewTicker(store.DefaultRenewInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, lane := range r.leases.renew(ctx, r.deps.Log) {
				r.laneRevoked(lane)
			}
			r.replanAndClaim(ctx)
		}
	}
}

// replanAndClaim re-states the plan and takes whatever this worker can now hold.
//
// PLANNING IS THE LEADER'S JOB AND CLAIMING IS EVERY WORKER'S. The leader check is advisory — see
// store.Leadership — so being wrong about it costs nothing: a plan written by a former leader loses
// the store's compare-and-set, and the claims below are fenced by the epoch either way.
func (r *runner) replanAndClaim(ctx context.Context) {
	for node, lc := range r.laneCtls() {
		r.planAndClaim(ctx, node, lc)
	}
}

// planAndClaim states one node's lanes and takes what it can.
func (r *runner) planAndClaim(ctx context.Context, node record.NodeID, lc *laneCtl) {
	if !r.leases.coordinated() {
		return
	}
	rows := lc.laneRows()
	if r.isLeader() {
		if err := r.leases.coord.Plan(ctx, r.p.spec.Tenant, r.p.spec.ID, r.leases.gen, rows); err != nil {
			if ctx.Err() == nil {
				r.deps.Log.Warn("planning lanes", "node", node, "error", err)
			}
			// NOT CLAIMING AFTER A FAILED PLAN. The rows this worker would claim are the ones it just
			// failed to write, so every claim would be for an assignment that is not in the plan — a
			// burst of fenced errors that says nothing the plan failure did not already say.
			return
		}
	}
	r.leases.claim(ctx, r.deps.Log, rows)
}

// laneRevoked records the cost of losing a lane.
//
// THE COST IS NAMED AND COUNTED RATHER THAN HIDDEN. Records this worker had in flight are settled for
// accounting only; the new holder re-reads from the last DURABLE cursor, so up to one in-flight
// window is re-delivered. That is disclosed rather than fixed, because letting a fenced worker
// advance an upstream to make the overlap go away is specified data loss — and an idempotent sink
// absorbs the overlap, while nothing recovers the loss.
func (r *runner) laneRevoked(lane record.LaneID) {
	st := r.p.ledger.Stats(lane)
	r.p.obs.revokedUnsettled(lane, st.InFlight)
	r.noteOnSource(lane, connector.Event{
		Kind:     connector.EventLaneRevoked,
		Severity: fault.TransientInternal,
		Message: "the lease on this lane was lost; this worker has stopped reading it and its " +
			"in-flight records will be re-delivered by the new holder",
	})
}

// noteOnSource records an engine-raised event against the source that owns a lane, so it reaches the
// read model's RecentEvents the same way a connector's own does.
func (r *runner) noteOnSource(lane record.LaneID, e connector.Event) {
	_, node, ok := r.sourceFor(lane)
	if !ok {
		return
	}
	if rt := r.sourceRT(node); rt != nil {
		rt.Note(e)
	}
}

// commitAllowed reports whether a lane's upstream may be advanced.
//
// THIS IS THE FENCE WITH TEETH, AND IT IS NOT YET DEMONSTRATED. Every other check in this file
// decides what to READ; this one decides what to tell an upstream it may forget. A worker that lost
// a lane and acknowledged it anyway discards records the new holder has not delivered, and no epoch
// undoes that, because by then the data is gone from the source.
//
// HONESTLY LABELLED: deleting this check leaves every test in lease_test.go passing, including the
// one written specifically to isolate it by holding records in flight across a revocation. The read
// fence gets there first in every scenario constructed so far — Assigned stops offering the lane,
// the source stops producing, and the ack that would have been refused is never generated. So this
// is defence in depth that has not been shown to be reachable, which is a different thing from
// defence that has been shown to work, and it is worth more as a labelled unknown than as an
// assumed guarantee. Reaching it needs a fixture that can settle records for a lane AFTER the read
// loop has stopped for it.
func (r *runner) commitAllowed(lane record.LaneID) bool {
	return r.leases.holds(lane, time.Now())
}

// revokedUnsettled records how many records were in flight when a lane's lease lapsed.
//
// A GAUGE RATHER THAN A COUNTER, matching the name: canal_lane_revoked_unsettled_records has no
// _total suffix because the question is "how big was the overlap this lane's new holder has to
// absorb", which is a magnitude per event and not a running sum.
func (o *obs) revokedUnsettled(lane record.LaneID, n uint64) {
	if o == nil {
		return
	}
	o.revoked.Set(float64(n), o.pipeline, laneLabel(lane))
}
