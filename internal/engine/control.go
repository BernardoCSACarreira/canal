package engine

import (
	"context"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// THE SOURCE CONTROL GOROUTINE, which build.go has described as part of the contract since it was
// written and which did not exist.
//
// "A SOURCE node runs exactly two: the read goroutine (Open, Read, Close) and the control goroutine
// (Commit, Heartbeat, Backlog, Nack, assignment refresh)." Only the read goroutine was built. Commit
// went to its own pump, correctly, and the other three were resolved by the registry, reported by the
// negotiation as declared, and called from nowhere at all. Seven connectors in this module implement
// Heartbeat, five implement Backlog and three implement Nack.
//
// THE WORST OF IT WAS A GUARD THAT COULD NOT KEEP ITS PROMISE. negotiate.go refuses a build whose
// source prunes its upstream on commit, gates a stream lane behind a scan, and cannot heartbeat —
// with the diagnostic "the upstream would pin its retention for the whole scan". A source that
// DECLARES Heartbeater passed that gate and then pinned its upstream's retention anyway, because
// nothing ever heartbeated. pkg/connector names the consequence outright: with no messages to
// acknowledge, a logical-replication slot is never reclaimed and the disk fills.
//
// The three calls have nothing in common except their cadence and their goroutine:
//
//   - Heartbeat keeps an upstream from pinning its own retention, and it is what makes
//     LaneStatus.Idle mean something. IT NEVER CARRIES A POSITION — it runs concurrently with the
//     read path, so a position arriving here has no defined order against records the read path has
//     already produced, and committing it would advance the cursor past unsettled records. A source
//     advances a cursor without producing a record by returning a zero-record positioned batch.
//   - Backlog answers "how much is left", for the read model. Polled rather than pushed, and
//     round-robined under a per-tick cap because it may be an expensive remote query and a pipeline
//     may hold tens of thousands of lanes.
//   - Nack tells a source that a record reached a terminal disposition, so it can park a message or
//     notify upstream. Event-driven rather than periodic, so it is queued by the write path and
//     drained here.
//
// ASSIGNMENT REFRESH IS THE FOURTH AND IS STILL ABSENT, deliberately: assignments come from
// store.Coordinator, which has no implementation, so in this runtime the set of lanes a worker holds
// cannot change after Open. There is nothing to refresh and nothing is pretending otherwise.

// laneActivity is the control loop's runtime-only bookkeeping.
//
// RUNTIME-ONLY MATTERS. None of it belongs in laneRecord, which is persisted: writing a row on every
// heartbeat would turn a liveness signal into a store write per lane per tick, and idleness is not a
// fact that should survive a restart — a fresh process has not heard from anybody yet.
type laneActivity struct {
	mu sync.Mutex

	// lastRecord is when a lane last produced records. Zero means "not since this run started".
	lastRecord map[record.LaneID]time.Time

	// idleSince is set only when a heartbeat was actually DELIVERED, which is what keeps
	// LaneStatus.Idle meaning "the source has reported this lane quiet" rather than "the core thinks
	// nothing is happening". The distinction is the whole point: a stuck lane is also quiet, and only
	// one of the two is healthy.
	idleSince map[record.LaneID]time.Time

	backlog map[record.LaneID]telemetry.Backlog
	nacks   map[record.LaneID][]connector.Nack

	// eventLag is the source's OWN answer to "how far behind the newest available record am I",
	// which is a better one than differencing Position.At: the source knows what it has not read
	// yet and the core does not.
	eventLag map[record.LaneID]time.Duration

	// next is the round-robin cursor for backlog polling, so every lane is eventually refreshed
	// without any tick paying for all of them.
	next int
}

func newLaneActivity() *laneActivity {
	return &laneActivity{
		lastRecord: map[record.LaneID]time.Time{},
		idleSince:  map[record.LaneID]time.Time{},
		backlog:    map[record.LaneID]telemetry.Backlog{},
		nacks:      map[record.LaneID][]connector.Nack{},
		eventLag:   map[record.LaneID]time.Duration{},
	}
}

// saw records that a lane produced records, which ends any idleness.
func (a *laneActivity) saw(lane record.LaneID, at time.Time) {
	a.mu.Lock()
	a.lastRecord[lane] = at
	delete(a.idleSince, lane)
	a.mu.Unlock()
}

// quietFor reports how long a lane has produced nothing, measured from since when it has never
// produced at all.
func (a *laneActivity) quietFor(lane record.LaneID, now, since time.Time) time.Duration {
	a.mu.Lock()
	last, ok := a.lastRecord[lane]
	a.mu.Unlock()
	if !ok {
		last = since
	}
	return now.Sub(last)
}

// reportedIdle records that a heartbeat was delivered for a lane that had been quiet since when.
func (a *laneActivity) reportedIdle(lane record.LaneID, since time.Time) {
	a.mu.Lock()
	if _, already := a.idleSince[lane]; !already {
		a.idleSince[lane] = since
	}
	a.mu.Unlock()
}

func (a *laneActivity) setBacklog(lane record.LaneID, b telemetry.Backlog) {
	a.mu.Lock()
	a.backlog[lane] = b
	a.mu.Unlock()
}

func (a *laneActivity) setEventTimeLag(lane record.LaneID, d time.Duration) {
	a.mu.Lock()
	a.eventLag[lane] = d
	a.mu.Unlock()
}

// queueNack holds a terminal failure for the next drain.
//
// BOUNDED, because the queue is fed by the write path and drained by a call that can fail. An
// unbounded one is the defect this module already had in baseRuntime.events: third-party code with no
// rate limit on one side and an append on the other. Dropping the oldest is the right direction — a
// nack is a courtesy to the upstream and the record's own accounting is already correct — and it is
// reported rather than silent.
func (a *laneActivity) queueNack(lane record.LaneID, n connector.Nack) (dropped bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	q := append(a.nacks[lane], n)
	if len(q) > maxQueuedNacks {
		q = q[len(q)-maxQueuedNacks:]
		dropped = true
	}
	a.nacks[lane] = q
	return dropped
}

const maxQueuedNacks = 1024

// takeNacks removes and returns everything queued.
func (a *laneActivity) takeNacks() map[record.LaneID][]connector.Nack {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.nacks) == 0 {
		return nil
	}
	out := a.nacks
	a.nacks = map[record.LaneID][]connector.Nack{}
	return out
}

// returnNacks puts undelivered nacks back for the next tick.
func (a *laneActivity) returnNacks(lane record.LaneID, ns []connector.Nack) {
	a.mu.Lock()
	a.nacks[lane] = append(ns, a.nacks[lane]...)
	a.mu.Unlock()
}

// observed is what the read model reads back for one lane.
func (a *laneActivity) observed(lane record.LaneID) (idleSince time.Time, bl *telemetry.Backlog, lag *float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if b, ok := a.backlog[lane]; ok {
		copied := b
		bl = &copied
	}
	if d, ok := a.eventLag[lane]; ok {
		secs := d.Seconds()
		lag = &secs
	}
	return a.idleSince[lane], bl, lag
}

// nextLanes returns up to n lanes starting from the round-robin cursor.
func (a *laneActivity) nextLanes(all []record.LaneID, n int) []record.LaneID {
	if len(all) == 0 {
		return nil
	}
	if n >= len(all) {
		return all
	}
	a.mu.Lock()
	start := a.next % len(all)
	a.next = (start + n) % len(all)
	a.mu.Unlock()

	out := make([]record.LaneID, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, all[(start+i)%len(all)])
	}
	return out
}

// maxBacklogPollsPerTick bounds one tick's work. A source with 900 discovered streams at 32-way
// chunking holds tens of thousands of lanes, and Backlog may be a remote query per lane; polling all
// of them on a timer would be a self-inflicted load test. Backlog.AsOf is what makes the resulting
// staleness visible rather than a lie, which is why the type carries it.
const maxBacklogPollsPerTick = 32

// controlLoop is one source's control goroutine.
func (r *runner) controlLoop(ctx context.Context, id record.NodeID, stop <-chan struct{}) {
	src := r.p.sources[id]
	if src == nil || (src.Heartbeat == nil && src.Backlog == nil && src.Nackable == nil) {
		// Nothing declared, so there is no goroutine to run. The registry leaves each handle nil
		// unless the capability was DECLARED, so this is the declaration governing and not a type
		// assertion behind the operator's back.
		return
	}
	t := time.NewTicker(r.deps.ControlInterval)
	defer t.Stop()

	for {
		select {
		case <-stop:
			// A LAST DRAIN ON THE WAY OUT. A record abandoned in the final moments of a run has
			// already been settled and its cursor already advanced past it; if the upstream is never
			// told, nothing will ever tell it. The context is detached because shutdown is precisely
			// when the run context is cancelled.
			r.drainNacks(context.WithoutCancel(ctx), id, src)
			return
		case <-t.C:
			r.heartbeatQuietLanes(ctx, id, src)
			r.pollBacklogs(ctx, id, src)
			r.drainNacks(ctx, id, src)
		}
	}
}

// liveLanes returns the lanes a node holds that are not finished.
//
// A finished lane is excluded from all three calls: its cursor is final, it will never produce again,
// and heartbeating it would ask the source to hold a slot for something that is done.
func (r *runner) liveLanes(id record.NodeID) []record.LaneID {
	lc := r.lanes[id]
	if lc == nil {
		return nil
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	out := make([]record.LaneID, 0, len(lc.order))
	for _, lane := range lc.order {
		if rec, ok := lc.lanes[lane]; ok && !rec.Finished {
			out = append(out, lane)
		}
	}
	return out
}

// heartbeatQuietLanes tells the source about every lane that has produced nothing for an interval.
func (r *runner) heartbeatQuietLanes(ctx context.Context, id record.NodeID, src *registry.ResolvedSource) {
	if src.Heartbeat == nil {
		return
	}
	now := time.Now()
	for _, lane := range r.liveLanes(id) {
		quiet := r.activity.quietFor(lane, now, r.started)
		if quiet < r.deps.ControlInterval {
			continue
		}
		type hb struct {
			lane record.LaneID
			idle time.Duration
		}
		_, err := sandbox(ctx, r.p.obs, id, src.Name, hb{lane, quiet},
			func(c context.Context, h hb) (struct{}, error) {
				return struct{}{}, src.Heartbeat.Heartbeat(c, h.lane, h.idle)
			})
		if err != nil {
			// NOT FATAL, and that is a decision. A heartbeat is auxiliary: failing it delays an
			// upstream's retention release, which is a real cost and a visible one, while stopping a
			// healthy pipeline over a transient auxiliary call is a larger one. The submit-time gate
			// in negotiate.go is where the un-survivable version of this is refused.
			r.p.obs.fault(id, err)
			r.deps.Log.Warn("a source refused a heartbeat; its upstream may pin its own retention",
				"node", id, "lane", lane, "idle", quiet, "error", err)
			continue
		}
		// Only now. Idle means the source WAS TOLD, so a heartbeat that failed leaves the lane
		// reporting a climbing checkpoint age — which is the honest reading, because nobody has
		// confirmed it is quiet rather than stuck.
		r.activity.reportedIdle(lane, now.Add(-quiet))
	}
}

// pollBacklogs refreshes up to one tick's worth of lanes.
func (r *runner) pollBacklogs(ctx context.Context, id record.NodeID, src *registry.ResolvedSource) {
	if src.Backlog == nil {
		return
	}
	for _, lane := range r.activity.nextLanes(r.liveLanes(id), maxBacklogPollsPerTick) {
		got, err := sandbox(ctx, r.p.obs, id, src.Name, lane,
			func(c context.Context, l record.LaneID) (connector.Backlog, error) {
				return src.Backlog.Backlog(c, l)
			})
		if err != nil {
			// The previous answer is kept rather than being replaced with nothing: it carries its own
			// AsOf, so a stale number is visibly stale, whereas discarding it turns a temporary query
			// failure into "unknown backlog" and loses the last thing anybody knew.
			r.p.obs.fault(id, err)
			r.deps.Log.Warn("a source could not report its backlog", "node", id, "lane", lane, "error", err)
			continue
		}
		b := telemetry.Backlog{Records: got.Records, Bytes: got.Bytes, Exact: got.Exact, AsOf: got.AsOf}
		if b.AsOf.IsZero() {
			// A polled backlog with no AsOf implies a liveness it does not have, so the host stamps
			// the moment it asked rather than letting the field read as the zero time.
			b.AsOf = time.Now()
		}
		r.activity.setBacklog(lane, b)
		if got.EventTimeLag != nil {
			r.activity.setEventTimeLag(lane, *got.EventTimeLag)
		}
	}
}

// drainNacks delivers every queued terminal failure.
func (r *runner) drainNacks(ctx context.Context, id record.NodeID, src *registry.ResolvedSource) {
	if src.Nackable == nil {
		return
	}
	for lane, ns := range r.activity.takeNacks() {
		if len(ns) == 0 {
			continue
		}
		type call struct {
			lane record.LaneID
			ns   []connector.Nack
		}
		_, err := sandbox(ctx, r.p.obs, id, src.Name, call{lane, ns},
			func(c context.Context, k call) (struct{}, error) {
				return struct{}{}, src.Nackable.Nack(c, k.lane, k.ns)
			})
		if err != nil {
			// Requeued for the next tick, under the queue's cap. The alternative — dropping on the
			// first failure — means a source that must park a poison message never parks it and the
			// broker redelivers it forever.
			r.activity.returnNacks(lane, ns)
			r.p.obs.fault(id, err)
			r.deps.Log.Warn("a source refused a nack; the upstream was not told these records failed",
				"node", id, "lane", lane, "records", len(ns), "error", err)
		}
	}
}

// nack queues a terminal failure for the source that owns this record's lane.
//
// The Nack is keyed on the SOURCE'S OWN handle or position and never on a record.RecordID: an id is
// assigned by the core after Read returned, so the source has never seen it and cannot map it to a
// delivery. Which of the two fields carries the answer is a property of the lane — a handle for a
// discrete lane, a position for a prefix one — and pkg/connector says so in exactly those terms.
func (r *runner) nack(rec *record.Record, at record.Position, ferr error, reason string, attempts int) {
	if rec == nil || r.activity == nil {
		return
	}
	lane := rec.Origin().Lane
	src, node := r.sourceOfLane(lane)
	if src == nil || src.Nackable == nil {
		return
	}
	n := connector.Nack{
		Class:    fault.ClassOf(ferr),
		Reason:   reason,
		Attempts: attempts,
	}
	if r.laneOrdering(node, lane) == connector.OrderingDiscrete {
		// Record.Handle, set by the source through SetHandle before it returned from Read, and NOT
		// Origin.Upstream: Upstream is the vendor's record id for the first idempotency layer, while
		// the handle is the delivery — a queue receipt, a delivery tag, an ack id. It is the same
		// thing Ack.Handles carries for the success path, which is what makes a nack and an ack
		// speak the same vocabulary to a queue source.
		n.Handle = rec.Handle()
	} else {
		// A prefix lane resumes from a position, and the finest one the core has for a record is the
		// position of the batch it arrived in. Handing over the batch's is honest about the
		// granularity; inventing a per-record position would not be.
		n.Position = at
	}
	if r.activity.queueNack(lane, n) {
		r.deps.Log.Warn("the nack queue for a lane overflowed; the oldest terminal failures were dropped",
			"node", node, "lane", lane, "cap", maxQueuedNacks)
	}
}

// sourceOfLane finds the source node that owns a lane.
func (r *runner) sourceOfLane(lane record.LaneID) (*registry.ResolvedSource, record.NodeID) {
	for node, lc := range r.lanes {
		lc.mu.Lock()
		_, ok := lc.lanes[lane]
		lc.mu.Unlock()
		if ok {
			return r.p.sources[node], node
		}
	}
	return nil, ""
}

// laneOrdering reports a lane's declared ordering.
func (r *runner) laneOrdering(node record.NodeID, lane record.LaneID) connector.Ordering {
	lc := r.lanes[node]
	if lc == nil {
		return connector.OrderingPrefix
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if rec, ok := lc.lanes[lane]; ok {
		return rec.Spec.Ordering
	}
	return connector.OrderingPrefix
}
