package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/ledger"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// SCOPE (design rule R10, labelled rather than implied).
//
// This is the single-worker, byte-path engine. It runs the shape the package doc describes — read,
// admit, write, settle, flush, commit — for a graph of sources feeding sinks, in one process.
//
// What it does NOT do yet, and where each would go:
//
//   - TRANSFORMS AND BUFFERS. No component of either kind is registered anywhere in the module, so
//     there is no node to run. The ledger already models the fan-out they need through Expand.
//   - MULTI-WORKER COORDINATION. See the note in runtime.go.
//   - REOPENING A COMPONENT. fault.NotConnected asks for Open to be called again before any further
//     call. It is routed as an uncounted retry, which is the right WAIT but not the right ACTION:
//     nothing reopens, so a component that needs it retries until MaxElapsed. Reopening a source
//     mid-run has to reconcile its lane state, which is why it is not a two-line change.
//
// None is a hidden gap: the first two are component kinds with no instances and the negotiation
// refuses a pipeline that asks for one; the third is labelled here and in route.
//
// Codecs used to be on this list, and so did fault routing. Codecs are resolved in codec.go; faults
// are routed by retry.go and acted on in write.go.

// Run executes the pipeline until ctx is cancelled, the input ends, or a terminal fault occurs.
func (p *Pipeline) Run(ctx context.Context) error {
	if len(p.sources) == 0 || len(p.sinks) == 0 {
		return fault.Contract(fault.OpOpen,
			fmt.Errorf("engine: a pipeline needs at least one source and one sink"))
	}
	// Refused BEFORE anything is opened, so a pipeline whose contract this build cannot keep never
	// touches a source, a sink or a byte of state. Build has already warned about it.
	if err := Executable(p.negotiated); err != nil {
		return fault.Contract(fault.OpOpen, err)
	}
	r := newRunner(p)
	// Published BEFORE run so that a status read during a slow open sees "starting" rather than
	// "pending" — the two are different answers, and opening a source against an unreachable upstream
	// is exactly when somebody is watching.
	//
	// WHICH IS WHY newRunner EXISTS. Publishing a runner whose fields the run goroutine was still
	// assigning was a data race against every concurrent Status: the topology maps, the lane
	// activity and the start time were all written after the store. A published runner is fully
	// formed; what happens afterwards is only ever INSERTION into maps that have their own lock.
	p.active.Store(r)
	return r.run(ctx)
}

// newRunner builds a runner that is safe to read from another goroutine the instant it exists.
func newRunner(p *Pipeline) *runner {
	now := time.Now()
	r := &runner{
		p:    p,
		deps: p.deps,

		lanes:  map[record.NodeID]*laneCtl{},
		srcRT:  map[record.NodeID]*sourceRuntime{},
		sinkRT: map[record.NodeID]*sinkRuntime{},

		lastCheckpoint: map[record.LaneID]time.Time{},
		started:        now,
		lastPersist:    now,

		activity:     newLaneActivity(),
		checkpointer: newCheckpointer(),
		awaiting:     newAwaitingCommit(),
	}
	r.leases = newLeaseTable(p.deps, specRefFields{tenant: p.spec.Tenant, pipeline: p.spec.ID},
		p.spec.Revision)
	r.status.phase = telemetry.PhaseStarting
	r.mainSinks, r.failedSinks = partitionSinks(p.spec, p.sinks)

	// THE TRIGGER MUST FIRE BEFORE ADMISSION BLOCKS, which is why it is half the budget and not the
	// whole of it.
	//
	// With a Flusher every un-durable record counts against the lane budget, so a trigger equal to
	// the budget is unreachable: the read loop fills the budget, Admit blocks, and the flush that
	// would release it never happens because the batch that would have triggered it is stuck in
	// admission. Measured at 3,450 records/s — the budget divided by the tick interval — where half
	// the budget gives 92,000/s on the same input.
	//
	// Half leaves room for at least one more read batch, which is all the margin the invariant
	// needs. FlushRecords caps it lower when an operator wants a tighter durability window.
	trigger := p.deps.FlushRecords
	if half := p.spec.LaneBudget / 2; half > 0 && half < trigger {
		trigger = half
	}
	if trigger <= 0 {
		trigger = 1
	}
	r.deferred = newDeferred(trigger)
	return r
}

// runner holds one execution of a pipeline.
type runner struct {
	p    *Pipeline
	deps Deps

	// lanes and srcRT are kept so shutdown can reach the lane table for the final commit, and so the
	// commit pump can find the source that owns an acknowledged lane. sinkRT is kept so the read model
	// can collect the events a sink reported through connector.Runtime.Note.
	//
	// nodesMu GUARDS ONLY THE MAPS, never the I/O that produces what goes in them. A component is
	// constructed and opened outside the lock and inserted under it, so a status read during a slow
	// open is not blocked by that open — it simply sees the nodes that are up so far, with the rest
	// reporting Connected false, which is the honest answer and a more useful one than waiting.
	nodesMu sync.RWMutex
	lanes   map[record.NodeID]*laneCtl
	srcRT   map[record.NodeID]*sourceRuntime
	sinkRT  map[record.NodeID]*sinkRuntime

	// leases is this worker's claims, membership its row in the worker set and leadership its
	// advisory planner role. All three are inert for a worker with no store.Coordinator, which is the
	// standalone deployment. See lease.go.
	leases     *leaseTable
	membership store.Membership
	leadership store.Leadership

	// status is the read model's own state: the phase, the previous condition set and the rate
	// samples. See status.go. activity is the control loop's, see control.go.
	status   statusState
	activity *laneActivity

	// mainSinks and failedSinks are the sink nodes split by what their inbound edges carry, computed
	// once at start because it is a property of the spec and cannot change during a run.
	mainSinks   []record.NodeID
	failedSinks []record.NodeID

	// deferred holds records accepted by a sink that has not yet made them durable.
	deferred *deferred

	// checkpointer owns the monotonic checkpoint id and the pending committable set; awaiting holds
	// the records those committables cover until a commit answers for them; checkpointVersion is the
	// store's compare-and-set version for the checkpoint key.
	checkpointer      *checkpointer
	awaiting          *awaitingCommit
	checkpointVersion uint64

	mu       sync.Mutex
	firstErr error

	// firstErrAt is when the fault that ended the run was recorded. The read model needs it and
	// fault.Fault carries no timestamp of its own — deliberately, since a fault crosses a wire and a
	// clock does not travel well, so the moment it was OBSERVED is the host's to record.
	firstErrAt time.Time

	// lastCheckpoint is when each lane's durable cursor last advanced, and started is the fallback
	// for a lane that has not advanced at all. Together they are what makes canal_checkpoint_age
	// seconds ALWAYS available: a pipeline that has never checkpointed reports its age since start
	// and climbing, rather than reporting nothing and looking healthy to an absent() alert.
	lastCheckpoint map[record.LaneID]time.Time
	started        time.Time
	lastPersist    time.Time
}

// markCheckpointed records that these lanes' cursors are now durable.
func (r *runner) markCheckpointed(lanes map[record.LaneID]record.Position) {
	now := time.Now()
	r.mu.Lock()
	for id := range lanes {
		r.lastCheckpoint[id] = now
	}
	r.lastPersist = now
	r.mu.Unlock()
}

// refreshGauges republishes every point-in-time series.
//
// It runs on the flush ticker rather than on events, because an age is only useful if it climbs
// WITHOUT anything happening. A checkpoint age updated only when a checkpoint is written is frozen
// at its last good value exactly when the pipeline stalls, which is the one moment it matters.
//
// EVERY SERIES HERE IS PROJECTED FROM THE SAME PASS THE STATUS DOCUMENT IS BUILT FROM. The scrape
// and the document are two serialisations of one observation, so they cannot disagree about the
// state of the pipeline at a given moment — which is the whole reason [runner.laneStatuses] returns
// its facts rather than each consumer walking the lanes itself.
func (r *runner) refreshGauges() {
	o := r.p.obs
	if o == nil {
		return
	}
	now := time.Now()

	r.mu.Lock()
	persist := r.lastPersist
	r.mu.Unlock()
	o.staleness.Set(now.Sub(persist).Seconds(), o.pipeline)

	lanes, facts := r.laneStatuses(now)
	for i := range lanes {
		ls := &lanes[i]
		lbl := laneLabel(ls.ID)

		// A FINISHED LANE IS NOT A STALLED ONE. Its cursor is final and durable, so an age that keeps
		// climbing would page somebody about a bounded pipeline that completed exactly as intended.
		// Forgetting the series is what omit-don't-zero means for a quantity that has stopped
		// existing rather than one that was never measured.
		if ls.Finished {
			o.checkpointAge.Forget(o.pipeline, lbl)
			o.inFlight.Forget(o.pipeline, lbl)
			o.inFlightMax.Forget(o.pipeline, lbl)
			o.replay.Forget(o.pipeline, lbl)
			o.oldestPending.Forget(o.pipeline, lbl)
			continue
		}
		if ls.CheckpointAge != nil {
			o.checkpointAge.Set(*ls.CheckpointAge, o.pipeline, lbl)
		}
		o.inFlight.Set(float64(ls.InFlight), o.pipeline, lbl)
		o.inFlightMax.Set(float64(ls.InFlightBudget), o.pipeline, lbl)
		o.replay.Set(float64(ls.ReplayRecords), o.pipeline, lbl)

		// OMIT, DO NOT ZERO. A lane with nothing pending has no oldest pending record, and an age of
		// zero would read as "the oldest one arrived just now" — the opposite of the truth, and the
		// reading that makes a `> 60` alert quietly stop firing.
		if ls.OldestPendingAge != nil {
			o.oldestPending.Set(*ls.OldestPendingAge, o.pipeline, lbl)
		} else {
			o.oldestPending.Forget(o.pipeline, lbl)
		}
	}

	o.reconcile.Set(float64(int64(facts.admitted)-int64(facts.settled)-int64(facts.abandoned)), o.pipeline)
	r.publishConditions(r.conditions(now, facts, r.p.configView()))
}

func (r *runner) fail(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	if r.firstErr == nil {
		r.firstErr, r.firstErrAt = err, time.Now()
	}
	r.mu.Unlock()
}

// setPhase moves the pipeline's coarse state, which is the read model's headline.
func (r *runner) setPhase(p telemetry.Phase) {
	r.status.mu.Lock()
	r.status.phase = p
	r.status.mu.Unlock()
}

// firstError reports the fault that ended the run, if one has.
func (r *runner) firstError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.firstErr
}

func (r *runner) run(ctx context.Context) error {
	// The run context is cancelled to stop reading; the shutdown work that follows must NOT inherit
	// that cancellation, or the final flush is cancelled at exactly the moment it matters most.
	runCtx, stopReading := context.WithCancel(ctx)
	defer stopReading()

	// JOINING BEFORE THE OPENS, for the same reason the config watch starts there: a worker that is
	// slow to open is still a worker, and a status document that cannot see it during exactly the
	// window somebody is watching is the document's own failure. Leaving is deferred here so it runs
	// after everything below has stopped touching a lane.
	r.joinCluster(ctx)
	defer r.leaveCluster(context.WithoutCancel(ctx))

	// THE CONFIG WATCH STARTS BEFORE ANYTHING IS OPENED, and takes ctx rather than runCtx so it keeps
	// answering through the drain. "Did my config take effect" is asked most often while a pipeline is
	// stuck opening against an unreachable upstream, which is exactly the window a watch started after
	// the opens would miss. It touches no record and holds no lock this function takes; see config.go.
	defer r.watchConfig(ctx)()

	// Sinks open before sources. A source that produces before its sink can accept is a batch with
	// nowhere to go, held against the lane budget for nothing.
	if err := r.openSinks(runCtx); err != nil {
		r.fail(err)
		r.setPhase(telemetry.PhaseFailed)
		return err
	}
	r.status.mu.Lock()
	r.status.sinkReady = true
	r.status.mu.Unlock()
	// RECOVERY BEFORE ANY RECORD MOVES. A previous run may have left staged artifacts the
	// destination has and this process does not know about; resolving them after starting to read
	// would let a new checkpoint advance a cursor past records nobody published. It runs after the
	// sinks are open because only an open sink can answer for its own committables.
	if err := r.recoverCheckpoint(runCtx); err != nil {
		r.fail(err)
		r.setPhase(telemetry.PhaseFailed)
		return err
	}
	if err := r.openSources(runCtx); err != nil {
		r.fail(err)
		r.setPhase(telemetry.PhaseFailed)
		return err
	}
	r.status.mu.Lock()
	r.status.sourceReady = true
	r.status.mu.Unlock()
	r.setPhase(telemetry.PhaseRunning)

	var wg sync.WaitGroup

	// The commit pump runs on its own goroutine per the package doc: phase three must never run
	// inline from the persister, because a slow connector would then block the flush cycle for every
	// other lane. It takes ctx rather than runCtx, so late acknowledgements are still delivered
	// while draining.
	pumpDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(pumpDone)
		r.commitPump(ctx)
	}()

	flushStop := make(chan struct{})
	flushDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(flushDone)
		r.flushLoop(ctx, flushStop)
	}()

	// THE SECOND GOROUTINE PER SOURCE. It takes ctx rather than runCtx so heartbeats keep an
	// upstream's retention moving while the pipeline drains, and it is stopped by a channel so every
	// call into a connector has returned before Run does — and therefore before the host calls Close.
	// The lease loop takes ctx rather than runCtx, so leases keep being renewed while the pipeline
	// drains — a worker that stopped renewing when it stopped reading would have its lanes reassigned
	// underneath a drain that is still settling records for them.
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.leaseLoop(ctx)
	}()

	controlStop := make(chan struct{})
	for id := range r.p.sources {
		wg.Add(1)
		go func(id record.NodeID) {
			defer wg.Done()
			r.controlLoop(ctx, id, controlStop)
		}(id)
	}

	readers := &sync.WaitGroup{}
	for id := range r.p.sources {
		readers.Add(1)
		go func(id record.NodeID) {
			defer readers.Done()
			r.readLoop(runCtx, id)
		}(id)
	}
	readers.Wait()
	stopReading()

	// DRAINING IS ITS OWN PHASE, and the two fields that describe it are only meaningful here. An
	// operator watching a stop wants to know when it started and when canal will give up, and
	// "drained" and "drain timed out" are distinct events because the second means records replay.
	r.status.mu.Lock()
	r.status.phase = telemetry.PhaseDraining
	r.status.stoppingSince = time.Now()
	r.status.drainDeadline = r.status.stoppingSince.Add(r.deps.GracePeriod)
	r.status.mu.Unlock()

	// Drain: settlement continues while the ledger empties. A graceful stop must not discard a
	// commit that is one millisecond from safe.
	//
	// A FAILING RUN IS NOT DRAINED, and that is a deliberate exception rather than an oversight. The
	// records a fault-stop left in flight are held by a write that gave up, so nothing will ever
	// present them again and the ledger can never empty: draining is then a guaranteed wait of the
	// whole grace period — thirty seconds by default — for an outcome that is already known. It is
	// also not free of consequence, so it is logged and the leak is reported below: those records
	// replay on restart, which at-least-once permits and an operator should be told about.
	// THE TERMINAL FLUSH COMES BEFORE THE DRAIN, not after.
	//
	// The drain waits for the ledger to empty, and for a deferring sink the only thing that can
	// empty it is a flush or a commit. Draining first therefore waits for settlement that only the
	// step after it can produce — a deadlock by construction, and for a staging sink that seals only
	// at end of input it is guaranteed rather than occasional. Found by running a Committer through
	// the drain and watching it time out with the commit still pending.
	terminal := connector.FlushEndOfInput
	if ctx.Err() != nil {
		terminal = connector.FlushDrain
	}
	r.flushOnce(context.WithoutCancel(ctx), terminal)

	var drainErr error
	if r.firstError() != nil {
		r.deps.Log.Warn("the pipeline is stopping on a fault, so it is not draining; in-flight records will replay",
			"pipeline", r.p.spec.ID)
	} else {
		drainCtx, cancelDrain := context.WithTimeout(context.WithoutCancel(ctx), r.deps.GracePeriod)
		defer cancelDrain()
		drainErr = r.p.ledger.Drain(drainCtx)
	}

	// And one more after the drain, so anything that settled while it ran reaches the store. The
	// reason is the same one the pre-drain flush used: a bounded pipeline that reached the end of
	// its input is finalising for good, and a staging sink has to seal the 4 MB file it was hoping
	// would reach 128 MB; a pipeline stopped by a signal may well be restarted, and the same sink is
	// right to keep waiting.
	r.flushOnce(context.WithoutCancel(ctx), terminal)

	// And one final gauge refresh, so the last scrape describes the state the pipeline actually
	// stopped in. Without it a run shorter than the flush interval published no gauges at all, and
	// a completed lane kept reporting the in-flight count it had a tick before it drained.
	r.refreshGauges()

	// CLOSING flushStop ONLY ASKS THE LOOP TO STOP; waiting for flushDone is what makes it true.
	//
	// Without the wait, a ticker firing at this instant leaves the flush loop inside flushOnce while
	// the main goroutine closes the ledger underneath it. Ledger.send then finds the ledger closed
	// and DROPS the acknowledgement — silently, and by design, because dropping is the safe
	// direction for the data. But the ack is phase three: dropping it means the source is never told
	// it may advance, so a source that prunes its upstream on commit never releases the tail of a
	// clean shutdown, every time the race is lost.
	//
	// It surfaced as canal_records_committed_total intermittently missing from a scrape, which is
	// exactly the sort of thing that stays invisible until something measures it.
	// The control loops stop here too, and their last act is to deliver any nack the write path
	// queued in the final moments — a record already settled and already committed past, which
	// nothing else will ever tell the upstream about.
	close(controlStop)
	close(flushStop)
	<-flushDone
	_ = r.p.ledger.Close() // closing the ack channel is what ends the pump
	<-pumpDone
	wg.Wait()

	r.reportUndrained()

	if leaks := r.p.ledger.Leaks(); len(leaks) > 0 {
		// A drain that did not complete is a DIFFERENT event from a completed drain, because it
		// means records may replay. Naming it is the difference between an operator knowing and
		// guessing.
		r.deps.Log.Warn("drain did not settle every group; records in these may replay on restart",
			"pipeline", r.p.spec.ID, "groups", len(leaks))
		for _, lk := range leaks {
			r.p.obs.leaks.Add(1, r.p.obs.pipeline, string(lk.Node))
		}
	}
	if drainErr != nil {
		r.fail(fmt.Errorf("engine: drain: %w", drainErr))
	}

	// THE TERMINAL PHASE, and the distinction between the last two is the whole reason PhaseCompleted
	// exists: without it a finished batch job looks identical to a stalled stream. A run that ended
	// because its input ended is COMPLETED; one that ended because somebody cancelled it is STOPPED
	// and may well be restarted. A fault outranks both.
	switch {
	case r.firstError() != nil:
		r.setPhase(telemetry.PhaseFailed)
	case ctx.Err() != nil:
		r.setPhase(telemetry.PhaseStopped)
	default:
		r.setPhase(telemetry.PhaseCompleted)
	}
	// One last condition refresh, so the final scrape describes the state the pipeline stopped in
	// rather than the one it was in a tick earlier.
	r.refreshGauges()

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.firstErr
}

// openSources constructs a runtime per source and opens it.
func (r *runner) openSources(ctx context.Context) error {
	for id := range r.p.sources {
		whenFull := r.whenFullFor(id)
		lc := newLaneCtl(r.deps, r.specFields(), id,
			func(lane record.LaneID, ord connector.Ordering, budget int) error {
				if err := r.p.ledger.Lane(lane, ord, budget, whenFull); err != nil {
					return err
				}
				// STAMPED WHERE THE LEDGER LEARNS THE LANE, so it happens in BOTH deployment shapes.
				// Setting it only after a successful claim covered the coordinated one and left every
				// standalone acknowledgement carrying epoch 0 — which is the only shape cmd/canal
				// builds, so the defect the unreachable-function guard found would have stayed live
				// in the shipping binary while the guard read as satisfied. epochFor answers
				// singleWorkerEpoch here; planAndClaim raises it to the lease's when there is one.
				r.p.ledger.SetEpoch(lane, r.leases.epochFor(lane))
				return nil
			},
			func(lane record.LaneID) connector.Admission {
				st := r.p.ledger.Stats(lane)
				return connector.Admission{
					Budget:   int(st.InFlightBudget),
					Headroom: int(st.InFlightBudget) - int(st.InFlight),
				}
			}, r.leases)
		if err := lc.load(ctx); err != nil {
			return err
		}
		r.setLaneCtl(id, lc)

		// CLAIMED BEFORE THE SOURCE IS OPENED, for the warm-start path: load() has just read this
		// node's durable lanes, and the source will ask Assigned inside Open. The cold-start path goes
		// through afterAnnounce, because those lanes do not exist yet.
		lc.afterAnnounce = func(c context.Context) { r.planAndClaim(c, id, lc) }
		r.planAndClaim(ctx, id, lc)

		rt := &sourceRuntime{
			baseRuntime: baseRuntime{
				ctx: ctx, deps: r.deps,
				tenant: r.p.spec.Tenant, pipeline: r.p.spec.ID, node: id,
				streams: configuredStreams(r.p.spec), component: r.p.sources[id].Name,
				cfg: r.p.configs[id],
			},
			lanes: lc,
			state: &stateHandle{deps: r.deps, tenant: r.p.spec.Tenant, pipeline: r.p.spec.ID, node: id},
		}

		// Every call into a connector goes through the sandbox, so a panic is a classified fault and
		// a hang is abandoned rather than wedging the host.
		src := r.p.sources[id]
		if _, err := sandbox(ctx, r.p, id, src.Name, rt,
			func(c context.Context, rt *sourceRuntime) (struct{}, error) {
				return struct{}{}, src.Source.Open(c, rt)
			}); err != nil {
			return fmt.Errorf("engine: opening source %s: %w", id, err)
		}
		r.setSourceRT(id, rt)
	}
	return nil
}

// openSinks opens every sink.
func (r *runner) openSinks(ctx context.Context) error {
	for id, sk := range r.p.sinks {
		rt := &sinkRuntime{baseRuntime: baseRuntime{
			ctx: ctx, deps: r.deps,
			tenant: r.p.spec.Tenant, pipeline: r.p.spec.ID, node: id,
			streams: configuredStreams(r.p.spec), component: sk.Name,
			cfg: r.p.configs[id],
		}}
		r.setSinkRT(id, rt)
		opening := connector.Opening{
			Guarantee: r.p.negotiated.Guarantee,
			Streams:   configuredStreams(r.p.spec),
		}
		if _, err := sandbox(ctx, r.p, id, sk.Name, opening,
			func(c context.Context, o connector.Opening) (struct{}, error) {
				return struct{}{}, sk.Sink.Open(c, rt, o)
			}); err != nil {
			return fmt.Errorf("engine: opening sink %s: %w", id, err)
		}

		// The encoder opens with its sink and through the same sandbox. A codec is third-party code
		// on the hot path like any other component: it can panic, and a panic in it must become a
		// classified fault rather than take the host down.
		cc := r.p.codecs[id]
		if cc == nil {
			continue
		}
		if _, err := sandbox(ctx, r.p, id, sk.Name, rt,
			func(c context.Context, rt *sinkRuntime) (struct{}, error) {
				return struct{}{}, cc.open(c, rt)
			}); err != nil {
			return fmt.Errorf("engine: opening the codec for sink %s: %w", id, err)
		}
	}
	return nil
}

// maxReadBatch bounds the buffer one Read is handed. The lane budget is the real backpressure.
const maxReadBatch = 512

// readLoop is one source's read goroutine.
//
// A source runs TWO goroutines in the full design — this one and a control goroutine for Commit,
// Heartbeat and assignment refresh — because promising that Commit never runs concurrently with a
// blocking Read is unsatisfiable: an idle tail source would then never commit. The commit pump is
// that second goroutine, shared across sources here because nothing in it is per-source.
func (r *runner) readLoop(ctx context.Context, id record.NodeID) {
	src := r.p.sources[id]
	rt := r.sourceRT(id)

	// Delivery does NOT inherit the read context.
	//
	// Cancelling stops READING; it must not abandon a batch already admitted to the ledger. A group
	// whose delivery is interrupted can never settle — nothing will ever write those records again —
	// so the ledger stays non-empty and the drain that waits for it times out. That is not a
	// hypothetical: it is what a cancelled run did before this existed, wedged at 499 settlements of
	// 500 with the write for the five-hundredth having already succeeded at the sink.
	//
	// The grace period bounds it, so a wedged sink cannot make shutdown hang forever.
	deliverCtx, cancelDeliver := context.WithTimeout(context.WithoutCancel(ctx), r.deps.GracePeriod)
	defer cancelDeliver()

	assigned, err := rt.lanes.Assigned(ctx)
	if err != nil {
		r.fail(err)
		return
	}
	if len(assigned) == 0 {
		r.deps.Log.Warn("source announced no lanes; it has nothing to read", "node", id)
		return
	}

	// One allocator per lane. It owns record identity: ids, group ids and the origin stamp are the
	// core's to assign, never the connector's.
	lane := assigned[0]
	alloc := record.NewAllocator(r.p.spec.Tenant, r.p.spec.ID, id, lane.ID, lane.Spec.Stream, 1, 1)

	// Resolved once. Neither can change during a run, and the shed path logs its policy on a line
	// that can fire twenty thousand times a second.
	policy := r.whenFullFor(id)
	clock := r.clockFor(id)

	// readAttempt is RESET after every successful read. Carrying it across successes would make
	// MaxAttempts a lifetime budget rather than a per-failure one, so a source that hiccuped four
	// times over a week would stop on the fourth — a week apart, and correctly reported as
	// "retries exhausted".
	var readAttempt attempt

	for {
		if ctx.Err() != nil {
			return
		}

		batch := record.NewBatch(alloc, maxReadBatch)
		_, err := sandbox(ctx, r.p, id, src.Name, batch,
			func(c context.Context, b *record.Batch) (struct{}, error) {
				return struct{}{}, src.Source.Read(c, b)
			})

		switch {
		case errors.Is(err, fault.ErrEndOfInput):
			// A bounded lane is complete. Finishing it is durable, which is what lets a gate that
			// depends on it open exactly once.
			if ferr := rt.lanes.Finish(context.WithoutCancel(ctx), lane.ID); ferr != nil {
				r.fail(ferr)
			}
			r.p.ledger.FinishLane(lane.ID)
			return
		case errors.Is(err, context.Canceled):
			return
		case err != nil:
			// A READ FAULT CANNOT BE DEAD-LETTERED OR DROPPED: there is no record yet to route, so
			// the only two answers are wait-and-try-again and stop. Both terminal dispositions and
			// a stall collapse into stopping this source, which is why the disposition is used
			// only to decide whether to keep going.
			if readAttempt.started.IsZero() {
				readAttempt.started = time.Now()
			}
			r.p.obs.fault(id, err)
			rt := route(err, false, r.p.spec.Retry, &readAttempt, time.Now())
			if rt.disp != dispRetry {
				r.fail(fmt.Errorf("engine: source %s read (%s after %d attempts): %w",
					id, rt.reason, readAttempt.count, err))
				return
			}
			r.deps.Log.Warn("retrying a read",
				"node", id, "wait", rt.delay, "attempt", readAttempt.count,
				"class", fault.ClassOf(err), "error", err)
			r.p.obs.waited(id, fault.ClassOf(err), rt.delay)
			if serr := sleep(ctx, rt.delay); serr != nil {
				return
			}
			continue
		}
		readAttempt = attempt{}

		// THE CLOCK CHECK RUNS BEFORE ADMISSION, because that is where a timestamp enters canal and
		// the only point at which the record is still the source's alone. Once admitted its event
		// time has already been used by the lane's event-time lag.
		batch, err = r.applyClock(deliverCtx, clock, batch)
		if err != nil {
			r.fail(err)
			return
		}

		// A record its own source declared broken is disposed of before admission, for the same
		// reason: it never takes a settlement reference and the position still advances over it.
		batch, err = r.routeMarked(deliverCtx, id, batch)
		if err != nil {
			r.fail(err)
			return
		}

		r.p.obs.recordsRead.Add(float64(batch.Len()), r.p.obs.pipeline, laneLabel(lane.ID), src.Name)
		if batch.Len() > 0 {
			// A lane that produced records is not quiet, which ends any idleness the control loop had
			// reported. A ZERO-RECORD POSITIONED BATCH deliberately does not count: it advances the
			// cursor without producing, so the lane genuinely has nothing to say and a heartbeat is
			// exactly what should keep holding its slot.
			r.activity.saw(lane.ID, time.Now())
		}

		if err := r.p.ledger.Admit(ctx, batch); err != nil {
			// A SHED IS NOT A FAILURE, it is the policy working. The lane was at its budget and the
			// operator configured something other than block, so the batch does not enter the
			// pipeline, its position advances past it and the source is told on the next
			// acknowledgement. Reading continues — stopping here would turn a load-shedding policy
			// into a slightly noisier version of stopping.
			//
			// LOUDLY, though. This is the one configured path in the engine that loses data on
			// purpose, and every record it drops is counted under buffer_full and named in a log
			// line at ERROR. An operator who chose it should see exactly what it cost.
			//
			// COUNTED BEFORE THE CANCELLATION CHECK, deliberately. The ledger has already dropped
			// these records and advanced past them, so returning first because shutdown happened to
			// win the race would leave the engine's count short of the ledger's — the one number an
			// operator has for a configured loss, quietly undercounting at every shutdown.
			shed := errors.Is(err, ledger.ErrShed)
			if shed {
				r.deps.Log.Error("records were dropped because the lane is full and its policy sheds",
					"node", id, "lane", lane.ID, "records", batch.Len(),
					"when_full", policy, "budget", r.p.spec.LaneBudget)
				r.p.obs.recordsAbandoned.Add(float64(batch.Len()), r.p.obs.pipeline,
					laneLabel(lane.ID), telemetry.ReasonBufferFull)
			}
			if ctx.Err() != nil {
				return
			}
			if shed {
				continue
			}
			r.fail(fmt.Errorf("engine: admitting a batch from %s: %w", id, err))
			return
		}

		if batch.Len() == 0 {
			// A positioned batch with no records: the lane advanced without producing. Legal, and
			// the ledger has already taken the position.
			continue
		}

		if err := r.deliver(deliverCtx, batch); err != nil {
			r.fail(err)
			return
		}
	}
}

// request is one sink call plus the records it carries, so settlement can attribute the result.
type request struct {
	req     *connector.Request
	records []*record.Record
}

// buildRequests turns a batch into the sink calls it becomes.
//
// A STRUCTURED sink is handed the whole batch as Rows: the records themselves, no encoding, no
// framing, no ambiguity about where one ends.
//
// A BYTE sink is encoded by its resolved chain. Whether the batch becomes ONE request or one per
// record is decided by [codecChain.batches], and the rule it encodes — a framer is what makes
// batching legal — is a correctness constraint, not a tuning knob. See that method for the blob
// this used to produce without one.
// It takes a record slice rather than the batch, because a retry re-presents a SUBSET: the records
// a previous attempt failed on, re-encoded from scratch. Encoding is not cached across attempts —
// an encoder is allowed to be stateful, and reusing bytes it produced for a call that failed would
// assume otherwise.
func buildRequests(ctx context.Context, sk *registry.ResolvedSink, cc *codecChain, records []*record.Record) ([]request, error) {
	if sk.Structured != nil {
		req := &connector.Request{
			Count:   len(records),
			Records: make([]record.Ref, 0, len(records)),
			Rows:    records,
		}
		for _, rec := range records {
			req.Records = append(req.Records, rec.Ref())
		}
		return []request{{req: req, records: records}}, nil
	}

	if cc.batches() {
		var body []byte
		refs := make([]record.Ref, 0, len(records))
		for _, rec := range records {
			var err error
			if body, err = cc.encode(ctx, body, rec); err != nil {
				return nil, fault.Mapping(fault.OpEncode, fmt.Errorf("encoding record %v: %w", rec.Origin().ID, err))
			}
			refs = append(refs, rec.Ref())
		}
		// The terminator closes the request rather than any single record — a JSON array's "]".
		// Appended once, after the last frame, exactly as the Framer contract says.
		body = append(body, cc.framer.Terminator()...)
		req, err := finish(cc, body, refs, len(records))
		if err != nil {
			return nil, err
		}
		return []request{{req: req, records: records}}, nil
	}

	out := make([]request, 0, len(records))
	for _, rec := range records {
		body, err := cc.encode(ctx, nil, rec)
		if err != nil {
			return nil, fault.Mapping(fault.OpEncode, fmt.Errorf("encoding record %v: %w", rec.Origin().ID, err))
		}
		req, err := finish(cc, body, []record.Ref{rec.Ref()}, 1)
		if err != nil {
			return nil, err
		}
		out = append(out, request{req: req, records: []*record.Record{rec}})
	}
	return out, nil
}

// finish compresses a body if the chain has a compressor and assembles the request.
//
// UncompressedBytes is recorded BEFORE compression, because it is what the sink's own byte-size
// limits are expressed against and what makes a compression ratio computable at all.
func finish(cc *codecChain, body []byte, refs []record.Ref, count int) (*connector.Request, error) {
	uncompressed := len(body)
	if cc.compressor != nil {
		out, err := cc.compressor.Compress(nil, body)
		if err != nil {
			return nil, fault.Permanent(fault.OpEncode, fmt.Errorf("compressing a request: %w", err))
		}
		body = out
	}
	return &connector.Request{
		Count:             count,
		Records:           refs,
		Body:              body,
		UncompressedBytes: uncompressed,
		ContentType:       cc.contentType,
		ContentEncoding:   cc.contentEncoding,
	}, nil
}

// deliver writes one batch to every sink that takes main records and settles the outcomes.
//
// Sinks are filtered by EDGE SELECT rather than written to indiscriminately. Before this, every
// sink in the graph received every batch, which is accidentally right when all edges carry main
// and silently wrong the moment one carries failed — a dead-letter sink would have received the
// whole stream.
func (r *runner) deliver(ctx context.Context, batch *record.Batch) error {
	for _, id := range r.mainSinks {
		if err := r.writeSet(ctx, id, batch.Records, batch.Lane, batch.Position); err != nil {
			return err
		}
		// A deferring sink holds what it was just given, so this is where the pipeline finds out it
		// has accumulated enough un-durable work to be worth flushing now.
		r.flushIfDue(ctx, id)
	}
	return nil
}

// outcomesFor turns one WriteResult into the ledger's outcomes.
//
// IT IS CALLED WITH THE RECORDS THAT LANDED, never the whole request. Its one caller —
// [runner.writeOnce] — has already removed everything res.Failed names, because a named failure
// belongs to the routing tree in retry.go: it may deserve a retry, and settling it here would
// advance the cursor past a record the policy was about to re-present.
//
// The failed arm below is therefore UNREACHABLE, and it stays for one reason: if a future caller
// ever passes the full set, abandoning a failed record is wrong but reporting it Delivered is
// catastrophic. It is the safe floor under a mistake, not a live path.
//
// Passing everything to this function IS the defect this PR fixed. Every named failure was settled
// Abandoned on first sight, so a sink rejecting one record of a batch for a transient reason lost
// it permanently. Read the arm as a tombstone.
func outcomesFor(node record.NodeID, records []*record.Record, res connector.WriteResult) []ledger.Outcome {
	failed := make(map[record.RecordID]*fault.Fault, len(res.Failed))
	for i := range res.Failed {
		failed[res.Failed[i].Record] = res.Failed[i].Fault()
	}
	dup := make(map[record.RecordID]bool, len(res.Duplicates))
	for _, id := range res.Duplicates {
		dup[id] = true
	}

	out := make([]ledger.Outcome, 0, len(records))
	for _, rec := range records {
		id := rec.Origin().ID
		o := ledger.Outcome{Record: id, Node: node, Disposition: ledger.Delivered}
		switch {
		case failed[id] != nil:
			o.Disposition, o.Fault = ledger.Abandoned, failed[id]
		case dup[id]:
			o.Disposition = ledger.Duplicate
		}
		out = append(out, o)
	}
	return out
}

// flushLoop is phase two: it takes the lanes whose prefix has advanced to a new safe position and
// makes them durable.
//
// Phase three does not run here. It runs in the commit pump, from acknowledgements the ledger emits
// only after Committed — which is what keeps "canal's own write is flushed before upstream is told"
// an ordering the code enforces rather than one the prose asks for.
func (r *runner) flushLoop(ctx context.Context, stop <-chan struct{}) {
	interval := r.deps.FlushInterval
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-stop:
			return
		case <-t.C:
			r.flushOnce(context.WithoutCancel(ctx), connector.FlushCheckpoint)
			// Unconditionally, even when nothing was flushable: a stalled pipeline produces no
			// flushes, and that is precisely when its checkpoint age has to keep climbing.
			r.refreshGauges()
		}
	}
}

// flushOnce performs one phase-two write.
//
// SINKS FLUSH FIRST. A cursor is only allowed to name a position whose records are durable AT THE
// DESTINATION, so every deferring sink is given the chance to earn its acknowledgement before the
// ledger is asked what has resolved. Asking first and flushing second would persist a position for
// records still sitting in a sink's buffer, which is the failure ADR 0006 exists to prevent.
func (r *runner) flushOnce(ctx context.Context, reason connector.FlushReason) {
	r.flushSinks(ctx, reason)

	// A Committer stages on Write and publishes on Commit, so it mints its committables here —
	// BEFORE the checkpoint is written, because the checkpoint has to carry them.
	id := r.checkpointer.next()
	minted, err := r.prepare(ctx, connector.CommitPoint{
		ID:       id,
		Reason:   reason,
		Deadline: time.Now().Add(r.deps.GracePeriod),
	})
	if err != nil {
		r.fail(err)
		return
	}
	r.checkpointer.stage(id, minted)

	flushable := r.p.ledger.Flushable()

	// Nothing to record at all: no cursor moved and no committable was minted. Skipping keeps an
	// idle pipeline from writing a checkpoint per tick.
	if len(flushable) == 0 && len(minted) == 0 && !r.checkpointer.hasPending() {
		return
	}

	cp, err := r.buildCheckpoint(ctx, id)
	if err != nil {
		r.fail(err)
		return
	}
	r.fillLaneStates(cp, flushable)

	// ONE ATOMIC WRITE: every lane cursor plus the checkpoint carrying the committables that cover
	// them. See commit.go for why they cannot be two writes.
	start := time.Now()
	if err := r.persistCheckpoint(ctx, cp, flushable); err != nil {
		r.fail(err)
		return
	}
	r.p.obs.commitLatency.ObserveSince(start, r.p.obs.pipeline, telemetry.CommitPhasePersist)
	r.markCheckpointed(flushable)

	// Step three of the two-phase commit, and only after the checkpoint is durable. A crash here
	// leaves committables recovery will offer back to the sink, which is the whole reason they are
	// written first.
	if offer := r.checkpointer.offerable(); len(offer) > 0 {
		if err := r.publish(ctx, id, offer); err != nil {
			r.fail(err)
			return
		}
	}

	// ONLY NOW may the ledger emit acknowledgements for these positions. Calling Committed before
	// the write returned would tell a source to advance past data canal has no durable record of,
	// which is design rule R4's original violation.
	if len(flushable) > 0 {
		r.p.ledger.Committed(flushable)
	}
}

// fillLaneStates copies the lane table into the checkpoint envelope.
//
// The cursors are ALSO written as their own rows in the same batch, which is not a second
// representation so much as two readers: the lane rows are what a warm start reads to answer
// Assigned, and the envelope is what makes a committable and the span it covers one atomic record.
func (r *runner) fillLaneStates(cp *Checkpoint, positions map[record.LaneID]record.Position) {
	for _, lc := range r.laneCtls() {
		lc.mu.Lock()
		for id, rec := range lc.lanes {
			cursor := rec.Cursor
			if pos, ok := positions[id]; ok {
				cursor = pos
			}
			cp.Lanes[id] = LaneState{
				Cursor:     cursor,
				Group:      rec.Spec.Group,
				After:      rec.Spec.StartAfter,
				Kind:       rec.Spec.Kind,
				Ordering:   rec.Spec.Ordering,
				Bounded:    rec.Spec.Boundedness == connector.Bounded,
				Finished:   rec.Finished,
				FinishedAt: rec.FinishedAt,
				Label:      rec.Spec.Label,
				Version:    lc.versions[id],
			}
		}
		lc.mu.Unlock()
	}
}

// commitPump is phase three: it hands durable acknowledgements to sources.
func (r *runner) commitPump(ctx context.Context) {
	for ack := range r.p.ledger.Acks() {
		src, srcNode, ok := r.sourceFor(ack.Lane)
		if !ok {
			continue
		}

		// THE FENCE WITH TEETH. Every other lease check decides what this worker READS; this one
		// decides what it tells an upstream it may forget. A worker that lost a lane and acknowledged
		// it anyway discards records the new holder has not delivered, and no epoch undoes that
		// because by then the data is gone from the source. See lease.go.
		if !r.commitAllowed(ack.Lane) {
			r.deps.Log.Warn("not acknowledging a lane this worker no longer holds; its upstream keeps "+
				"the records for the new holder", "lane", ack.Lane)
			continue
		}

		start := time.Now()
		if _, err := sandbox(ctx, r.p, srcNode, src.Name, ack,
			func(c context.Context, a connector.Ack) (struct{}, error) {
				return struct{}{}, src.Source.Commit(c, a)
			}); err != nil {
			// A source that refuses an acknowledgement has not lost data: the position stays durable
			// and the next acknowledgement carries it again. Logged rather than fatal.
			r.deps.Log.Warn("source refused an acknowledgement; its upstream position was not advanced",
				"lane", ack.Lane, "error", err)
			continue
		}
		r.p.obs.commitLatency.ObserveSince(start, r.p.obs.pipeline, telemetry.CommitPhaseUpstrm)
		r.p.obs.recordsCommitted.Add(float64(ack.Records), r.p.obs.pipeline, laneLabel(ack.Lane))
	}
}

// specFields is the slice of the spec the lane runtime needs.
func (r *runner) specFields() specRefFields {
	return specRefFields{
		tenant:   r.p.spec.Tenant,
		pipeline: r.p.spec.ID,
		budget:   r.p.spec.LaneBudget,
	}
}

// sourceFor finds the source node that owns a lane.
func (r *runner) sourceFor(lane record.LaneID) (*registry.ResolvedSource, record.NodeID, bool) {
	for id, lc := range r.laneCtls() {
		lc.mu.Lock()
		_, ok := lc.lanes[lane]
		lc.mu.Unlock()
		if ok {
			return r.p.sources[id], id, true
		}
	}
	return nil, "", false
}

// configuredStreams projects the spec's stream table into the connector-facing view.
func configuredStreams(s spec.Spec) []connector.ConfiguredStream {
	out := make([]connector.ConfiguredStream, 0, len(s.Streams))
	for _, c := range s.Streams {
		out = append(out, connector.ConfiguredStream{
			Stream: c.Stream,
			Mode:   c.Write,
			Keys:   c.Keys,
		})
	}
	return out
}

// controlIntervalFor resolves a source node's control cadence.
//
// pkg/registry attaches heartbeat_interval to a source's config form ONLY when it declares
// Heartbeater, which makes it the most specific statement anyone has made about how often that
// source wants to be told its lanes are quiet — and yesterday's control goroutine ignored it in
// favour of the deployment-wide Deps.ControlInterval. Found by the inert-field guard on its first
// run, in code written the day before.
func (r *runner) controlIntervalFor(id record.NodeID) time.Duration {
	cfg := r.p.configs[id]
	if cfg == nil || !cfg.Has(config.FieldHeartbeatInterval) {
		return r.deps.ControlInterval
	}
	d, err := config.Get[time.Duration](cfg, config.FieldHeartbeatInterval)
	if err != nil || d <= 0 {
		return r.deps.ControlInterval
	}
	return d
}

// whenFullFor resolves a source node's admission policy.
//
// SPECIFIC BEATS GENERAL, the same precedence spec.StreamFor uses: the node's own stage-standard
// when_full wins over the pipeline-wide one. spec.Spec says Retry and WhenFull are node-overridable
// and the registry offers when_full on EVERY node's config form, so an operator setting it there had
// every reason to expect it to mean something — and until this existed, neither the node field nor
// the pipeline field was read anywhere at all.
//
// Only a SOURCE's policy is consulted, because admission happens at the source edge and nowhere
// else. A sink's when_full will start mattering when a buffer node can sit in front of it.
func (r *runner) whenFullFor(id record.NodeID) connector.WhenFull {
	cfg := r.p.configs[id]
	if cfg == nil || !cfg.Has(config.FieldWhenFull) {
		return r.p.spec.WhenFull
	}
	raw, err := config.Get[string](cfg, config.FieldWhenFull)
	if err != nil {
		return r.p.spec.WhenFull
	}
	w, ok := connector.ParseWhenFull(raw)
	if !ok {
		// Unreachable through a validated config — the field is an enum and Validate refuses anything
		// else — so falling back rather than failing is the conservative reading of a state that
		// should not exist.
		return r.p.spec.WhenFull
	}
	return w
}
