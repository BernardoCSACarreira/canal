package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/ledger"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// THE READ PATH READS EVERY LANE IT HOLDS. Until this file existed it read one.
//
// readLoop took assigned[0] and looped on it forever, so a worker holding thirty scan chunks read
// one of them and twenty-nine were read by nobody. Three declarations covered that gap and all
// three were inert: connector.LaneReader, the interface written for exactly this;
// SourceCaps.ReadsLanes, which four hostile connectors declare; and
// registry.ResolvedSource.ReadConcurrency, a method with no callers anywhere in the module.
//
// FANNING Source.Read OUT OVER GOROUTINES IS NOT THE FIX, and it was tried. "Read is never called
// concurrently with itself" is the contract every connector in this module is written against, and
// the race detector found the violation inside one run. LaneReader exists because it takes a SET of
// pre-bound batches on ONE goroutine: a source multiplexing 900 streams over a single connection
// and a source with 32 independent chunk readers are then the same interface at ReadConcurrency 1
// and 32 respectively, rather than two interfaces.
//
// Two further promises the read loop made and did not keep are kept here, because they are the same
// twenty lines and the multi-lane shape makes both reachable in ordinary operation rather than only
// under a hostile connector:
//
//   - record.Batch.EndOfLane, which the engine never read. It is how a source retires ONE lane of
//     the many it holds — "a single finished lane is signalled by that batch's EndOfLane, not by an
//     error" — so without it ReadLanes has no way to finish anything short of all of them. On the
//     one-lane path it was survivable by accident: linefile emits a zero-record EndOfLane batch and
//     then returns ErrEndOfInput on the NEXT call, so the lane retired one round-trip late. An
//     unbounded source retiring a revoked partition got no LaneFinished ack at all.
//   - The spin rule. Both Source.Read and LaneReader.ReadLanes say the core raises
//     fault.PermanentContract on the second consecutive empty unpositioned return. Nothing did. See
//     [spinner].

// idsPerLane is how many record ids and group ids one lane may hand out in a run, and
// maxLanesPerRun is how many lanes may draw a slice of that space.
//
// EVERY ALLOCATOR USED TO START AT 1, AND THE LEDGER KEYS IDS GLOBALLY. l.groups is
// map[record.GroupID] and l.byRec is map[record.RecordID], and neither carries a lane — so lane A's
// first batch and lane B's first batch were the same group, and their first records the same
// record. The ledger refuses a duplicate live group loudly, which is how this surfaced; the
// duplicate RECORD id would not have been refused at all, and settlement is keyed on record id.
//
// One lane per source node, and one source node in every test, is the only reason it never showed.
// Two source nodes collide on main today. A source reading eight lanes at once collides inside a
// second, which is how this was found.
//
// record.NewAllocator's own doc says "the engine derives them from the generation so that ids do
// not repeat within a generation". This is that derivation. It did not exist.
//
// 2^44 ids per lane, 2^20 lanes per run. A lane producing a million records a second exhausts its
// slice in about five hundred years, and a run exhausts its lanes after a million announcements.
// The alternative — keying the ledger by (lane, id) — changes Outcome, Register, Expand and every
// caller of each, to fix a collision the id space can simply not have.
const (
	laneIDShift    = 44
	maxLanesPerRun = 1 << (64 - laneIDShift)
)

// allocatorFor returns a lane's identity stamp, seeded so its ids cannot collide with any other
// lane's in this run.
func (r *runner) allocatorFor(node record.NodeID, lane connector.LaneAssignment) (*record.Allocator, error) {
	n := r.laneOrdinal.Add(1) - 1
	if n >= maxLanesPerRun {
		// Wrapping would silently re-issue a live lane's ids, which is the one outcome worse than
		// stopping: the ledger would attribute one lane's settlements to another and report nothing.
		return nil, fault.Internal(fault.OpRead, fmt.Errorf(
			"engine: this run has read %d lanes, which is every id slice it has", maxLanesPerRun))
	}
	base := n << laneIDShift
	return record.NewAllocator(r.p.spec.Tenant, r.p.spec.ID, node, lane.ID, lane.Spec.Stream,
		record.RecordID(base+1), record.GroupID(base+1)), nil
}

// laneRead is one lane's read state: what to read and what stamps the records that come back.
type laneRead struct {
	lane connector.LaneAssignment

	// alloc is never retargeted and never shared. Record identity — ids, group ids, the origin
	// stamp — is the core's to assign, and a batch carries its allocator's lane. Letting a source
	// point one batch at another lane instead was tried and mislabelled 33350 of 33500 records.
	alloc *record.Allocator
}

// readLanes reads every lane assigned to a source that implements connector.LaneReader.
//
// It partitions them into at most ReadConcurrency disjoint groups and reads each group on its own
// goroutine, which is what makes ReadConcurrency 1 the multiplexed shape and ReadConcurrency N the
// independent-connections shape without the source implementing two things.
func (r *runner) readLanes(ctx, deliverCtx context.Context, id record.NodeID,
	assigned []connector.LaneAssignment,
) {
	src := r.p.sources[id]
	groups := partitionLanes(assigned, src.ReadConcurrency(r.p.spec.Parallelism))

	r.deps.Log.Info("reading lanes",
		"node", id, "lanes", len(assigned), "concurrent_reads", len(groups),
		"declared", src.Caps.ReadConcurrency, "parallelism", r.p.spec.Parallelism)

	var wg sync.WaitGroup
	for _, group := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.readLaneGroup(ctx, deliverCtx, id, group)
		}()
	}
	wg.Wait()
}

// partitionLanes splits lanes into at most n groups.
//
// DISJOINT is the word the LaneReader contract uses and it is the whole safety argument: no lane
// appears in two live calls, so no two goroutines ever touch the same batch or the same allocator,
// and a source declaring ReadConcurrency > 1 needs only whatever locking its own upstream client
// needs.
//
// Contiguous rather than round-robin, because Assigned returns lanes in announce order — a source
// that announced one lane per key range gets adjacent ranges in one call, which is the sequential
// scan it was expecting. Group sizes differ by at most one.
func partitionLanes(lanes []connector.LaneAssignment, n int) [][]connector.LaneAssignment {
	if n < 1 {
		n = 1
	}
	if n > len(lanes) {
		n = len(lanes)
	}
	out := make([][]connector.LaneAssignment, 0, n)
	for i := range n {
		out = append(out, lanes[i*len(lanes)/n:(i+1)*len(lanes)/n])
	}
	return out
}

// readLaneGroup reads one disjoint group of lanes until every lane in it is finished or revoked, or
// the run stops.
func (r *runner) readLaneGroup(ctx, deliverCtx context.Context, id record.NodeID,
	group []connector.LaneAssignment,
) {
	src := r.p.sources[id]
	rt := r.sourceRT(id)

	// Resolved once, as on the one-lane path: neither can change during a run.
	policy := r.whenFullFor(id)
	clock := r.clockFor(id)

	live := make([]laneRead, 0, len(group))
	for _, lane := range group {
		alloc, err := r.allocatorFor(id, lane)
		if err != nil {
			r.fail(err)
			return
		}
		live = append(live, laneRead{lane: lane, alloc: alloc})
	}

	var readAttempt attempt
	var idle spinner

	for {
		if ctx.Err() != nil {
			return
		}

		// A REVOKED LANE LEAVES THE READ SET; THE GROUP KEEPS READING. That is the one place this
		// differs from the one-lane path, where losing the lane and losing the source are the same
		// event. Here the other lanes in the group are still this worker's to read, and stopping the
		// call would hand a second worker's reclaim the power to stop reads it has nothing to do with.
		live = r.stillHeld(id, rt, live)
		if len(live) == 0 {
			return
		}

		// Fresh batches every call. The contract asks only that each be pre-bound and Reset, which a
		// new batch satisfies by construction, and reusing them would mean handing the source a batch
		// whose records the ledger and the sink still hold pointers into.
		dst := make([]*record.Batch, len(live))
		for i, lr := range live {
			dst[i] = record.NewBatch(lr.alloc, maxReadBatch)
		}

		_, err := sandbox(ctx, r.p, id, src.Name, dst,
			func(c context.Context, d []*record.Batch) (struct{}, error) {
				return struct{}{}, src.LaneReader.ReadLanes(c, d)
			})

		// AN ABANDONED CALL'S BATCHES ARE NOT OURS TO READ. The component never returned, its
		// goroutine is still running, and it is still calling Add on these very batches — so even
		// asking how many records one holds is a race, and admitting one would hand the ledger
		// references to records the source has not finished writing.
		if errors.Is(err, errAbandoned) {
			return
		}

		// THE ENGINE ADMITS WHAT IS IN EVERY BATCH BEFORE HANDLING THE ERROR, which is what lets the
		// contract tell a source never to discard records it has already produced. Cancellation means
		// drain: a source that buffered four records and then saw ctx expire returns those four AND
		// ctx.Err(), and dropping them because the second value was non-nil would lose data the source
		// did everything right to hand over.
		progress := false
		for i, lr := range live {
			b := dst[i]
			// Counted from what the SOURCE produced, before admission filters anything: the question
			// the spin rule asks is whether the source reported something, not whether it survived.
			progress = progress || b.Len() > 0 || !b.Position.IsZero() || b.EndOfLane
			if b.EndOfLane {
				// MARKED BEFORE THE BATCH IS ADMITTED, so the flag cannot lose a race with the flush
				// loop: this batch's position is not flushable until it is admitted, and the ledger
				// holds the flag back until the lane has nothing left in flight.
				r.p.ledger.FinishLane(lr.lane.ID)
			}
			if !r.admit(ctx, deliverCtx, id, src.Name, lr.lane.ID, b, policy, clock) {
				return
			}
		}

		// Retirement is per lane and is read off the batch, so a group of thirty chunks finishes
		// twenty-nine of them and goes on reading the thirtieth.
		live = r.retireFinished(ctx, rt, live, dst)
		if len(live) == 0 {
			return
		}

		switch {
		case err == nil:
			readAttempt = attempt{}
			if idle.saw(progress) {
				r.fail(fault.Contract(fault.OpRead, fmt.Errorf(
					"engine: source %s returned from %d consecutive ReadLanes calls over %d lanes with no records, no position and no finished lane; ReadLanes blocks until it has something to report",
					id, idle.consecutive, len(live))))
				return
			}
		case errors.Is(err, fault.ErrEndOfInput):
			// EVERY lane this source holds is finished and no more will be announced. One finished
			// lane is EndOfLane on that batch, retired above.
			for _, lr := range live {
				r.finishLane(ctx, rt, lr.lane.ID)
			}
			return
		case errors.Is(err, context.Canceled):
			return
		default:
			if !r.retryRead(ctx, id, err, &readAttempt) {
				return
			}
		}
	}
}

// stillHeld drops the lanes this worker has lost and reports what is left.
func (r *runner) stillHeld(id record.NodeID, rt *sourceRuntime, live []laneRead) []laneRead {
	kept := live[:0]
	for _, lr := range live {
		if rt.lanes.Revoked(lr.lane.ID) {
			r.deps.Log.Warn("this worker no longer holds a lane it was reading; dropping it from the read set",
				"node", id, "lane", lr.lane.ID)
			continue
		}
		kept = append(kept, lr)
	}
	return kept
}

// retireFinished finishes every lane whose batch carried EndOfLane and reports what is left.
func (r *runner) retireFinished(ctx context.Context, rt *sourceRuntime, live []laneRead,
	dst []*record.Batch,
) []laneRead {
	kept := live[:0]
	for i, lr := range live {
		if dst[i].EndOfLane {
			r.finishLane(ctx, rt, lr.lane.ID)
			continue
		}
		kept = append(kept, lr)
	}
	return kept
}

// finishLane retires a lane: durably in the lane table, so a gate that depends on it opens exactly
// once, and then in the ledger, so the next acknowledgement carries LaneFinished and the source
// learns the retirement became durable rather than inferring it from silence.
//
// IT DOES NOT WAIT FOR THE LANE'S ADMITTED GROUPS TO SETTLE, and both Ledger.FinishLane and
// record.Batch.EndOfLane say the engine should. The gap predates this file — the ErrEndOfInput path
// has always finished immediately — and closing it needs the ledger to say when a lane's last group
// settles, which it has no way to report. Every caller goes through here so that when it can, this
// is the one place that changes.
func (r *runner) finishLane(ctx context.Context, rt *sourceRuntime, lane record.LaneID) {
	if err := rt.lanes.Finish(context.WithoutCancel(ctx), lane); err != nil {
		r.fail(err)
	}
	r.p.ledger.FinishLane(lane)
}

// spinner counts consecutive reads that reported nothing at all.
//
// A read returning no records, no advanced position and no finished lane has neither blocked nor
// made progress, so the loop calls it again immediately. A source that does it forever burns a core
// and looks, from every metric outside itself, exactly like a busy pipeline. Both Source.Read and
// LaneReader.ReadLanes state that the core raises fault.PermanentContract on the second consecutive
// one; nothing did, in either shape, for as long as either contract had existed.
//
// THE SECOND AND NOT THE FIRST, because one is legal and ordinary: a source draining on
// cancellation returns early with nothing to report, and faulting on that would turn every clean
// shutdown into a contract violation against a source that behaved perfectly.
type spinner struct{ consecutive int }

// saw records one read and reports whether the source has now spun twice running.
func (s *spinner) saw(progress bool) bool {
	if progress {
		s.consecutive = 0
		return false
	}
	s.consecutive++
	return s.consecutive >= 2
}

// retryRead classifies a read fault and reports whether the caller may read again.
//
// A READ FAULT CANNOT BE DEAD-LETTERED OR DROPPED: there is no record yet to route, so the only two
// answers are wait-and-try-again and stop. Both terminal dispositions and a stall collapse into
// stopping the source, which is why the disposition is used only to decide whether to keep going.
func (r *runner) retryRead(ctx context.Context, id record.NodeID, err error, at *attempt) bool {
	if at.started.IsZero() {
		at.started = time.Now()
	}
	r.p.obs.fault(id, err)
	rt := route(err, false, r.p.spec.Retry, at, time.Now())
	if rt.disp != dispRetry {
		r.fail(fmt.Errorf("engine: source %s read (%s after %d attempts): %w",
			id, rt.reason, at.count, err))
		return false
	}
	r.deps.Log.Warn("retrying a read",
		"node", id, "wait", rt.delay, "attempt", at.count,
		"class", fault.ClassOf(err), "error", err)
	r.p.obs.waited(id, fault.ClassOf(err), rt.delay)
	return sleep(ctx, rt.delay) == nil
}

// admit takes one batch from the source to the sink: the clock check, marked-record routing, the
// ledger, and delivery. It reports whether the read loop may continue.
//
// Shared by both read paths because everything after the read is identical — the only difference
// between one Read and one ReadLanes is how many batches come back from a call.
func (r *runner) admit(ctx, deliverCtx context.Context, id record.NodeID, srcName string,
	lane record.LaneID, batch *record.Batch, policy connector.WhenFull, clock clockCheck,
) bool {
	// THE CLOCK CHECK RUNS BEFORE ADMISSION, because that is where a timestamp enters canal and the
	// only point at which the record is still the source's alone. Once admitted its event time has
	// already been used by the lane's event-time lag.
	batch, err := r.applyClock(deliverCtx, clock, batch)
	if err != nil {
		r.fail(err)
		return false
	}

	// A record its own source declared broken is disposed of before admission, for the same reason:
	// it never takes a settlement reference and the position still advances over it.
	batch, err = r.routeMarked(deliverCtx, id, batch)
	if err != nil {
		r.fail(err)
		return false
	}

	r.p.obs.recordsRead.Add(float64(batch.Len()), r.p.obs.pipeline, laneLabel(lane), srcName)
	if batch.Len() > 0 {
		// A lane that produced records is not quiet, which ends any idleness the control loop had
		// reported. A ZERO-RECORD POSITIONED BATCH deliberately does not count: it advances the
		// cursor without producing, so the lane genuinely has nothing to say and a heartbeat is
		// exactly what should keep holding its slot.
		r.activity.saw(lane, time.Now())
	}

	if err := r.p.ledger.Admit(ctx, batch); err != nil {
		// A SHED IS NOT A FAILURE, it is the policy working. The lane was at its budget and the
		// operator configured something other than block, so the batch does not enter the pipeline,
		// its position advances past it and the source is told on the next acknowledgement. Reading
		// continues — stopping here would turn a load-shedding policy into a slightly noisier version
		// of stopping.
		//
		// LOUDLY, though. This is the one configured path in the engine that loses data on purpose,
		// and every record it drops is counted under buffer_full and named in a log line at ERROR. An
		// operator who chose it should see exactly what it cost.
		//
		// COUNTED BEFORE THE CANCELLATION CHECK, deliberately. The ledger has already dropped these
		// records and advanced past them, so returning first because shutdown happened to win the race
		// would leave the engine's count short of the ledger's — the one number an operator has for a
		// configured loss, quietly undercounting at every shutdown.
		shed := errors.Is(err, ledger.ErrShed)
		if shed {
			r.deps.Log.Error("records were dropped because the lane is full and its policy sheds",
				"node", id, "lane", lane, "records", batch.Len(),
				"when_full", policy, "budget", r.p.spec.LaneBudget)
			r.p.obs.recordsAbandoned.Add(float64(batch.Len()), r.p.obs.pipeline,
				laneLabel(lane), telemetry.ReasonBufferFull)
		}
		if ctx.Err() != nil {
			return false
		}
		if shed {
			return true
		}
		r.fail(fmt.Errorf("engine: admitting a batch from %s: %w", id, err))
		return false
	}

	if batch.Len() == 0 {
		// A positioned batch with no records: the lane advanced without producing. Legal, and the
		// ledger has already taken the position.
		return true
	}

	if err := r.deliver(deliverCtx, batch); err != nil {
		r.fail(err)
		return false
	}
	return true
}
