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
// Three further promises the read loop made and did not keep are kept here, because they are the
// same twenty lines and the multi-lane shape makes all three reachable in ordinary operation rather
// than only under a hostile connector:
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
//   - "The engine admits what is in the batch BEFORE handling the error", which every error branch
//     contradicted by returning without looking at the batch — so a source draining on cancellation
//     exactly as instructed lost its last window. See the admission loop in [runner.readLaneGroup],
//     and the two things it must not do: touch an ABANDONED call's batches, which the component is
//     still writing to, and admit a batch with nothing in it, which puts a zero position into a
//     lane's prefix.

// laneIDShift splits the id space into one slice per lane: the ordinal goes in the high bits and a
// lane counts up through the low ones. maxLanesPerRun is how many slices there are.
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

// readLanes reads every lane assigned to a source that implements connector.LaneReader, INCLUDING
// the ones it announces later.
//
// It partitions the assignment into at most ReadConcurrency disjoint groups and reads each on its own
// goroutine — which is what makes ReadConcurrency 1 the multiplexed shape and N the
// independent-connections shape without the source implementing two things — and then supervises
// them, rebuilding the set when the lane table gains a lane.
//
// A LANE ANNOUNCED MID-RUN USED TO BE READ BY NOBODY. laneCtl.Changes() existed, notify() fired it on
// every announce and finish, and nothing anywhere selected on it, so the assignment was whatever
// Assigned() happened to return once. That is not a hypothetical shape: multi-stream-source calls
// maybeSweep from Read, which re-reconciles its stream set on a timer and announces a lane for every
// stream that has appeared upstream since. Those lanes were durable, planned, claimed — and never
// read.
func (r *runner) readLanes(ctx, deliverCtx context.Context, id record.NodeID,
	assigned []connector.LaneAssignment,
) {
	src := r.p.sources[id]
	rt := r.sourceRT(id)
	concurrency := src.ReadConcurrency(r.p.spec.Parallelism)

	// ONE ALLOCATOR PER LANE FOR THE WHOLE RUN, held here rather than inside a group, because groups
	// are now rebuilt and allocators must not be. A fresh allocator for a lane already being read
	// would restart its record ids inside a range the ledger may still hold — and allocatorFor draws
	// a new ordinal each time, so rebuilding on every announce would also spend the run's id space at
	// the rate the source discovers streams.
	allocs := map[record.LaneID]*record.Allocator{}

	for {
		live := make([]laneRead, 0, len(assigned))
		for _, lane := range assigned {
			alloc, ok := allocs[lane.ID]
			if !ok {
				var err error
				if alloc, err = r.allocatorFor(id, lane); err != nil {
					r.fail(err)
					return
				}
				allocs[lane.ID] = alloc
			}
			live = append(live, laneRead{lane: lane, alloc: alloc})
		}

		groups := partitionLanes(live, concurrency)
		r.deps.Log.Info("reading lanes",
			"node", id, "lanes", len(live), "concurrent_reads", len(groups),
			"declared", src.Caps.ReadConcurrency, "parallelism", r.p.spec.Parallelism)

		// The groups get a context this loop can cancel WITHOUT touching deliverCtx, so rebuilding
		// stops reading and never abandons a batch already on its way to a sink.
		//
		// WHAT HAPPENS TO THE INTERRUPTED READ DEPENDS ON THE SOURCE, and both outcomes are safe. A
		// source that returns promptly on cancellation has DRAINED — its records come back with
		// ctx.Err() and are admitted, because the read loop admits before it handles the error. One
		// that does not return in time is ABANDONED, and then its batches are untouchable while its
		// goroutine is still writing to them, so they are dropped unread.
		//
		// Nothing is lost either way, and for the same reason: an abandoned batch was never admitted,
		// so no cursor advanced past its records and they are read again. Saying only the first half —
		// which an earlier version of this comment did — claims a guarantee the abandoned path does
		// not give.
		gctx, stopGroups := context.WithCancel(ctx)
		done := make(chan struct{})
		var wg sync.WaitGroup
		for _, group := range groups {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r.readLaneGroup(gctx, deliverCtx, id, group)
			}()
		}
		go func() { wg.Wait(); close(done) }()

		next, rebuild := r.awaitLaneChange(ctx, rt.lanes, id, done, laneIDsOf(live))
		stopGroups()
		wg.Wait()
		if !rebuild {
			return
		}
		assigned = next
	}
}

// laneWatcher is the two methods awaitLaneChange needs from a lane table.
//
// NARROWED FOR A SEAM, and the seam is the point. The ORDER in which the two are called is the whole
// correctness of the loop below, and laneCtl is a concrete type — so with *sourceRuntime as the
// parameter there was no way to interleave an announce between the calls, and the ordering was held
// in place by an argument and nothing else. An interface lets a test supply an Assigned that
// announces as a side effect, which is exactly the window.
type laneWatcher interface {
	Changes() <-chan struct{}
	Assigned(context.Context) ([]connector.LaneAssignment, error)
}

// awaitLaneChange blocks until the read set needs rebuilding, the readers finish, or the run stops.
//
// IT REBUILDS ONLY FOR LANES NOBODY IS READING, which is what keeps this from thrashing. notify()
// fires on every announce AND every finish, and a finish is already handled inside the group that
// owned the lane: the batch carrying EndOfLane retires it and it leaves that group's read set.
// Rebuilding on a removal would cancel every other lane's in-flight read to learn something the
// reader had already acted on — for a bounded source finishing thirty chunks one at a time, thirty
// pointless restarts.
func (r *runner) awaitLaneChange(ctx context.Context, lanes laneWatcher, id record.NodeID,
	done <-chan struct{}, owned map[record.LaneID]bool,
) ([]connector.LaneAssignment, bool) {
	var readersDone bool
	for {
		// THE CHANNEL IS TAKEN BEFORE THE TABLE IS READ, and that ordering is this loop's whole
		// correctness. notify() CLOSES the channel it holds and installs a fresh one, so a channel
		// taken here is a promise to be woken for anything that happens after this line.
		//
		// Waiting first and reading after loses an announce permanently. The window is between the
		// Assigned() call and the next time round: a notify landing there closes a channel nobody is
		// holding any more, and the fresh channel this loop then waits on is not closed again until
		// the NEXT announce — which for a source that announces once is never. Taking it first turns
		// that window into an already-closed channel and one extra, harmless lap.
		//
		// PINNED, and it took a seam to pin. No fixture driving a real connector can aim at a window two
		// adjacent statements wide: reversing these lines leaves
		// TestALaneAnnouncedAfterReadingStartedIsRead green — measured, three runs — because that
		// source announces from inside ReadLanes, so the next read produces another notify and the lost
		// wake-up is recovered by it. That is why the parameter is [laneWatcher] rather than the
		// concrete table. A fake whose Assigned ANNOUNCES AS A SIDE EFFECT puts the notify exactly
		// here, whichever order the two calls are in, and reversing them then hangs the loop for good.
		// See TestAnAnnounceBetweenTakingTheChannelAndReadingTheTableIsNotLost.
		changed := lanes.Changes()

		next, err := lanes.Assigned(ctx)
		if err != nil {
			r.fail(err)
			return nil, false
		}
		if gained := added(next, owned); len(gained) > 0 {
			r.deps.Log.Info("picking up lanes announced since this source started reading",
				"node", id, "gained", len(gained), "reading", len(next))
			return next, true
		}
		// Nothing gained and nobody reading: every lane this source held is finished or revoked, and
		// the check above has just confirmed no late arrival is waiting. This is the ordinary end of a
		// bounded source.
		if readersDone {
			return nil, false
		}

		select {
		case <-ctx.Done():
			return nil, false
		case <-done:
			// One more lap rather than an immediate return, so a lane announced in the same moment the
			// last reader finished is still picked up — with the channel already taken above, that
			// check cannot race the announce either.
			readersDone = true
		case <-changed:
		}
	}
}

// added reports which of next's lanes nobody is reading yet.
func added(next []connector.LaneAssignment, owned map[record.LaneID]bool) []record.LaneID {
	var out []record.LaneID
	for _, a := range next {
		if !owned[a.ID] {
			out = append(out, a.ID)
		}
	}
	return out
}

// laneIDsOf indexes a read set by lane, for comparing against a fresh assignment.
func laneIDsOf(live []laneRead) map[record.LaneID]bool {
	out := make(map[record.LaneID]bool, len(live))
	for _, lr := range live {
		out[lr.lane.ID] = true
	}
	return out
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
func partitionLanes(lanes []laneRead, n int) [][]laneRead {
	if n < 1 {
		n = 1
	}
	if n > len(lanes) {
		n = len(lanes)
	}
	out := make([][]laneRead, 0, n)
	for i := range n {
		out = append(out, lanes[i*len(lanes)/n:(i+1)*len(lanes)/n])
	}
	return out
}

// readLaneGroup reads one disjoint group of lanes until every lane in it is finished or revoked, or
// the run stops.
func (r *runner) readLaneGroup(ctx, deliverCtx context.Context, id record.NodeID,
	group []laneRead,
) {
	src := r.p.sources[id]
	rt := r.sourceRT(id)

	// Resolved once, as on the one-lane path: neither can change during a run.
	policy := r.whenFullFor(id)
	clock := r.clockFor(id)

	// COPIED, because the loop below filters in place through live[:0] and the caller's slice is a
	// window into an array the supervisor owns.
	//
	// The windows partitionLanes hands out do not overlap, so filtering within one cannot reach
	// another group's lanes — but that safety is a property of how the partition happens to be cut,
	// and a group would have to know it to be sure. Copying costs one small allocation per rebuild
	// and removes the need to know.
	live := append(make([]laneRead, 0, len(group)), group...)

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
			//
			// A BATCH WITH NOTHING IN IT IS NOT ADMITTED. Here that is the common case rather than an
			// edge — a source multiplexing thirty lanes fills the two that had data and leaves the
			// other twenty-eight untouched every single call — and admitting an untouched one would
			// enter a ZERO position into that lane's prefix tracker, which becomes the lane's
			// resolved position once the prefix reaches it.
			said := b.Len() > 0 || !b.Position.IsZero() || b.EndOfLane
			progress = progress || said
			if !said {
				continue
			}
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
// once, and in the ledger, so the lane's final acknowledgement carries LaneFinished and the source
// learns the retirement became durable rather than inferring it from silence.
//
// THE WAIT FOR THE LANE'S ADMITTED GROUPS TO SETTLE IS THE LEDGER'S, not this function's. Both
// Ledger.FinishLane and record.Batch.EndOfLane say the engine must not tell a source its lane has
// finished while records are still in flight, and the engine cannot see when the last one settles —
// so the ledger holds the flag until its own in-flight count for the lane reaches zero. Calling
// this while work is outstanding is therefore correct and is what the EndOfLane path does.
//
// The DURABLE half does not wait, and that is a gap this branch does not close: a lane marked
// finished in the table opens its downstream gate immediately, so a StartAfter tail can begin
// before the scan it waits behind is fully settled. The ErrEndOfInput path has always done this.
func (r *runner) finishLane(ctx context.Context, rt *sourceRuntime, lane record.LaneID) {
	if err := rt.lanes.Finish(context.WithoutCancel(ctx), lane); err != nil {
		// A FENCED FINISH LOSES THE LANE, NOT THE PIPELINE, which is the blast radius store.Lease's
		// own doc fixes: "the loser's lane — not its whole process — is revoked".
		//
		// Finish became refusable when it started carrying the lane's lease epoch, and this call site
		// escalated every error to r.fail — so a lane reclaimed in the window between the read loop's
		// revocation check and its retirement would have taken the whole run down with it. Nothing
		// is lost by stopping here: the new holder reads the lane and retires it itself, and a
		// revoked lane is acknowledged by nobody, so the ledger's flag would have no ack to ride on.
		if fault.ClassOf(err) == fault.Fenced {
			r.deps.Log.Warn("not retiring a lane this worker no longer holds; its new holder will",
				"lane", lane, "error", err)
			return
		}
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
// THE SECOND AND NOT THE FIRST, because both contracts say so and because one is ordinary: a poll
// that timed out inside the source, a multiplexed reader whose one ready lane turned out to hold
// only filtered rows. Faulting on a single quiet return would make a contract violation out of a
// source that was merely idle for a moment.
//
// Only a read that RETURNED NIL is counted. A source draining on cancellation returns ctx.Err() and
// never reaches here, which is why shutdown cannot trip this.
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
