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
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
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
	r := &runner{p: p, deps: p.deps}
	return r.run(ctx)
}

// runner holds one execution of a pipeline.
type runner struct {
	p    *Pipeline
	deps Deps

	// lanes and srcRT are kept so shutdown can reach the lane table for the final commit, and so the
	// commit pump can find the source that owns an acknowledged lane.
	lanes map[record.NodeID]*laneCtl
	srcRT map[record.NodeID]*sourceRuntime

	// mainSinks and failedSinks are the sink nodes split by what their inbound edges carry, computed
	// once at start because it is a property of the spec and cannot change during a run.
	mainSinks   []record.NodeID
	failedSinks []record.NodeID

	mu       sync.Mutex
	firstErr error

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

	for _, lc := range r.lanes {
		lc.mu.Lock()
		ids := make([]record.LaneID, 0, len(lc.lanes))
		done := make(map[record.LaneID]bool, len(lc.lanes))
		for id, rec := range lc.lanes {
			ids = append(ids, id)
			done[id] = rec.Finished
		}
		lc.mu.Unlock()

		for _, id := range ids {
			// A FINISHED LANE IS NOT A STALLED ONE. Its cursor is final and durable, so an age that
			// keeps climbing would page somebody about a bounded pipeline that completed exactly as
			// intended. Forgetting the series is what omit-don't-zero means for a quantity that has
			// stopped existing rather than one that was never measured.
			if done[id] {
				o.checkpointAge.Forget(o.pipeline, laneLabel(id))
				o.inFlight.Forget(o.pipeline, laneLabel(id))
				o.inFlightMax.Forget(o.pipeline, laneLabel(id))
				o.replay.Forget(o.pipeline, laneLabel(id))
				continue
			}
			r.mu.Lock()
			at, ok := r.lastCheckpoint[id]
			if !ok {
				// Never checkpointed. Age from the start of the run, which is the honest answer and
				// the one that alarms.
				at = r.started
				r.lastCheckpoint[id] = at
			}
			r.mu.Unlock()
			o.checkpointAge.Set(now.Sub(at).Seconds(), o.pipeline, laneLabel(id))

			st := r.p.ledger.Stats(id)
			o.inFlight.Set(float64(st.InFlight), o.pipeline, laneLabel(id))
			o.inFlightMax.Set(float64(st.InFlightBudget), o.pipeline, laneLabel(id))
			o.replay.Set(float64(st.ReplayRecords), o.pipeline, laneLabel(id))
		}
	}
}

func (r *runner) fail(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	if r.firstErr == nil {
		r.firstErr = err
	}
	r.mu.Unlock()
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

	r.lanes = map[record.NodeID]*laneCtl{}
	r.srcRT = map[record.NodeID]*sourceRuntime{}
	r.lastCheckpoint = map[record.LaneID]time.Time{}
	r.started = time.Now()
	r.lastPersist = r.started
	r.mainSinks, r.failedSinks = partitionSinks(r.p.spec, r.p.sinks)

	// Sinks open before sources. A source that produces before its sink can accept is a batch with
	// nowhere to go, held against the lane budget for nothing.
	if err := r.openSinks(runCtx); err != nil {
		return err
	}
	if err := r.openSources(runCtx); err != nil {
		return err
	}

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

	// Drain: settlement continues while the ledger empties. A graceful stop must not discard a
	// commit that is one millisecond from safe.
	//
	// A FAILING RUN IS NOT DRAINED, and that is a deliberate exception rather than an oversight. The
	// records a fault-stop left in flight are held by a write that gave up, so nothing will ever
	// present them again and the ledger can never empty: draining is then a guaranteed wait of the
	// whole grace period — thirty seconds by default — for an outcome that is already known. It is
	// also not free of consequence, so it is logged and the leak is reported below: those records
	// replay on restart, which at-least-once permits and an operator should be told about.
	var drainErr error
	if r.firstError() != nil {
		r.deps.Log.Warn("the pipeline is stopping on a fault, so it is not draining; in-flight records will replay",
			"pipeline", r.p.spec.ID)
	} else {
		drainCtx, cancelDrain := context.WithTimeout(context.WithoutCancel(ctx), r.deps.GracePeriod)
		defer cancelDrain()
		drainErr = r.p.ledger.Drain(drainCtx)
	}

	// One final flush, so anything that settled during the drain is durable before we stop.
	r.flushOnce(context.WithoutCancel(ctx))

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
	close(flushStop)
	<-flushDone
	_ = r.p.ledger.Close() // closing the ack channel is what ends the pump
	<-pumpDone
	wg.Wait()

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

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.firstErr
}

// openSources constructs a runtime per source and opens it.
func (r *runner) openSources(ctx context.Context) error {
	for id := range r.p.sources {
		lc := newLaneCtl(r.deps, r.specFields(), id,
			func(lane record.LaneID, ord connector.Ordering, budget int) error {
				return r.p.ledger.Lane(lane, ord, budget)
			},
			func(lane record.LaneID) connector.Admission {
				st := r.p.ledger.Stats(lane)
				return connector.Admission{
					Budget:   int(st.InFlightBudget),
					Headroom: int(st.InFlightBudget) - int(st.InFlight),
				}
			})
		if err := lc.load(ctx); err != nil {
			return err
		}
		r.lanes[id] = lc

		rt := &sourceRuntime{
			baseRuntime: baseRuntime{
				ctx: ctx, deps: r.deps,
				tenant: r.p.spec.Tenant, pipeline: r.p.spec.ID, node: id,
				streams: configuredStreams(r.p.spec),
			},
			lanes: lc,
			state: &stateHandle{deps: r.deps, tenant: r.p.spec.Tenant, pipeline: r.p.spec.ID, node: id},
		}

		// Every call into a connector goes through the sandbox, so a panic is a classified fault and
		// a hang is abandoned rather than wedging the host.
		src := r.p.sources[id]
		if _, err := sandbox(ctx, r.p.obs, id, src.Name, rt,
			func(c context.Context, rt *sourceRuntime) (struct{}, error) {
				return struct{}{}, src.Source.Open(c, rt)
			}); err != nil {
			return fmt.Errorf("engine: opening source %s: %w", id, err)
		}
		r.srcRT[id] = rt
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
		}}
		opening := connector.Opening{
			Guarantee: r.p.negotiated.Guarantee,
			Streams:   configuredStreams(r.p.spec),
		}
		if _, err := sandbox(ctx, r.p.obs, id, sk.Name, opening,
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
		if _, err := sandbox(ctx, r.p.obs, id, sk.Name, rt,
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
	rt := r.srcRT[id]

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
		_, err := sandbox(ctx, r.p.obs, id, src.Name, batch,
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

		r.p.obs.recordsRead.Add(float64(batch.Len()), r.p.obs.pipeline, laneLabel(lane.ID), src.Name)

		if err := r.p.ledger.Admit(ctx, batch); err != nil {
			if ctx.Err() != nil {
				return
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
		if err := r.writeSet(ctx, id, batch.Records); err != nil {
			return err
		}
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
			r.flushOnce(context.WithoutCancel(ctx))
			// Unconditionally, even when nothing was flushable: a stalled pipeline produces no
			// flushes, and that is precisely when its checkpoint age has to keep climbing.
			r.refreshGauges()
		}
	}
}

// flushOnce performs one phase-two write.
func (r *runner) flushOnce(ctx context.Context) {
	flushable := r.p.ledger.Flushable()
	if len(flushable) == 0 {
		return
	}

	for node, lc := range r.lanes {
		mine := map[record.LaneID]record.Position{}
		lc.mu.Lock()
		for lane, pos := range flushable {
			if _, ok := lc.lanes[lane]; ok {
				mine[lane] = pos
			}
		}
		lc.mu.Unlock()
		if len(mine) == 0 {
			continue
		}
		start := time.Now()
		if err := lc.commit(ctx, mine); err != nil {
			r.fail(fmt.Errorf("engine: persisting lane cursors for %s: %w", node, err))
			return
		}
		r.p.obs.commitLatency.ObserveSince(start, r.p.obs.pipeline, telemetry.CommitPhasePersist)
		r.markCheckpointed(mine)
		// ONLY NOW may the ledger emit acknowledgements for these positions. Calling Committed
		// before the write returned would tell a source to advance past data canal has no durable
		// record of, which is design rule R4's original violation.
		r.p.ledger.Committed(mine)
	}
}

// commitPump is phase three: it hands durable acknowledgements to sources.
func (r *runner) commitPump(ctx context.Context) {
	for ack := range r.p.ledger.Acks() {
		src, srcNode, ok := r.sourceFor(ack.Lane)
		if !ok {
			continue
		}
		start := time.Now()
		if _, err := sandbox(ctx, r.p.obs, srcNode, src.Name, ack,
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
	for id, lc := range r.lanes {
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
