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
	wg.Add(1)
	go func() {
		defer wg.Done()
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

	close(flushStop)
	_ = r.p.ledger.Close() // closing the ack channel is what ends the pump
	<-pumpDone
	wg.Wait()

	if leaks := r.p.ledger.Leaks(); len(leaks) > 0 {
		// A drain that did not complete is a DIFFERENT event from a completed drain, because it
		// means records may replay. Naming it is the difference between an operator knowing and
		// guessing.
		r.deps.Log.Warn("drain did not settle every group; records in these may replay on restart",
			"pipeline", r.p.spec.ID, "groups", len(leaks))
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
		if _, err := sandbox(ctx, src.Name, rt,
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
			streams: configuredStreams(r.p.spec),
		}}
		opening := connector.Opening{
			Guarantee: r.p.negotiated.Guarantee,
			Streams:   configuredStreams(r.p.spec),
		}
		if _, err := sandbox(ctx, sk.Name, opening,
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
		if _, err := sandbox(ctx, sk.Name, rt,
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
		_, err := sandbox(ctx, src.Name, batch,
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
			rt := route(err, false, r.p.spec.Retry, &readAttempt, time.Now())
			if rt.disp != dispRetry {
				r.fail(fmt.Errorf("engine: source %s read (%s after %d attempts): %w",
					id, rt.reason, readAttempt.count, err))
				return
			}
			r.deps.Log.Warn("retrying a read",
				"node", id, "wait", rt.delay, "attempt", readAttempt.count,
				"class", fault.ClassOf(err), "error", err)
			if serr := sleep(ctx, rt.delay); serr != nil {
				return
			}
			continue
		}
		readAttempt = attempt{}

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
		if err := lc.commit(ctx, mine); err != nil {
			r.fail(fmt.Errorf("engine: persisting lane cursors for %s: %w", node, err))
			return
		}
		// ONLY NOW may the ledger emit acknowledgements for these positions. Calling Committed
		// before the write returned would tell a source to advance past data canal has no durable
		// record of, which is design rule R4's original violation.
		r.p.ledger.Committed(mine)
	}
}

// commitPump is phase three: it hands durable acknowledgements to sources.
func (r *runner) commitPump(ctx context.Context) {
	for ack := range r.p.ledger.Acks() {
		src, ok := r.sourceFor(ack.Lane)
		if !ok {
			continue
		}
		if _, err := sandbox(ctx, src.Name, ack,
			func(c context.Context, a connector.Ack) (struct{}, error) {
				return struct{}{}, src.Source.Commit(c, a)
			}); err != nil {
			// A source that refuses an acknowledgement has not lost data: the position stays durable
			// and the next acknowledgement carries it again. Logged rather than fatal.
			r.deps.Log.Warn("source refused an acknowledgement; its upstream position was not advanced",
				"lane", ack.Lane, "error", err)
		}
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
func (r *runner) sourceFor(lane record.LaneID) (*registry.ResolvedSource, bool) {
	for id, lc := range r.lanes {
		lc.mu.Lock()
		_, ok := lc.lanes[lane]
		lc.mu.Unlock()
		if ok {
			return r.p.sources[id], true
		}
	}
	return nil, false
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
