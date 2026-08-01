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
//
// Neither is a hidden gap: each is a component kind with no instances, and the negotiation refuses
// a pipeline that asks for one.
//
// Codecs used to be on this list. They are resolved now — see codec.go — which is what let the
// one-encoded-record-per-request fallback become the no-framer case rather than the only case.

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

func (r *runner) run(ctx context.Context) error {
	// The run context is cancelled to stop reading; the shutdown work that follows must NOT inherit
	// that cancellation, or the final flush is cancelled at exactly the moment it matters most.
	runCtx, stopReading := context.WithCancel(ctx)
	defer stopReading()

	r.lanes = map[record.NodeID]*laneCtl{}
	r.srcRT = map[record.NodeID]*sourceRuntime{}

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
	drainCtx, cancelDrain := context.WithTimeout(context.WithoutCancel(ctx), r.deps.GracePeriod)
	defer cancelDrain()
	drainErr := r.p.ledger.Drain(drainCtx)

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
			r.fail(fmt.Errorf("engine: source %s read: %w", id, err))
			return
		}

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
func buildRequests(ctx context.Context, sk *registry.ResolvedSink, cc *codecChain, batch *record.Batch) ([]request, error) {
	if sk.Structured != nil {
		req := &connector.Request{
			Count:   batch.Len(),
			Records: make([]record.Ref, 0, batch.Len()),
			Rows:    batch.Records,
		}
		for _, rec := range batch.Records {
			req.Records = append(req.Records, rec.Ref())
		}
		return []request{{req: req, records: batch.Records}}, nil
	}

	if cc.batches() {
		var body []byte
		refs := make([]record.Ref, 0, batch.Len())
		for _, rec := range batch.Records {
			var err error
			if body, err = cc.encode(ctx, body, rec); err != nil {
				return nil, fault.Permanent(fault.OpEncode, fmt.Errorf("encoding record %v: %w", rec.Origin().ID, err))
			}
			refs = append(refs, rec.Ref())
		}
		// The terminator closes the request rather than any single record — a JSON array's "]".
		// Appended once, after the last frame, exactly as the Framer contract says.
		body = append(body, cc.framer.Terminator()...)
		req, err := finish(cc, body, refs, batch.Len())
		if err != nil {
			return nil, err
		}
		return []request{{req: req, records: batch.Records}}, nil
	}

	out := make([]request, 0, batch.Len())
	for _, rec := range batch.Records {
		body, err := cc.encode(ctx, nil, rec)
		if err != nil {
			return nil, fault.Permanent(fault.OpEncode, fmt.Errorf("encoding record %v: %w", rec.Origin().ID, err))
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

// deliver writes one batch to every sink and settles the outcomes.
func (r *runner) deliver(ctx context.Context, batch *record.Batch) error {
	for id, sk := range r.p.sinks {
		reqs, err := buildRequests(ctx, sk, r.p.codecs[id], batch)
		if err != nil {
			return fmt.Errorf("engine: encoding for sink %s: %w", id, err)
		}
		for _, rq := range reqs {
			res, err := sandbox(ctx, sk.Name, rq.req,
				func(c context.Context, q *connector.Request) (connector.WriteResult, error) {
					return sk.Sink.Write(c, q)
				})
			if err != nil {
				return fmt.Errorf("engine: sink %s write: %w", id, err)
			}

			// A sink that under-reports is a sink whose success cannot be trusted, and settling on
			// it would advance the source past records nobody wrote.
			if ok, want := res.Reconcile(rq.req.Count); !ok {
				return fault.Contract(fault.OpWrite, fmt.Errorf(
					"engine: sink %s accounted for %d of %d records; a WriteResult must name every record it did not write",
					id, want, rq.req.Count))
			}

			r.p.ledger.Settle(outcomesFor(id, rq.records, res))
		}
	}
	return nil
}

// outcomesFor turns one WriteResult into the ledger's outcomes.
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
