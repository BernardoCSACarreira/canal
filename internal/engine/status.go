package engine

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// THE READ MODEL, PRODUCED.
//
// [telemetry.PipelineStatus] has been a declared shape with no producer since it was written: the
// architecture describes the document, the frontend contract test marshals it, and nothing in the
// module ever constructed one. A type nothing builds is a design sketch, and the operator question
// it exists to answer — "is this pipeline healthy, and did my config take effect" — had no answer at
// all.
//
// THE RULE THIS FILE OBEYS IS THE DOCUMENT'S OWN: every unknown is a nil pointer, never a zero. A
// field the engine cannot measure is left nil and named as a gap below, because a confident zero is
// worse than an absence — a UI renders "0 records/s" as a fact and "unknown" as a question.
//
// What this build cannot answer, and why, so the omissions are legible rather than accidental:
//
//   - PipelineStatus.Config. The redacted tree needs each component's declared config.Spec to know
//     which fields are secret, and Build does not keep the redaction it computes. Emitting the raw
//     tree instead is precisely the leak config.Redacted exists to prevent, so nothing is emitted.
//   - LaneStatus.Progress. record.Fraction needs a lane's scalar BOUNDS and a lane declares only its
//     current Scalar, so there is nothing to divide by.
//   - NodeStatus.Utilization and .BlockedForSeconds. Neither is measured anywhere in the engine.
//   - Buffers. There is no buffer node type.
//
// Generation used to be on that list in all but name — it was ObservedGeneration's own value,
// reported twice — and it is not any more: config.go watches store.ConfigStore for the revision this
// process has NOT applied.

// statusState is the read model's own state on a runner: the phase, the last computed condition set
// and the samples a rate is differenced from.
//
// It is separate from the metric instruments deliberately. Instruments are write-only — a counter
// cannot be read back — and a status document needs the values, not the exports.
type statusState struct {
	mu sync.Mutex

	phase       telemetry.Phase
	sourceReady bool
	sinkReady   bool

	// stoppingSince and drainDeadline are only meaningful in PhaseDraining, which is what makes them
	// pointers on the document.
	stoppingSince time.Time
	drainDeadline time.Time

	// conditions is the previous computation, kept so LastTransitionTime is the time the status LAST
	// CHANGED rather than the time it was last looked at. A transition time that moves on every
	// scrape is not a transition time.
	conditions map[telemetry.ConditionType]telemetry.Condition

	// The rate window. A rate needs two samples, so both rates stay nil until the second one — an
	// average taken over a pipeline's whole life is not the number an operator reading "records per
	// second" believes they are reading.
	sampledAt time.Time
	sampledIn uint64
	sampledOu uint64
	rateIn    *float64
	rateOut   *float64
}

// nodeTally is the per-node accounting the read model needs and the metric label sets cannot give
// back: canal_records_read_total is labelled by LANE, and summing a counter family over a label is
// something a scrape does, not something a process can ask its own registry for.
//
// One instance per node, allocated at Build from the spec graph, so the map is read-only once the
// run starts and the counters are plain atomics on a hot path.
type nodeTally struct {
	in     atomic.Uint64
	out    atomic.Uint64
	faults atomic.Uint64

	// backoffNanos is time, not attempts, for the same reason MBackoff is.
	backoffNanos atomic.Uint64
}

// Status materialises the read model for this pipeline.
//
// It is safe to call at any time, including before Run and after it returns — a completed bounded
// pipeline still has to be able to say what it did, and a pipeline that has never started reports
// [telemetry.PhasePending] rather than an empty document.
func (p *Pipeline) Status(q telemetry.StatusQuery) telemetry.PipelineStatus {
	now := time.Now()

	// LOADED ONCE AND THREADED THROUGH, for the same reason refreshGauges and the document are built
	// from one pass over the lanes: two loads of an atomic that another goroutine is publishing to can
	// return different observations, and Generation and the spec_applied message are computed from
	// opposite ends of this function. A document reading "generation 2" beside "revision 1 is stored"
	// contradicts itself in exactly the field pair the config watch exists to produce.
	cfg := p.configView()

	s := telemetry.PipelineStatus{
		Tenant:   p.spec.Tenant,
		Pipeline: p.spec.ID,

		// TWO REVISIONS AT LAST. Generation is what the control plane stores and ObservedGeneration is
		// what this process runs, and the config watch is what makes the first of them a number this
		// worker did not pick itself. A worker with no store.ConfigStore still reports one revision
		// twice — the spec it loaded is the only one that exists — but it now does so because that is
		// the answer, not because there was nowhere else to look. See config.go.
		Generation:         cfg.generation(p.spec.Revision),
		ObservedGeneration: p.spec.Revision,

		AsOf:    now,
		Version: p.version.Add(1),

		// One worker reporting on itself has heard from every worker there is.
		Complete: true,

		Negotiated: p.negotiated,
	}

	// A SINGLE WORKER REPORTING ON ITSELF HAS NO STALENESS THRESHOLD, and nil says so rather than
	// inventing a number. The field starts meaning something when store.StatusStore.Aggregate exists
	// and has to decide whose last report still counts.
	s.StaleAfterSeconds = nil

	r := p.active.Load()
	if r == nil {
		s.Phase = telemetry.PhasePending
		s.Conditions = pendingConditions(now, p, cfg)
		return s
	}
	r.fillStatus(&s, now, q, cfg)
	return s
}

// pendingConditions is the condition set for a pipeline that has not run.
//
// Everything the run would establish is UNKNOWN rather than false. "Not connected" is a claim about
// a connection that was attempted; a pipeline that has not started has attempted nothing, and
// reporting false would make a never-started pipeline indistinguishable from a broken one.
func pendingConditions(now time.Time, p *Pipeline, cfg *configView) []telemetry.Condition {
	gen := p.spec.Revision
	out := make([]telemetry.Condition, 0, len(telemetry.ConditionTypes))
	for _, t := range telemetry.ConditionTypes {
		c := telemetry.Condition{
			Type: t, Status: telemetry.StatusUnknown, Reason: telemetry.ReasonStarting,
			Message: "the pipeline has not started", LastTransitionTime: now, ObservedGeneration: gen,
		}
		switch t {
		case telemetry.CondConfigured:
			c.Status, c.Reason = telemetry.StatusTrue, telemetry.ReasonApplied
			c.Message = "the spec was validated and negotiated"

		// SPEC APPLIED IS THE ONE CONDITION A NOT-YET-STARTED PIPELINE CAN STILL ANSWER, because the
		// config watch does not depend on anything Run establishes. It answers true for a worker with
		// no config store and unknown for one whose store has not been read yet, which is what keeps
		// "built and never started" from claiming its config landed on the strength of never looking.
		case telemetry.CondSpecApplied:
			applied := configCondition(p.deps.Config != nil, cfg, gen)
			c.Status, c.Reason, c.Message = applied.Status, applied.Reason, applied.Message
		}
		out = append(out, c)
	}
	return out
}

// fillStatus completes a document from live runner state.
func (r *runner) fillStatus(s *telemetry.PipelineStatus, now time.Time, q telemetry.StatusQuery,
	cfg *configView,
) {
	r.status.mu.Lock()
	phase := r.status.phase
	stopping, deadline := r.status.stoppingSince, r.status.drainDeadline
	r.status.mu.Unlock()

	s.Phase = phase
	if phase == telemetry.PhaseDraining && !stopping.IsZero() {
		st, dl := stopping, deadline
		s.StoppingSince = &st
		if !dl.IsZero() {
			s.DrainDeadline = &dl
		}
	}

	lanes, facts := r.laneStatuses(now)

	// THE ROLLUP IS COMPUTED FROM EVERY LANE, before any filtering or paging. A per-stream summary
	// that only summarised the page would be worse than none: it would look like an answer and be a
	// statement about an arbitrary subset.
	s.Streams = rollUpStreams(lanes)
	s.LaneCount = len(lanes)

	lanes, s.LanesCursor, s.LanesTruncated = pageLanes(lanes, q)
	s.Lanes = lanes
	s.Nodes = r.nodeStatuses(facts)
	s.Scan = r.scanProgress(now, facts)
	// LeaseExpires is the SOONEST of this worker's leases, or nil when there is no coordinator to
	// have granted one. Leader is what this process believes, which is advisory by construction —
	// store.Leadership says so — and true by default for a standalone run that is the only planner
	// there is.
	s.Workers = []telemetry.WorkerStatus{{
		ID: string(r.deps.Worker), Since: r.started, Leader: r.isLeader(),
		Lanes: len(lanes), LastHeard: now, LeaseExpires: r.leases.soonestExpiry(),
	}}
	s.Throughput = r.throughput(now, facts)
	s.RecentEvents = r.recentEvents()
	s.LastFault = r.lastFault()
	s.Conditions = r.conditions(now, facts, cfg)
}

// maxLanesPerDocument is the DEFAULT page, not a hard cap: a caller that wants more asks for more,
// and a caller that asks for nothing gets one page rather than a source's whole scan-chunk table.
const maxLanesPerDocument = 200

// pageLanes applies a StatusQuery to the full, sorted lane list.
//
// KEYSET, NOT OFFSET. The cursor is the id of the last lane on the page and the next page is
// everything after it, so a lane finishing between two reads shifts nothing: an offset would skip a
// row every time the list shrank behind the reader. The list is sorted by id in laneStatuses, which
// is what makes this well defined.
//
// The cursor is documented as opaque so this can become an index, a shard key or a per-worker
// fan-out token later without a wire change.
func pageLanes(all []telemetry.LaneStatus, q telemetry.StatusQuery) (page []telemetry.LaneStatus,
	cursor string, truncated bool,
) {
	if q.Stream != "" {
		kept := make([]telemetry.LaneStatus, 0, len(all))
		for _, l := range all {
			if l.Stream == string(q.Stream) {
				kept = append(kept, l)
			}
		}
		all = kept
	}
	if q.LaneCursor != "" {
		// Binary search rather than a scan: the list is sorted by id, and walking it per page makes
		// paging the whole table quadratic — 145 pages over 29,000 lanes is four million comparisons
		// to hand back what a search finds in fifteen. The scale this field exists for is the scale
		// that makes the difference matter.
		i := sort.Search(len(all), func(i int) bool { return string(all[i].ID) > q.LaneCursor })
		all = all[i:]
	}

	limit := maxLanesPerDocument
	if q.LaneLimit != nil {
		limit = *q.LaneLimit
	}
	if limit < 0 {
		limit = 0
	}
	if len(all) <= limit {
		return all, "", false
	}

	// A SILENTLY SHORT ARRAY IS THE SAME LIE AS AN OMITTED WORKER, so the cut is announced — and now
	// it is also followable, which is what LanesTruncated used to admit it was not.
	page = all[:limit]
	truncated = true
	if limit > 0 {
		cursor = string(page[limit-1].ID)
	}
	// limit == 0 leaves the cursor empty, which is correct rather than a gap: an empty cursor means
	// START FROM THE BEGINNING, and a caller that asked for no lanes has not consumed any. It is
	// LanesTruncated, not the cursor, that says whether more exist.
	return page, cursor, truncated
}

// rollUpStreams summarises every lane by stream.
//
// Sorted by stream name so two reads of an unchanged pipeline produce the same document.
func rollUpStreams(lanes []telemetry.LaneStatus) []telemetry.StreamStatus {
	if len(lanes) == 0 {
		return nil
	}
	type acc struct {
		st        telemetry.StreamStatus
		backlog   telemetry.Backlog
		haveAll   bool
		anyAnswer bool
	}
	byStream := map[string]*acc{}
	order := make([]string, 0, 8)

	for i := range lanes {
		l := &lanes[i]
		a, ok := byStream[l.Stream]
		if !ok {
			a = &acc{st: telemetry.StreamStatus{Stream: record.StreamName(l.Stream)}, haveAll: true}
			byStream[l.Stream] = a
			order = append(order, l.Stream)
		}
		a.st.Lanes++
		if l.Finished {
			a.st.LanesFinished++
		}
		if l.Blocked {
			a.st.LanesBlocked++
		}
		if l.Idle {
			a.st.LanesIdle++
		}
		a.st.RecordsRead += l.RecordsRead
		a.st.RecordsCommitted += l.RecordsCommitted
		a.st.RecordsAbandoned += l.RecordsAbandoned
		a.st.InFlight += l.InFlight

		// MAX, not mean. The alert on a stream is its worst lane, and an average hides one stuck
		// chunk behind thirty-one healthy ones.
		if l.CheckpointAge != nil && (a.st.MaxCheckpointAge == nil || *l.CheckpointAge > *a.st.MaxCheckpointAge) {
			v := *l.CheckpointAge
			a.st.MaxCheckpointAge = &v
		}
		if l.EventTimeLag != nil && (a.st.MaxEventTimeLag == nil || *l.EventTimeLag > *a.st.MaxEventTimeLag) {
			v := *l.EventTimeLag
			a.st.MaxEventTimeLag = &v
		}

		// A SUM OVER SOME OF THE LANES IS NOT A BACKLOG. One lane that cannot answer makes the stream's
		// total unknown, because a partial sum reads as a small backlog rather than as an absent one —
		// and of the two mistakes that is the dangerous one.
		if l.Backlog == nil {
			a.haveAll = false
			continue
		}
		a.anyAnswer = true
		if l.Backlog.Records != nil && a.backlog.Records == nil {
			var z uint64
			a.backlog.Records = &z
		}
		if l.Backlog.Records != nil {
			*a.backlog.Records += *l.Backlog.Records
		} else {
			a.haveAll = false
		}
		if l.Backlog.Bytes != nil {
			if a.backlog.Bytes == nil {
				var z uint64
				a.backlog.Bytes = &z
			}
			*a.backlog.Bytes += *l.Backlog.Bytes
		}
		// Exact only if every contributing lane was exact, and AsOf is the OLDEST reading, because a
		// sum is only as fresh as its stalest term.
		if a.st.Lanes == 1 || a.backlog.AsOf.After(l.Backlog.AsOf) {
			a.backlog.AsOf = l.Backlog.AsOf
		}
		a.backlog.Exact = a.backlog.Exact || a.st.Lanes == 1
		if !l.Backlog.Exact {
			a.backlog.Exact = false
		}
	}

	sort.Strings(order)
	out := make([]telemetry.StreamStatus, 0, len(order))
	for _, name := range order {
		a := byStream[name]
		if a.haveAll && a.anyAnswer {
			b := a.backlog
			a.st.Backlog = &b
		}
		out = append(out, a.st)
	}
	return out
}

// laneFacts is what one pass over the lanes learned, so the conditions, the node rollup, the scan
// bar and the rates are all computed from ONE observation rather than from four that disagree.
type laneFacts struct {
	total     int
	finished  int
	blocked   int
	idle      int
	inFlight  uint64
	admitted  uint64
	settled   uint64
	abandoned uint64

	// lastAdvance is the most recent moment ANY lane's durable cursor moved. Progressing is a
	// statement about canal's own record of progress, so it is derived from this and nothing else.
	lastAdvance time.Time

	// caughtUp is true when every unfinished lane has nothing in flight and its durable cursor has
	// reached its delivered prefix.
	caughtUp bool

	// perNode is each source node's admitted and settled totals, for the node rollup.
	perNode map[record.NodeID][2]uint64

	// scanTotal and scanDone are lane counts; scanWeight and scanWeightDone are the declared record
	// weights, which are what a fraction may be computed from.
	scanTotal, scanDone          int
	scanWeight, scanWeightDone   uint64
	everyScanLaneDeclaredAWeight bool
}

// laneStatuses projects every lane the runner holds, and collects the facts every other projection
// needs on the same pass.
func (r *runner) laneStatuses(now time.Time) ([]telemetry.LaneStatus, laneFacts) {
	f := laneFacts{
		caughtUp:                     true,
		perNode:                      map[record.NodeID][2]uint64{},
		everyScanLaneDeclaredAWeight: true,
	}
	var out []telemetry.LaneStatus

	for node, lc := range r.laneCtls() {
		lc.mu.Lock()
		ids := make([]record.LaneID, len(lc.order))
		copy(ids, lc.order)
		recs := make(map[record.LaneID]laneRecord, len(ids))
		gated := make(map[record.LaneID][]record.LaneGroup, len(ids))
		for _, id := range ids {
			if rec, ok := lc.lanes[id]; ok {
				recs[id] = *rec
				gated[id] = lc.gatedOnLocked(rec)
			}
		}
		lc.mu.Unlock()

		for _, id := range ids {
			rec, ok := recs[id]
			if !ok {
				continue
			}
			st := r.p.ledger.Stats(id)

			f.total++
			f.admitted += st.RecordsRead
			f.settled += st.Settled
			f.abandoned += st.AbandonedTotal
			f.inFlight += st.InFlight
			agg := f.perNode[node]
			f.perNode[node] = [2]uint64{agg[0] + st.RecordsRead, agg[1] + st.Settled}

			ls := telemetry.LaneStatus{
				ID:     id,
				Name:   rec.Spec.Name,
				Stream: string(rec.Spec.Stream),
				Kind:   rec.Spec.Kind.String(),
				Group:  string(rec.Spec.Group),
				Label:  rec.Spec.Label,
				Worker: string(r.deps.Worker),

				RecordsRead:      st.RecordsRead,
				RecordsCommitted: st.RecordsCommitted,
				RecordsAbandoned: st.AbandonedTotal,
				InFlight:         st.InFlight,
				InFlightBudget:   st.InFlightBudget,
				ReplayRecords:    st.ReplayRecords,
				Blocked:          st.Blocked,
				Finished:         rec.Finished,
			}
			for _, g := range gated[id] {
				ls.GatedOn = append(ls.GatedOn, string(g))
			}
			if st.Blocked {
				f.blocked++
				secs := st.BlockedFor.Seconds()
				ls.BlockedFor = &secs
			}
			if st.OldestPendingAge > 0 {
				secs := st.OldestPendingAge.Seconds()
				ls.OldestPendingAge = &secs
			}

			// TWO FIELDS FOR TWO FACTS. Position is the DURABLE cursor's label and Resolved is the
			// delivered prefix's, and the difference between them is the whole point: a delivered
			// prefix is not progress until it has been persisted.
			//
			// The string is connector-authored and rendered verbatim — "binlog.000042:1273",
			// "lsn 0/1A2B3C4", "byte 55" — which is how a UI shows a meaningful position for an
			// arbitrary connector with no connector-specific code. The core never parses it. A
			// connector that supplies none leaves the field nil, which renders as unknown rather than
			// as a position of zero.
			if st.CommittedOK && st.Committed.Label != "" {
				lbl := st.Committed.Label
				ls.Position = &lbl
			}
			if st.ResolvedOK && st.Resolved.Label != "" {
				lbl := st.Resolved.Label
				ls.Resolved = &lbl
			}

			// WHAT THE SOURCE REPORTED, through the control goroutine. Idle is true only when a
			// heartbeat was actually DELIVERED, which is what keeps it meaning "the source says this
			// lane is quiet" rather than "the core cannot see anything happening" — a stuck lane is
			// also quiet, and only one of the two is healthy. An idle lane's CheckpointAge is still
			// reported truthfully; Idle is what tells an alert rule to ignore it.
			idleSince, backlog, reportedLag := r.activity.observed(id)
			if !idleSince.IsZero() {
				ls.Idle = true
				since := idleSince
				ls.IdleSince = &since
				f.idle++
			}
			ls.Backlog = backlog

			// The SOURCE'S OWN event-time lag wins, because it knows what it has not read yet and the
			// core does not. Falling back to differencing the committed position's observation time is
			// still a real lag, just a weaker one: it says how old the newest thing canal has made
			// durable is, not how far behind the newest thing that exists.
			switch {
			case reportedLag != nil:
				ls.EventTimeLag = reportedLag
			case st.CommittedOK && !st.Committed.At.IsZero():
				lag := now.Sub(st.Committed.At).Seconds()
				ls.EventTimeLag = &lag
			}

			if rec.Finished {
				f.finished++
			} else {
				// A FINISHED LANE IS NOT A STALLED ONE and it is not an uncaught-up one either: its
				// cursor is final and durable, which is the definition of done rather than behind.
				if st.InFlight > 0 || !samePosition(st.Resolved, st.ResolvedOK, st.Committed, st.CommittedOK) {
					f.caughtUp = false
				}
			}

			// CheckpointAge derives from the DURABLE cursor and from nothing else, and it is reported
			// for a finished lane too — the document says when this lane last moved, and a renderer
			// suppresses the alert from Finished rather than from a missing number.
			r.mu.Lock()
			at, seen := r.lastCheckpoint[id]
			r.mu.Unlock()
			if !seen {
				at = r.started
			} else if at.After(f.lastAdvance) {
				f.lastAdvance = at
			}
			age := now.Sub(at).Seconds()
			ls.CheckpointAge = &age
			if seen {
				committed := at
				ls.CommittedAt = &committed
			}

			if rec.Spec.Kind == connector.LaneKindScan {
				f.scanTotal++
				f.scanWeight += rec.Spec.Weight
				if rec.Spec.Weight == 0 {
					f.everyScanLaneDeclaredAWeight = false
				}
				if rec.Finished {
					f.scanDone++
					f.scanWeightDone += rec.Spec.Weight
				}
			}

			out = append(out, ls)
		}
	}

	// Sorted so two scrapes of an unchanged pipeline produce the same document. The lane map has no
	// order, and a document that reshuffles on every read is one no consumer can diff.
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, f
}

// samePosition reports whether the delivered prefix has been made durable.
//
// Unknown on either side means NOT the same: a lane whose resolver has an answer and whose cursor
// does not is behind, and one with no answer at all has nothing to be caught up with.
func samePosition(a record.Position, aok bool, b record.Position, bok bool) bool {
	if !aok {
		return true // nothing has resolved, so nothing is outstanding
	}
	if !bok {
		return false
	}
	if cmp, ok := a.Compare(b); ok {
		return cmp == 0
	}
	// No comparable encoding. Seq is core-assigned within a generation and both of these came from
	// this generation, so it is a valid comparison here even though it is not a durable one.
	return a.Seq == b.Seq
}

// nodeStatuses projects the graph. Every node in the spec appears, including one that never opened,
// because a missing row and a broken node look identical in a renderer.
func (r *runner) nodeStatuses(f laneFacts) []telemetry.NodeStatus {
	out := make([]telemetry.NodeStatus, 0, len(r.p.spec.Graph))
	for _, n := range r.p.spec.Graph {
		ns := telemetry.NodeStatus{
			ID: n.ID, Kind: string(n.Kind), Name: n.Name, Label: n.Label,
		}
		switch n.Kind {
		case registry.KindSource:
			ns.Connected = r.sourceRT(n.ID) != nil
			if agg, ok := f.perNode[n.ID]; ok {
				ns.RecordsIn, ns.RecordsOut = agg[0], agg[1]
			}
		case registry.KindSink:
			_, ns.Connected = r.p.sinks[n.ID]
			if t := r.p.obs.tallyFor(n.ID); t != nil {
				ns.RecordsIn, ns.RecordsOut = t.in.Load(), t.out.Load()
			}
		}
		if t := r.p.obs.tallyFor(n.ID); t != nil {
			ns.Faults = t.faults.Load()
			if nanos := t.backoffNanos.Load(); nanos > 0 {
				secs := time.Duration(nanos).Seconds()
				ns.BackoffSeconds = &secs
			}
		}
		out = append(out, ns)
	}
	return out
}

// scanProgress summarises every scan lane, for any source, with no connector code — which is the
// claim the architecture makes for this field.
func (r *runner) scanProgress(now time.Time, f laneFacts) *telemetry.ScanProgress {
	if f.scanTotal == 0 {
		// Nil rather than an empty struct, so a renderer stops drawing a scan bar without having to
		// branch on a phase.
		return nil
	}
	sp := &telemetry.ScanProgress{
		LanesTotal: f.scanTotal, LanesFinished: f.scanDone, StartedAt: r.started,
	}
	// A PROGRESS BAR THAT GUESSES IS WORSE THAN NO PROGRESS BAR. One lane with no declared weight
	// makes the weighted fraction wrong by an unknown amount, so the fraction is withheld rather
	// than approximated from the lane count.
	if !f.everyScanLaneDeclaredAWeight || f.scanWeight == 0 {
		return sp
	}
	frac := float64(f.scanWeightDone) / float64(f.scanWeight)
	sp.Fraction = &frac
	if frac > 0 && frac < 1 {
		eta := now.Add(time.Duration(float64(now.Sub(r.started)) * (1 - frac) / frac))
		sp.ETA = &eta
	}
	return sp
}

// throughput differences the last two samples.
func (r *runner) throughput(now time.Time, f laneFacts) telemetry.Throughput {
	r.status.mu.Lock()
	defer r.status.mu.Unlock()

	if !r.status.sampledAt.IsZero() {
		if dt := now.Sub(r.status.sampledAt).Seconds(); dt >= minRateWindow.Seconds() {
			in := float64(f.admitted-r.status.sampledIn) / dt
			out := float64(f.settled-r.status.sampledOu) / dt
			r.status.rateIn, r.status.rateOut = &in, &out
			r.status.sampledAt, r.status.sampledIn, r.status.sampledOu = now, f.admitted, f.settled
		}
	} else {
		r.status.sampledAt, r.status.sampledIn, r.status.sampledOu = now, f.admitted, f.settled
	}

	t := telemetry.Throughput{RecordsPerSecondIn: r.status.rateIn, RecordsPerSecondOut: r.status.rateOut}
	// RECORDS IN MINUS RECORDS OUT, where OUT is settled PLUS abandoned.
	//
	// Subtracting only the settled ones was wrong before shedding existed and obviously wrong after
	// it: an abandoned record leaves the pipeline by an accounted route — a dead letter, a drop, a
	// lane shed — and counting it as missing meant the delta went permanently non-zero the first time
	// any of those fired. That is precisely the signal being destroyed, because a delta that is always
	// non-zero cannot report the thing it exists for: a QUIESCENT pipeline whose records went
	// somewhere nobody accounted for.
	delta := int64(f.admitted) - int64(f.settled) - int64(f.abandoned)
	t.ReconcileDelta = &delta
	return t
}

// minRateWindow is how much wall time a rate needs before it means anything. Below it the quotient
// is dominated by whichever tick happened to land inside the window.
const minRateWindow = 500 * time.Millisecond

// recentEvents drains what the connectors reported through connector.Runtime.Note.
//
// baseRuntime.Note has carried the comment "the read model's RecentEvents ring is where they belong,
// and that arrives with the status document" since it was written. This is that.
func (r *runner) recentEvents() []telemetry.Event {
	var out []telemetry.Event
	collect := func(node record.NodeID, b *baseRuntime) {
		for _, e := range b.recent() {
			ev := telemetry.Event{
				At: e.At, Kind: e.Kind.String(), Node: node,
				Lane: e.Lane, Stream: string(e.Stream), Message: e.Message, Detail: e.Detail,
			}
			if e.Severity != fault.Unclassified {
				ev.Severity = e.Severity.String()
			}
			out = append(out, ev)
		}
	}
	srcRT, sinkRT := r.runtimes()
	for id, rt := range srcRT {
		collect(id, &rt.baseRuntime)
	}
	for id, rt := range sinkRT {
		collect(id, &rt.baseRuntime)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	if len(out) > maxEventsPerDocument {
		out = out[len(out)-maxEventsPerDocument:]
	}
	return out
}

const maxEventsPerDocument = 32

// lastFault renders the fault that ended the run, if one has.
func (r *runner) lastFault() *telemetry.FaultInfo {
	r.mu.Lock()
	err, at := r.firstErr, r.firstErrAt
	r.mu.Unlock()
	if err == nil {
		return nil
	}
	var f *fault.Fault
	if !errors.As(err, &f) {
		// An unclassified error still ended the run, and reporting nothing would leave a failed
		// pipeline with no explanation on the one field an operator reads for it.
		return &telemetry.FaultInfo{
			At: at, Class: fault.Unclassified.String(), Blame: fault.Unclassified.Blames().String(),
			Op: fault.OpUnknown.String(), User: err.Error(),
		}
	}
	// Attempts is not carried on a fault, and inventing a number would be worse than the zero the
	// field's type permits: the retry budget's accounting lives in engine/retry.go and never reaches
	// the value that escapes.
	return telemetry.NewFaultInfo(at, f, 0)
}
