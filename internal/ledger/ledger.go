package ledger

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Config configures one pipeline's ledger.
type Config struct {
	Tenant   record.TenantID
	Pipeline record.PipelineID

	// DefaultBudget is the in-flight allowance for a lane that does not override it.
	DefaultBudget int

	// GroupTTL bounds how long a group may stay unsettled before the ledger declares it LEAKED — named,
	// not silently resolved. Zero disables the reaper, which is legal only in a test.
	GroupTTL time.Duration
}

// group is one settlement unit: the set of records admitted together, which resolve together.
type group struct {
	lane record.LaneID
	node record.NodeID

	ticket  Ticket
	refs    uint32
	records int

	// handles are the source's own delivery handles for a discrete lane, keyed by the record they
	// belong to. They are the only progress vocabulary such a lane has.
	//
	// Keyed rather than a flat slice, because a partially abandoned group must be able to report the
	// handles that LANDED and the handles that did not, separately. A flat slice could only be released
	// whole or withheld whole, and withholding whole meant nine successful deliveries in a group of ten
	// were never acknowledged at all.
	handles map[record.RecordID][]byte

	// landed and abandonedHandles partition the group's handles as outcomes arrive.
	landed           [][]byte
	abandonedHandles [][]byte

	// abandonedBy attributes abandonments to the sink node that caused them, so a destructive-commit
	// source can distinguish a by-design best-effort shed from a real dead-letter.
	abandonedBy map[record.NodeID]uint64

	at        time.Time
	abandoned uint64
	settled   uint32
}

// laneState is everything the ledger knows about one lane.
type laneState struct {
	id       record.LaneID
	ordering connector.Ordering
	budget   int

	// tracker is non-nil only for a prefix-ordered lane. A discrete lane has no cursor, so there is no
	// prefix to resolve.
	tracker *Tracker[record.Position]

	seq uint64

	// resolved is the contiguous delivered prefix; committed is the DURABLE cursor. Two separate
	// concepts, two separate fields: a metric that reports the resolver's prefix as progress reports
	// progress that was never persisted.
	resolved    record.Position
	resolvedOK  bool
	committed   record.Position
	committedOK bool

	// pendingFlush is the highest safe position resolved since the engine last collected flushables.
	pendingFlush   record.Position
	pendingFlushOK bool

	// settledHandles and abandonedHandles accumulate the handles of LANDED and of terminally
	// not-delivered deliveries on a discrete lane, drained into the next acknowledgement. Two lists
	// because a source must be able to answer its peers differently, per delivery.
	settledHandles   [][]byte
	abandonedHandles [][]byte

	// ackAbandonedBy attributes the pending acknowledgement's abandonments per sink node.
	ackAbandonedBy map[record.NodeID]uint64

	// admittedSinceSafe is the MEASURED replay window: records admitted since the last durable safe
	// position. It is computed, never the budget under another name.
	admittedSinceSafe uint64

	ackRecords   uint64
	ackAbandoned uint64
	laneFinished bool

	revoked bool
	epoch   uint64

	blockedSince time.Time
	blocked      bool

	recordsRead      uint64
	recordsCommitted uint64
	recordsAbandoned uint64
}

// Ledger is the per-pipeline settlement graph. One Ledger holds one [Tracker] per prefix-ordered lane
// and one pending set per discrete-ordered lane.
//
// It is safe for concurrent use: admission runs on a source node's read goroutine while settlement runs
// on sink node goroutines and the commit pump runs on its own.
type Ledger struct {
	cfg Config

	mu     sync.Mutex
	lanes  map[record.LaneID]*laneState
	groups map[record.GroupID]*group
	byRec  map[record.RecordID]record.GroupID

	acks chan connector.Ack

	leaks  []Leak
	closed bool

	stopReaper chan struct{}
	reaperDone chan struct{}
}

// New creates a ledger. The caller must Close it: its absence leaks a reaper goroutine, a pending map
// and a commit pump per pipeline generation.
func New(cfg Config) *Ledger {
	if cfg.DefaultBudget < 1 {
		cfg.DefaultBudget = 1000
	}
	l := &Ledger{
		cfg:        cfg,
		lanes:      map[record.LaneID]*laneState{},
		groups:     map[record.GroupID]*group{},
		byRec:      map[record.RecordID]record.GroupID{},
		acks:       make(chan connector.Ack, 64),
		stopReaper: make(chan struct{}),
		reaperDone: make(chan struct{}),
	}
	go l.reap()
	return l
}

// Lane registers a lane. Called by the engine when a source announces one. It is idempotent on the
// lane id, because Announce is idempotent on the lane name.
func (l *Ledger) Lane(id record.LaneID, ord connector.Ordering, budget int) error {
	if id == "" {
		return fault.Contract(fault.OpBuffer, fmt.Errorf("ledger: lane id is empty"))
	}
	if budget < 1 {
		budget = l.cfg.DefaultBudget
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if st, ok := l.lanes[id]; ok {
		if st.ordering != ord {
			return fault.Contract(fault.OpBuffer,
				fmt.Errorf("ledger: lane %q was registered as %s and is now %s", id, st.ordering, ord))
		}
		return nil
	}
	st := &laneState{id: id, ordering: ord, budget: budget}
	if ord == connector.OrderingPrefix {
		st.tracker = NewTracker[record.Position](uint64(budget))
	}
	l.lanes[id] = st
	return nil
}

// SetEpoch records the lease epoch a lane is currently held under, so an emitted acknowledgement can
// carry it and a fenced worker's commit is refused by the store.
func (l *Ledger) SetEpoch(lane record.LaneID, epoch uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if st, ok := l.lanes[lane]; ok {
		st.epoch = epoch
	}
}

// Admit stamps the batch's position sequence, opens a settlement group, and returns once admission is
// within budget.
//
// BLOCKING HERE IS THE ENTIRE SOURCE-SIDE BACKPRESSURE MECHANISM: no credit protocol, no separate
// in-flight semaphore, no checkpoint-limit knob. A pull source observes nothing except that Read is not
// called again yet; a push source is handed the returned fault and renders it as a retryable refusal.
func (l *Ledger) Admit(ctx context.Context, b *record.Batch) error {
	if b == nil {
		return nil
	}
	l.mu.Lock()
	st, ok := l.lanes[b.Lane]
	if !ok {
		l.mu.Unlock()
		return fault.Contract(fault.OpBuffer, fmt.Errorf("ledger: lane %q is not registered", b.Lane))
	}
	if st.revoked {
		l.mu.Unlock()
		return fault.ErrFenced
	}

	// A batch whose records disagree with its lane is REFUSED, loudly.
	//
	// record.Batch.Lane is a settable field and a record's Origin.Lane is stamped by the batch's
	// allocator, so a source retargeting the field mislabelled every record and the ledger settled the
	// group under the field's lane regardless: measured at 33350 of 33500 records attributed to a lane
	// they did not come from, with no error anywhere. Silent settlement corruption is the worst
	// possible failure mode for this package, so it becomes a contract fault at the one place every
	// batch passes through.
	for _, r := range b.Records {
		if o := r.Origin(); o.Lane != b.Lane {
			l.mu.Unlock()
			return fault.Contract(fault.OpBuffer, fmt.Errorf(
				"ledger: batch declares lane %q but record %d was stamped for lane %q; a source serving several lanes implements connector.LaneReader rather than retargeting one batch",
				b.Lane, o.ID, o.Lane))
		}
	}

	// Invariant: the core assigns sequence, not the connector. Order is never imputed to a connector's
	// opaque bytes. See docs/decisions/0005-canonical-record-model.md.
	st.seq++
	b.SetSeq(st.seq)

	var refs uint32
	var handles map[record.RecordID][]byte
	for _, r := range b.Records {
		refs += r.Origin().Refs()
		if st.ordering == connector.OrderingDiscrete {
			if h := r.Handle(); h != nil {
				if handles == nil {
					handles = map[record.RecordID][]byte{}
				}
				handles[r.Origin().ID] = h
			}
		}
	}

	// A ZERO-RECORD BATCH IS RESOLVED AT ADMISSION, with zero references and zero weight.
	//
	// It is how a lane advances a cursor without producing anything: a page of already-seen rows, a
	// fully filtered tail, a chunk planner that only planned, an idle poll that only proved idleness,
	// and the documented zero-record EndOfLane batch that linefile has always emitted. Raising refs and
	// weight to one — which is what this code did — opened a group that could NEVER settle, because
	// Settle is keyed on record ids and there were none. Every such lane wedged forever, and the
	// bounded-file source's own final batch wedged its own lane.
	//
	// The position still enters the tracker, so it takes its place in the prefix IN ORDER behind
	// whatever is still outstanding. Skipping the tracker entirely would commit it immediately and
	// therefore past unsettled records, which is the one thing this package exists to prevent.
	empty := b.Len() == 0
	weight := uint64(b.Len())
	pos := b.Position
	g := &group{
		lane:    b.Lane,
		refs:    refs,
		records: b.Len(),
		handles: handles,
		at:      time.Now(),
	}
	if len(b.Records) > 0 {
		g.node = b.Records[0].Origin().Node
	}
	tracker := st.tracker
	l.mu.Unlock()

	if empty {
		// Nothing will ever settle it, so no group is opened and nothing is registered in byRec.
		// TrackResolved never blocks and reports whether the prefix moved.
		if tracker == nil {
			return nil
		}
		advanced, moved := tracker.TrackResolved(pos)
		l.mu.Lock()
		defer l.mu.Unlock()
		st.blocked = false
		if moved {
			st.resolved, st.resolvedOK = advanced, true
			if advanced.Safe {
				st.pendingFlush, st.pendingFlushOK = advanced, true
			}
		}
		return nil
	}

	// Track blocks OUTSIDE the ledger's mutex, so a full lane does not stall settlement on another lane.
	var ticket Ticket
	if tracker != nil {
		var err error
		ticket, err = tracker.Track(ctx, pos, weight, refs)
		if err != nil {
			return err
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	g.ticket = ticket
	l.groups[b.Group()] = g
	for _, r := range b.Records {
		l.byRec[r.Origin().ID] = b.Group()
	}
	st.recordsRead += uint64(b.Len())
	st.admittedSinceSafe += uint64(b.Len())
	st.blocked = false
	return nil
}

// Expand adds n references to a group, for a one-to-N expansion or a fan-out to n sink nodes. Called by
// the engine, never by a connector.
//
// It is what makes fan-out, filtering, expansion and regrouping need no core code path per topology:
// references travel on the record and the group merely counts them.
func (l *Ledger) Expand(g record.GroupID, n uint32) error {
	if n == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	gr, ok := l.groups[g]
	if !ok {
		return fault.Contract(fault.OpTransform, fmt.Errorf("ledger: group %d is not open", g))
	}
	gr.refs += n
	if gr.ticket.n != nil {
		gr.ticket.n.refs += n
	}
	return nil
}

// Register associates a derived record with its group, so a per-record settlement can find it. The
// engine calls it for every record a transform or decoder derives.
func (l *Ledger) Register(id record.RecordID, g record.GroupID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.groups[g]; ok {
		l.byRec[id] = g
	}
}

// Settle records terminal dispositions, IN BATCH.
//
// A per-record-only API costs hundreds of locked map operations for one five-hundred-record request;
// batch is the primitive and a single record is a batch of one.
func (l *Ledger) Settle(outs []Outcome) {
	type advance struct {
		lane record.LaneID
		pos  record.Position
	}
	var advances []advance

	l.mu.Lock()
	for _, o := range outs {
		gid, ok := l.byRec[o.Record]
		if !ok {
			continue
		}
		g, ok := l.groups[gid]
		if !ok {
			continue
		}
		delete(l.byRec, o.Record)

		g.settled++
		if h, ok := g.handles[o.Record]; ok {
			// Partition the group's handles as outcomes arrive, rather than releasing or withholding the
			// whole group. Nine landed deliveries in a group of ten used to be withheld entirely because one
			// was abandoned, so nine peers waited to their deadlines for an answer that already existed.
			if o.Disposition == Abandoned {
				g.abandonedHandles = append(g.abandonedHandles, h)
			} else {
				g.landed = append(g.landed, h)
			}
			delete(g.handles, o.Record)
		}
		if o.Disposition == Abandoned {
			g.abandoned++
			if o.Node != "" {
				if g.abandonedBy == nil {
					g.abandonedBy = map[record.NodeID]uint64{}
				}
				g.abandonedBy[o.Node]++
			}
		}

		st := l.lanes[g.lane]
		if st == nil {
			continue
		}

		if g.ticket.n != nil {
			var pos record.Position
			var moved bool
			if o.Disposition == Abandoned && g.settled >= g.refs {
				pos, moved = st.tracker.Abandon(g.ticket)
			} else {
				pos, moved = st.tracker.Release(g.ticket, 1)
			}
			if moved {
				advances = append(advances, advance{lane: g.lane, pos: pos})
			}
		}

		if g.settled >= g.refs {
			st.ackRecords += uint64(g.records)
			st.ackAbandoned += g.abandoned
			if o.Disposition == Abandoned {
				st.recordsAbandoned += g.abandoned
			}
			for node, n := range g.abandonedBy {
				if st.ackAbandonedBy == nil {
					st.ackAbandonedBy = map[record.NodeID]uint64{}
				}
				st.ackAbandonedBy[node] += n
			}
			if st.ordering == connector.OrderingDiscrete {
				st.settledHandles = append(st.settledHandles, g.landed...)
				st.abandonedHandles = append(st.abandonedHandles, g.abandonedHandles...)
			}
			delete(l.groups, gid)
		}
	}

	for _, a := range advances {
		st := l.lanes[a.lane]
		if st == nil {
			continue
		}
		st.resolved, st.resolvedOK = a.pos, true
		// Invariant: the committed position is the last SAFE position at or before the resolved prefix.
		// This is a core invariant, not a per-connector convention.
		if a.pos.Safe {
			st.pendingFlush, st.pendingFlushOK = a.pos, true
		}
	}
	l.mu.Unlock()

	// A discrete lane has no prefix to resolve, so its acknowledgement is emitted as soon as its
	// handles are known to be durable — but still only after the engine has reported the write durable,
	// which is what calling Settle means.
	l.emitDiscrete()
}

// Flushable returns the lanes whose prefix has advanced to a new safe position since the last call,
// with the positions to persist.
//
// The engine calls this, writes them durably, and only THEN calls [Ledger.Committed]. Nothing here
// emits an acknowledgement: phase two must complete first.
func (l *Ledger) Flushable() map[record.LaneID]record.Position {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out map[record.LaneID]record.Position
	for id, st := range l.lanes {
		if !st.pendingFlushOK || st.revoked {
			continue
		}
		if st.committedOK {
			if c, ok := st.pendingFlush.Compare(st.committed); ok && c <= 0 {
				// Never move a cursor backwards (invariant 2).
				st.pendingFlushOK = false
				continue
			}
			if st.pendingFlush.Seq <= st.committed.Seq {
				st.pendingFlushOK = false
				continue
			}
		}
		if out == nil {
			out = map[record.LaneID]record.Position{}
		}
		out[id] = st.pendingFlush
		st.pendingFlushOK = false
	}
	return out
}

// Committed tells the ledger which positions are now DURABLE. Only after this does [Ledger.Acks] yield
// an acknowledgement for them.
//
// Invariant 1: the source is told it may advance only after canal's own record of the position has been
// durably flushed. See docs/decisions/0006-three-phase-commit.md. Do not call this before the flush.
func (l *Ledger) Committed(m map[record.LaneID]record.Position) {
	var out []connector.Ack

	l.mu.Lock()
	for id, pos := range m {
		st, ok := l.lanes[id]
		if !ok {
			continue
		}
		st.committed, st.committedOK = pos, true
		st.recordsCommitted += st.ackRecords
		st.admittedSinceSafe = 0

		if st.revoked {
			// The fence: in-flight records settled for accounting, but NO acknowledgement is ever
			// delivered for a revoked lane. Letting a fenced worker tell an upstream to advance is
			// specified data loss.
			st.ackRecords, st.ackAbandoned, st.ackAbandonedBy = 0, 0, nil
			continue
		}
		if st.ordering != connector.OrderingPrefix {
			continue
		}
		out = append(out, connector.Ack{
			Lane:         id,
			Epoch:        st.epoch,
			Through:      pos,
			Records:      st.ackRecords,
			Abandoned:    st.ackAbandoned,
			AbandonedBy:  st.ackAbandonedBy,
			LaneFinished: st.laneFinished,
		})
		st.ackRecords, st.ackAbandoned, st.ackAbandonedBy = 0, 0, nil
	}
	l.mu.Unlock()

	for _, a := range out {
		l.send(a)
	}
}

// FinishLane marks a bounded lane's finish, which the engine does only once every group admitted for it
// has settled AND that fact is durable. The next acknowledgement for the lane carries LaneFinished.
func (l *Ledger) FinishLane(lane record.LaneID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if st, ok := l.lanes[lane]; ok {
		st.laneFinished = true
	}
}

// emitDiscrete drains settled handles into acknowledgements for discrete lanes.
func (l *Ledger) emitDiscrete() {
	var out []connector.Ack
	l.mu.Lock()
	for id, st := range l.lanes {
		if st.ordering != connector.OrderingDiscrete {
			continue
		}
		if len(st.settledHandles) == 0 && len(st.abandonedHandles) == 0 {
			continue
		}
		if st.revoked {
			st.settledHandles, st.abandonedHandles = nil, nil
			st.ackRecords, st.ackAbandoned, st.ackAbandonedBy = 0, 0, nil
			continue
		}
		out = append(out, connector.Ack{
			Lane:             id,
			Epoch:            st.epoch,
			Handles:          st.settledHandles,
			AbandonedHandles: st.abandonedHandles,
			Records:          st.ackRecords,
			Abandoned:        st.ackAbandoned,
			AbandonedBy:      st.ackAbandonedBy,
			LaneFinished:     st.laneFinished,
		})
		st.settledHandles, st.abandonedHandles = nil, nil
		st.recordsCommitted += st.ackRecords
		st.ackRecords, st.ackAbandoned, st.ackAbandonedBy = 0, 0, nil
	}
	l.mu.Unlock()
	for _, a := range out {
		l.send(a)
	}
}

func (l *Ledger) send(a connector.Ack) {
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return
	}
	// The channel is core-internal and never crosses the plugin boundary, so a channel is fine here. It
	// is buffered; a full buffer means the commit pump is wedged, which the pump's own metric reports.
	l.acks <- a
}

// Acks is the stream the engine pumps into Source.Commit.
//
// The pump runs on a DEDICATED per-source goroutine, never inline from the persister, because a slow
// connector would otherwise block the process-wide flush cycle — and the fix for exactly that ordering
// bug created a deadlock in one surveyed system six days later.
func (l *Ledger) Acks() <-chan connector.Ack { return l.acks }

// Revoke stops emitting acknowledgements for a lane.
//
// In-flight records still settle so that buffers drain and metrics are correct, but NO acknowledgement
// for the lane is ever delivered after Revoke. That is the fence. The new holder resumes from the last
// durable cursor and re-delivers whatever was in flight; the cost is up to one in-flight window of
// duplicates per reassignment, and it is counted and disclosed rather than hidden.
func (l *Ledger) Revoke(lane record.LaneID) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.lanes[lane]
	if !ok {
		return 0
	}
	st.revoked = true
	var unsettled uint64
	for _, g := range l.groups {
		if g.lane == lane {
			unsettled += uint64(g.records)
		}
	}
	return unsettled
}

// Stats reports one lane's accounting.
func (l *Ledger) Stats(lane record.LaneID) LaneStats {
	l.mu.Lock()
	st, ok := l.lanes[lane]
	if !ok {
		l.mu.Unlock()
		return LaneStats{}
	}
	out := LaneStats{
		Resolved:       st.resolved,
		ResolvedOK:     st.resolvedOK,
		Committed:      st.committed,
		CommittedOK:    st.committedOK,
		InFlightBudget: uint64(st.budget),
		ReplayRecords:  st.admittedSinceSafe,
		Blocked:        st.blocked,
	}
	if st.blocked {
		out.BlockedFor = time.Since(st.blockedSince)
	}
	for _, g := range l.groups {
		if g.lane == lane {
			out.PendingGroups++
		}
	}
	tracker := st.tracker
	out.Admitted = st.recordsRead
	out.AbandonedTotal = st.recordsAbandoned
	l.mu.Unlock()

	if tracker != nil {
		weight, _, oldest, oldestPos := tracker.Pending()
		_, settled, abandoned := tracker.Counts()
		out.InFlight = weight
		out.Settled = settled
		out.AbandonedTotal = abandoned
		out.OldestPendingAge = oldest
		out.OldestPendingPosition = oldestPos
	}
	return out
}

// LaneStats is one lane's accounting, as the read model consumes it.
type LaneStats struct {
	// Resolved is the contiguous DELIVERED prefix. Committed is the DURABLE cursor. Committed is always
	// at or behind Resolved, and the checkpoint-age metric derives from Committed alone — because a
	// watermark taken from the resolver reports progress that was never persisted.
	Resolved    record.Position
	ResolvedOK  bool
	Committed   record.Position
	CommittedOK bool

	Admitted       uint64
	Settled        uint64
	AbandonedTotal uint64
	InFlight       uint64
	PendingGroups  int

	OldestPendingAge      time.Duration
	OldestPendingPosition record.Position

	// InFlightBudget is the CONFIGURED cap. ReplayRecords is the MEASURED worst-case re-read on crash:
	// records admitted since the last durable safe position.
	//
	// Exporting the budget as "the replay window" is wrong whenever safe-gating is doing work — a lane
	// that has not seen a safe position in fifty thousand records replays fifty thousand, not its
	// budget. Both numbers exist under honest names.
	InFlightBudget uint64
	ReplayRecords  uint64

	Blocked    bool
	BlockedFor time.Duration
}

// Drain waits for everything outstanding to settle, up to ctx.
//
// Acknowledgements CONTINUE to be delivered during a drain: a graceful stop must not throw away a commit
// that is one millisecond from safe.
func (l *Ledger) Drain(ctx context.Context) error {
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		l.mu.Lock()
		open := len(l.groups)
		l.mu.Unlock()
		if open == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			// A drain that did not complete is a DIFFERENT event from a completed drain, because it
			// means records may replay. The caller names the unsettled groups rather than discarding
			// them silently.
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Open returns the groups still outstanding, for the drain-timeout report that must NAME them.
func (l *Ledger) Open() []Leak {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Leak, 0, len(l.groups))
	for gid, g := range l.groups {
		out = append(out, Leak{
			Group:   gid,
			Lane:    g.lane,
			Node:    g.node,
			Age:     time.Since(g.at),
			Records: g.records,
		})
	}
	return out
}

// Leaks returns groups that exceeded the configured time-to-live. Non-empty means a BUG, and the engine
// raises it as a condition rather than logging it.
func (l *Ledger) Leaks() []Leak {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.leaks
	l.leaks = nil
	return out
}

func (l *Ledger) reap() {
	defer close(l.reaperDone)
	if l.cfg.GroupTTL <= 0 {
		<-l.stopReaper
		return
	}
	t := time.NewTicker(l.cfg.GroupTTL / 4)
	defer t.Stop()
	for {
		select {
		case <-l.stopReaper:
			return
		case <-t.C:
			l.mu.Lock()
			for gid, g := range l.groups {
				if time.Since(g.at) > l.cfg.GroupTTL {
					l.leaks = append(l.leaks, Leak{
						Group:   gid,
						Lane:    g.lane,
						Node:    g.node,
						Age:     time.Since(g.at),
						Records: g.records,
					})
					// The group is NOT resolved: resolving it would acknowledge undelivered work. It is
					// NAMED, and the lane stalls loudly until an operator or the engine's retry policy
					// terminalises it.
					g.at = time.Now()
				}
			}
			l.mu.Unlock()
		}
	}
}

// Close stops the reaper and the commit pump and releases the trackers. Its absence leaks a goroutine, a
// map and a pump per pipeline generation.
func (l *Ledger) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	for _, st := range l.lanes {
		if st.tracker != nil {
			st.tracker.Close()
		}
	}
	l.mu.Unlock()

	close(l.stopReaper)
	<-l.reaperDone
	close(l.acks)
	return nil
}
