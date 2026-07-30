// Package fanout is a HOSTILE CONNECTOR SET written against canal's real interfaces to find out
// where a non-linear pipeline breaks them.
//
// THE CASE, as commissioned:
//
//	(1) one source fanning out to three sinks with DIFFERENT delivery guarantees and wildly
//	    different speeds — an exactly-once warehouse batching at 30s, an at-least-once queue at
//	    1ms, and a best-effort metrics endpoint that may be dropped under load. Progress may
//	    advance only when the guaranteed sinks are durable; the best-effort sink must never hold
//	    back the pipeline.
//
//	(2) two sources merging into one sink with a shared checkpoint.
//
// This package imports ONLY what a third-party connector may import — record, schema, config,
// fault, connector, registry — exactly as internal/example/linefile does. It deliberately does NOT
// import engine, ledger, spec, store or telemetry, because a connector author cannot. The topology
// that exercises these components, and the engine.Build run that PROVES the findings below, live in
// probe_test.go, which is a harness and not a connector.
//
// ============================================================================================
// SUMMARY OF FINDINGS, worst first. Each is argued in full at its own site in this file.
// ============================================================================================
//
//	F1  spec.StreamConfig.Write is ONE destination mode per stream for the WHOLE pipeline, and
//	    engine.negotiate checks it against EVERY sink. Fan-out to an upserting warehouse and an
//	    append-only queue is REFUSED AT BUILD. See the block above type warehouse.
//	    CONFIRMED by probe_test.go/TestFanOutIsRefused.
//
//	F2  spec.Spec.Guarantee is ONE requested tier per pipeline, and negotiate folds Min over
//	    every sink. Requesting ExactlyOnce for the warehouse branch produces a HARD diagnostic
//	    for each of the other two sinks. Requesting AtLeastOnce hands the warehouse
//	    Opening.Guarantee == at_least_once while it is in fact running two-phase commit.
//	    See the block above type warehouse. CONFIRMED.
//
//	F3  engine/negotiate.go:61 caps `effective` at AtLeastOnce for every Replayable source, so
//	    telemetry.Negotiated.Guarantee can NEVER report effectively_once or exactly_once for any
//	    pipeline at all. Build accepts a requested ExactlyOnce against a Committer sink and then
//	    reports at_least_once. One-line core bug, found by this case.
//	    CONFIRMED by probe_test.go/TestNegotiatedGuaranteeCannotReachExactlyOnce.
//
//	F4  engine.Checkpoint.WriterState is []record.Blob with NO node key, while TransformState is
//	    map[NodeID][]record.Blob. Two WriterState sinks in one graph — which this case has — get
//	    each other's blobs at RestoreState. See the block above warehouse.RestoreState.
//
//	F5  connector.Committable persists FirstRec/LastRec as record.RecordID, which record/ids.go
//	    documents as generation-local and "never appears in persisted state". After the restart
//	    the committable exists for, those ids name nothing. See warehouse.PrepareCommit.
//
//	F6  There is no way to declare a sink branch NON-PROGRESS-BEARING. Ledger.Expand adds a ref
//	    per fan-out edge and the prefix waits for all of them, so the only way to shed load at
//	    the metrics sink is to fail records into a terminal disposition — which makes
//	    Ack.Abandoned permanently non-zero and permanently indistinguishable from dead-lettering.
//	    See the block above type metrics, and cdcsrc.Commit.
//
//	F7  Durability cadence is pipeline-wide (engine.Deps.FlushInterval). A 30s-batch warehouse
//	    and a 1ms queue in one graph cannot both have their own. See warehouse.Flush.
//
//	F8  Flusher.Flush inherits Write's quadrant table, under which an empty Failed plus a nil
//	    error means EVERYTHING COVERED IS DURABLE. A sink whose durability cadence is coarser
//	    than the checkpoint cadence therefore cannot say "nothing new is durable; do not settle,
//	    do not resend". See warehouse.Flush.
//
//	F9  telemetry.Downgrade — ADR 0024's only escape hatch from F1/F2, and the one field that
//	    carries a NodeID — is unreachable: spec.Spec has no Downgrades field and negotiate never
//	    reads one. See the block above type metrics.
//
//	F10 spec.StreamConfig has no node scope, so a read mode wanted from source A is validated
//	    against source B. Two sources merging into one sink cannot have different lane kinds.
//	    See the block above type mergeSrc. CONFIRMED by probe_test.go/TestFanInIsRefused.
//
//	F11 telemetry.Negotiated.AckPoint and .DurabilityEdge are single-valued strings assigned
//	    inside `for id, sk := range res.sinks`, so with four sinks the operator is shown whichever
//	    branch Go's map iteration visited last. 200 identical Builds of this graph produce FOUR
//	    different durability_edge values and TWO different ack_point values.
//	    CONFIRMED by probe_test.go/TestAckPointIsNondeterministic.
//
//	F12 A sink that implements SchemaApplier and declares an HONEST partial SchemaChanges list is
//	    REFUSED under the DEFAULT drift policy, while the same sink with AppliesSchema false builds.
//	    DriftTryEvolve, whose entire purpose is "applies what is supported and emits an event for
//	    the rest", is validated identically to DriftLenient. Not fan-out-specific; found here by
//	    accident. See the block above type warehouse's ApplySchemaChange.
//	    CONFIRMED by probe_test.go/TestSchemaApplierIsPunishedForBeingHonest.
//
// WHAT FITS CLEANLY, stated so the receipt is not read as "everything is broken":
//
//   - The TOPOLOGY. Three sink nodes naming one source node, and one sink node naming two source
//     nodes, both pass validateGraph with no special case. spec.Edge is enough vocabulary.
//   - The SETTLEMENT ALGEBRA. Origin.refs + Ledger.Expand really does make a three-way fan-out
//     resolve exactly once with no per-topology code, and Tracker.discharge's `done` flag makes a
//     late duplicate release a no-op rather than a prefix corruption. I tried to break it by
//     ordering the three branches' outcomes every way round; it holds.
//   - The SHARED CHECKPOINT of case (2), in the durability sense: engine.Checkpoint.Lanes is a
//     map over the whole pipeline's lanes written through StateStore.SetMany, which is documented
//     all-or-nothing. Two sources' cursors already advance in one atomic record. No fix needed.
//   - A sink having NO progress awareness. Writing three sinks with three different guarantees, I
//     never once wanted a position, and never could have got one wrong.
//
// AND WHAT IS NOT A FINDING, so nobody churns on it:
//
//   - The 1ms queue branch's cursor waiting up to 30s for the warehouse branch is CORRECT and is
//     what the commission asked for. It costs a 30s replay window on the queue branch after a
//     crash, and LaneStats.ReplayRecords already discloses exactly that number under an honest
//     name. One prefix per lane is the design; I am not asking for per-branch cursors.
//   - A consistent CUT across the two merged sources (source A's cursor held back to source B's)
//     is not expressible. ADR 0010 names that as a deliberate non-goal. I am not reporting a
//     documented non-goal as a breakage.
package fanout

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// Reg is this harness's own registry. A real connector would pass registry.Default; using a private
// one keeps a stress package from polluting the process registry the rest of the repo asserts on.
var Reg = registry.New()

// cursorV1 versions every cursor token this package authors. Everything durable is (version, bytes).
const cursorV1 = 1

// ============================================================================================
// The fan-out source.
// ============================================================================================

func init() {
	registry.AddSource(Reg, registry.SourceDef[*cdcSrc]{
		Meta: registry.Meta{
			Name:    "hostile_cdc",
			Version: "1.0.0",
			Title:   "Hostile CDC tail",
			Summary: "A prefix-ordered changelog tail whose commit is DESTRUCTIVE upstream.",
			Notes: "Origin.Key is the big-endian encoding of the upstream row id, which is the " +
				"upstream's own primary key and is stable across re-reads of the same log position.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec().
			Field(config.Field{
				Name: "slot", Type: config.TypeString,
				Description: "Replication slot to read. Advancing it frees upstream log, which is why " +
					"this source declares PrunesOnCommit.",
				Examples: []any{"canal_slot"},
			}).
			Field(config.Field{
				Name: "stream", Type: config.TypeString, Default: "orders", Optional: true,
				Description: "Logical stream name announced for the tail lane.",
			}),
		Caps: connector.SourceCaps{
			Caps:            connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering: connector.OrderingPrefix,
			Boundedness:     []connector.Boundedness{connector.Unbounded},
			LaneKinds:       []connector.LaneKind{connector.LaneKindStream},
			MaxLanes:        1,
			// The whole reason this source is hostile: acting on a Commit DISCARDS upstream data, so
			// Ack.Abandoned is a decision this source is forced to make. See Commit.
			UpstreamRetention:   connector.PrunesOnCommit,
			ReplayWindow:        6 * time.Hour,
			UnitAssignment:      connector.UnitsStatic,
			Heartbeats:          true,
			Nackable:            true,
			ReportsBacklog:      true,
			ProducesEventTime:   true,
			ProducesChange:      true,
			ProducesSchema:      true,
			CompleteImages:      true,
			ComparablePositions: true,
			Replayable:          true,
			StableKeys:          true,
			MidLaneResume:       true,
		},
		New: func(_ context.Context, c *config.Config) (*cdcSrc, error) {
			s := &cdcSrc{
				slot:   config.Must[string](c, "slot"),
				stream: record.StreamName(config.Must[string](c, "stream")),
				lanes:  map[record.LaneID]*laneCursor{},
			}
			return s, c.Err()
		},
	})
}

// laneCursor is this source's per-lane read state: the upstream log offset it will read from next,
// plus the offset it has been told is durable downstream.
type laneCursor struct {
	next      uint64
	committed uint64
	live      bool
}

type cdcSrc struct {
	slot   string
	stream record.StreamName

	rt      connector.SourceRuntime
	persist *connector.Persister

	// mu guards lanes, which the read goroutine writes and the control goroutine (Commit,
	// Heartbeat, Backlog, Nack) reads. Source.Read's concurrency contract says those two
	// goroutines are the only ones, and that the control methods never run concurrently with each
	// other, so one mutex is the whole synchronisation this source needs.
	mu    sync.Mutex
	lanes map[record.LaneID]*laneCursor
	order []record.LaneID

	// abandonedSeen counts records the pipeline told us reached a terminal disposition. It exists
	// only to make the impossible decision in Commit measurable; see the block there.
	abandonedSeen uint64
}

func (s *cdcSrc) Open(ctx context.Context, rt connector.SourceRuntime) error {
	s.rt = rt
	s.persist = connector.AutoPersist(rt)

	as, err := rt.Lanes().Assigned(ctx)
	if err != nil {
		return err
	}
	if len(as) == 0 {
		id, err := rt.Lanes().Announce(ctx, connector.LaneSpec{
			Name:        "slot:" + s.slot,
			Stream:      s.stream,
			Kind:        connector.LaneKindStream,
			Ordering:    connector.OrderingPrefix,
			Boundedness: connector.Unbounded,
			Group:       "tail",
			Label:       "changelog tail on " + s.slot,
		})
		if err != nil {
			return err
		}
		s.adopt(id, 0)
		return nil
	}
	for _, a := range as {
		off, err := decodeOffset(a.Cursor.Token)
		if err != nil {
			return err
		}
		if _, _, err := s.persist.Load(ctx, a.ID); err != nil {
			return err
		}
		s.adopt(a.ID, off)
	}
	return nil
}

func (s *cdcSrc) adopt(id record.LaneID, off uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.lanes[id]; !ok {
		s.order = append(s.order, id)
	}
	s.lanes[id] = &laneCursor{next: off, committed: off, live: true}
}

// decodeOffset reads a cursor this connector authored. A fixed-width encoding genuinely cannot
// tolerate an unknown version, so rule three of the format contract is satisfied by failing loudly
// with both numbers named rather than by guessing.
func decodeOffset(t record.Blob) (uint64, error) {
	switch {
	case t.IsZero():
		return 0, nil
	case t.Version == cursorV1 && len(t.Bytes) == 8:
		return binary.BigEndian.Uint64(t.Bytes), nil
	default:
		return 0, fault.Contract(fault.OpOpen,
			fmt.Errorf("cursor version %d (%d bytes) unreadable by build %d", t.Version, len(t.Bytes), cursorV1))
	}
}

func encodeOffset(off uint64) record.Blob {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], off)
	return record.Blob{Version: cursorV1, Bytes: b[:]}
}

// batchSize is how many changes one Read produces. The engine's batch has its own hard cap and Add
// returns nil at it, so this is a courtesy bound.
const batchSize = 64

func (s *cdcSrc) Read(ctx context.Context, dst *record.Batch) error {
	lane, cur, ok := s.pick()
	if !ok {
		// Every lane this instance held has been revoked. Not end of input: the assignment may come
		// back, so wait for a change rather than claiming the source is finished.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.rt.Lanes().Changes():
			return nil
		}
	}

	dst.Reset()
	dst.Lane = lane

	from := cur
	for dst.Len() < batchSize {
		// CANCELLATION MEANS DRAIN, NOT ABORT: stop pulling new changes from upstream but keep
		// everything already produced into dst. The engine admits the batch before handling the
		// error, so returning ctx.Err() here with records present would be correct too — but only
		// once nothing is left, which is what this shape gives.
		if ctx.Err() != nil {
			break
		}
		r := dst.Add()
		if r == nil {
			break
		}
		off := from + uint64(dst.Len())
		s.fill(r, off)
	}
	if dst.Len() == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}

	next := from + uint64(dst.Len())
	s.advance(lane, next)

	var order [8]byte
	binary.BigEndian.PutUint64(order[:], next)
	scalar := float64(next)
	dst.Position = record.Position{
		Token:  encodeOffset(next),
		Order:  order[:],
		Scalar: &scalar,
		// Every change in this synthetic log is its own transaction, so every offset is a gap-free
		// resume point. A source that emitted mid-transaction rows would set Safe=false on those.
		Safe:  true,
		At:    time.Now(),
		Label: fmt.Sprintf("lsn %d", next),
	}
	return nil
}

// fill stamps one synthetic change onto an already-provenanced slot.
func (s *cdcSrc) fill(r *record.Record, off uint64) {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], off)

	after := record.StructPayload(record.Map{
		"id":     record.Uint(off),
		"amount": record.Int(int64(off) * 7),
		"state":  record.String("open"),
	})
	r.Payload = after
	r.EventTime = time.Now()
	r.Change = &record.Change{
		Version:        record.ChangeVersion,
		Op:             record.OpUpdate,
		Keys:           [][]string{{"id"}},
		After:          &after,
		BeforeComplete: record.CompletenessAbsent,
		// Declared as CompleteImages in Caps, so this must genuinely be whole or the warehouse's
		// RequiresCompleteImages check is a lie the core cannot catch.
		AfterComplete: record.CompletenessComplete,
		TxID:          fmt.Sprintf("tx-%d", off),
		CommitTime:    time.Now(),
	}
}

func (s *cdcSrc) pick() (record.LaneID, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range s.order {
		c := s.lanes[id]
		if c == nil || !c.live {
			continue
		}
		if s.rt != nil && s.rt.Lanes().Revoked(id) {
			c.live = false
			continue
		}
		return id, c.next, true
	}
	return "", 0, false
}

func (s *cdcSrc) advance(lane record.LaneID, next uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.lanes[lane]; c != nil {
		c.next = next
	}
}

// ============================================================================================
// F6 (part one) — the destructive commit cannot tell a by-design drop from a dead-letter.
// ============================================================================================
//
// THE SIGNATURE THAT BLOCKS ME:
//
//	type Ack struct {
//	    Lane      record.LaneID
//	    Epoch     uint64
//	    Through   record.Position
//	    Handles   [][]byte
//	    Records   uint64
//	    Abandoned uint64      // <-- ONE SCALAR, NO ATTRIBUTION
//	    LaneFinished bool
//	}
//
// Ack.Abandoned's own doc tells me what to do with it, and it is exactly right for a linear
// pipeline: "A source whose commit is DESTRUCTIVE, such as deleting a queue message, may refuse to
// advance when this is non-zero, leaving the message for another consumer. The core surfaces the
// number and the source chooses; the core never makes that choice on a source's behalf."
//
// This source's commit IS destructive — it declares PrunesOnCommit, and advancing the slot frees
// upstream log for recycling. So it is precisely the class of source that doc is written for.
//
// Now put it in the commissioned topology. The metrics branch sheds load by design: the ONLY
// mechanism canal offers for that is a terminal disposition (either the sink fails records and the
// node's retry.terminal is drop, or the edge is configured when_full: drop_newest, whose doc says
// "The affected group is settled abandoned so the source is told"). Either way ledger.Settle
// increments g.abandoned, laneState.ackAbandoned accumulates it, and Ack.Abandoned arrives
// non-zero on essentially EVERY acknowledgement this source will ever receive.
//
// So the two things I must distinguish are:
//
//	(a) the metrics endpoint shed a record under load. Expected. The warehouse and the queue both
//	    have it durably. Advancing the slot is CORRECT and refusing to advance is a disk-fill
//	    outage.
//	(b) the warehouse dead-lettered a record it could not represent. Advancing the slot past it
//	    destroys the only remaining copy.
//
// Ack carries one uint64 for both. There is no per-node breakdown, no per-edge breakdown, and
// nothing on SourceRuntime that lets me ask afterwards. Nackable does not rescue it either: Nack
// carries {Handle, Position, Class, Reason, Attempts} and no node, so a nack from the metrics
// branch and a nack from the warehouse branch are also indistinguishable — and Nack is called for
// terminal failures, which a by-design load shed also is.
//
// I therefore cannot write a correct Commit. What is written below is the least-bad option and it
// is WRONG, deliberately and visibly: it advances on (a) by treating all abandonment as expected,
// which silently accepts (b) as well. The alternative — refusing whenever Abandoned > 0 — pins the
// replication slot forever the moment the metrics endpoint gets slow, which is the documented
// severity-zero failure PrunesOnCommit exists to prevent.
//
// SMALLEST FIX, and it is additive: give Ack per-node attribution.
//
//	// AbandonedBy attributes Abandoned to the terminal node that abandoned, so a source with a
//	// destructive commit can distinguish a branch that sheds by design from one that
//	// dead-lettered. Nil when nothing was abandoned.
//	AbandonedBy map[record.NodeID]uint64 `json:"abandoned_by,omitempty"`
//
// Ack is a plain serialisable struct with no closures, channels or interface fields, and a map of
// string to uint64 keeps it so; the out-of-process seam is unaffected. Every existing source
// ignores the field and is unchanged, so NO ALREADY-WRITTEN CONNECTOR BREAKS. The ledger already
// holds what it needs: group.node is recorded at Admit and ledger.Outcome could carry the settling
// node, which is engine-internal.
//
// That fix makes (a) and (b) distinguishable but still requires every destructive source to know
// which node ids are best-effort. The BETTER fix is F6 part two — see the block above type metrics
// — which removes the best-effort branch from the settlement graph entirely, at which point
// Abandoned means only (b) and this method becomes writable as it stands. Both fixes are additive;
// if only one is taken, take that one.
func (s *cdcSrc) Commit(ctx context.Context, a Ack) error {
	off, err := decodeOffset(a.Through.Token)
	if err != nil {
		return err
	}

	s.mu.Lock()
	c := s.lanes[a.Lane]
	if c == nil {
		s.mu.Unlock()
		// Commit is never called for a lane whose lease we lost, so an unknown lane here is a
		// contract violation and not a fence.
		return fault.Contract(fault.OpCommitSource, fmt.Errorf("commit for unknown lane %q", a.Lane))
	}
	if off <= c.committed {
		s.mu.Unlock()
		return nil // never move a cursor backwards
	}
	s.abandonedSeen += a.Abandoned
	s.mu.Unlock()

	if a.Abandoned > 0 {
		// See the block above. This note is the only honest thing available: the source cannot make
		// the decision the interface asks it to make, so it says so where an operator can see it.
		s.rt.Note(connector.Event{
			At:       time.Now(),
			Kind:     connector.EventDegraded,
			Severity: fault.Indeterminate,
			Lane:     a.Lane,
			Message:  "advancing a destructive upstream past abandoned records",
			Detail: fmt.Sprintf("%d of %d records in this ack were abandoned and Ack carries no "+
				"attribution, so this source cannot tell a by-design best-effort drop from a "+
				"dead-letter; it is advancing, which is unsafe for the latter", a.Abandoned, a.Records),
		})
	}

	// The actual destructive act. In a real slot source this is a feedback message that frees WAL.
	if err := s.releaseUpstream(ctx, off); err != nil {
		// Escalated, not logged and dropped: the engine classifies, retries per policy, and raises
		// commit_failed. "We delivered the data and lost the progress record" is unreachable.
		return err
	}

	s.mu.Lock()
	c.committed = off
	s.mu.Unlock()

	// A second, source-shaped write of the same fact, which is what AutoPersist exists for: the
	// core already persisted the lane cursor in phase two before calling here.
	return s.persist.Commit(ctx, a)
}

func (s *cdcSrc) releaseUpstream(ctx context.Context, off uint64) error {
	if ctx.Err() != nil {
		return fault.Internal(fault.OpCommitSource, ctx.Err())
	}
	_ = off
	return nil
}

// Heartbeat holds the slot open while nothing is arriving. Required by Build for a PrunesOnCommit
// source in a pipeline that gates a stream lane behind a scan, and useful here regardless.
func (s *cdcSrc) Heartbeat(ctx context.Context, lane record.LaneID, idle time.Duration) error {
	s.mu.Lock()
	c := s.lanes[lane]
	var off uint64
	if c != nil {
		off = c.committed
	}
	s.mu.Unlock()
	if c == nil {
		return nil
	}
	_ = idle
	return s.releaseUpstream(ctx, off)
}

// Nack observes terminal failures in this source's own vocabulary. It is keyed on position rather
// than on record.RecordID, which is right — a RecordID is assigned after Read returned and this
// source has never seen one.
//
// It does NOT rescue F6: a Nack carries no node, so a nack raised by the best-effort metrics branch
// and one raised by the warehouse are the same value.
func (s *cdcSrc) Nack(_ context.Context, lane record.LaneID, ns []connector.Nack) error {
	for i := range ns {
		s.rt.Note(connector.Event{
			At:       time.Now(),
			Kind:     connector.EventNote,
			Severity: ns[i].Class,
			Lane:     lane,
			Message:  "upstream change reached a terminal disposition",
			Detail:   fmt.Sprintf("%s after %d attempts: %s", ns[i].Position.Label, ns[i].Attempts, ns[i].Reason),
		})
	}
	return nil
}

func (s *cdcSrc) Backlog(_ context.Context, lane record.LaneID) (connector.Backlog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.lanes[lane]
	if c == nil {
		return connector.Backlog{}, nil
	}
	lag := 250 * time.Millisecond
	return connector.Backlog{
		Records:      connector.Count(0),
		Exact:        true,
		AsOf:         time.Now(),
		EventTimeLag: &lag,
	}, nil
}

func (s *cdcSrc) Close(context.Context) error { return nil }

// Ack is a local alias so the argument list of Commit reads as the interface declares it. It exists
// only for the comment block above Commit to be able to quote the type by name.
type Ack = connector.Ack

// ============================================================================================
// F1 + F2 — the exactly-once warehouse, and why this pipeline cannot be built.
// ============================================================================================
//
// THE SIGNATURES THAT BLOCK ME:
//
//	// spec/spec.go
//	type Spec struct {
//	    Guarantee connector.Guarantee   `json:"guarantee"`   // ONE, pipeline-wide
//	    Streams   []StreamConfig        `json:"streams"`
//	}
//	type StreamConfig struct {
//	    Stream record.StreamName
//	    Read   []connector.LaneKind
//	    Write  connector.DestMode      // ONE, pipeline-wide, per stream
//	    Keys   [][]string
//	    Dedupe *DedupeConfig
//	}
//
//	// engine/negotiate.go, inside `for id, sk := range res.sinks`
//	for _, want := range s.Streams {
//	    if !hasMode(c.Modes, want.Write) { ...ERROR... }
//	}
//	if sk.Committer == nil && sk.Token == nil {
//	    if requested == connector.ExactlyOnce { ...ERROR... }
//	}
//
// F1. This warehouse is a keyed, upserting destination. It declares Modes {Upsert, Append} and
// RequiresCompleteImages, and if it is handed DestAppend it will do the thing Opening.Guarantee's
// doc invites and fail Open loudly, because appending a CDC update stream to a warehouse table
// produces a table of duplicates rather than a mirror. The queue branch is a log: it declares
// Modes {Append} and nothing else, because a queue cannot upsert. The metrics branch is Append too.
//
// stream "orders" therefore needs Write=Upsert at the warehouse node and Write=Append at the other
// two, simultaneously. StreamConfig.Write is one value for the whole pipeline and negotiate checks
// it against EVERY sink, so:
//
//	Write: Upsert -> "stream "orders" is configured to upsert, which sink "hostile_queue" does not
//	                 support" AND the same for hostile_metrics. Two errors. REFUSED.
//	Write: Append -> the warehouse is handed DestAppend and, per its own contract, must fail Open.
//
// There is no third option, and the measured refusal is worse than two errors: the DEAD-LETTER
// sink is refused as well. hostile_dlq writes fault envelopes and has nothing to do with stream
// "orders", but the stream table is pipeline-global, so a dead-letter destination can never satisfy
// a pipeline whose data stream is anything but append. A pipeline is thereby forbidden from having
// both an upserting sink and a dead-letter route, which is the combination the retry policy's own
// default (terminal: dead_letter) asks every operator to build.
//
// Two sink nodes with two codec blocks is the documented answer to
// per-sink WIRE FORMAT (stage-standard `codec`), and it works — but destination MODE is not a
// stage-standard field, it lives in the pipeline-wide Streams table, so the same trick is
// unavailable for the thing that actually decides whether the data is correct. That asymmetry is
// the defect: architecture.md's own claim that "source-side mode and destination-side mode are
// ORTHOGONAL, which is what makes M times N source/sink combinations free" holds only for M x N
// with N = 1.
//
// F2. Independently: Spec.Guarantee is one requested tier. The warehouse is a Committer and the
// commission says exactly-once. Requesting ExactlyOnce makes negotiate emit a hard capability
// diagnostic for hostile_queue and another for hostile_metrics, because neither implements
// Committer or TokenSink — correctly, for a pipeline-wide reading of the request, and fatally for
// this graph. Requesting AtLeastOnce builds, and then Opening.Guarantee arrives here as
// at_least_once while the engine is in fact driving PrepareCommit/Commit against this sink on
// every checkpoint. Negotiated.Guarantee is documented as "what did I actually get"; with fan-out
// it is neither what the operator got on any branch nor a truthful minimum, because ADR 0024's
// waiver (which is the ONLY sanctioned way to run below the request, and which is the one type in
// the whole design carrying a NodeID) cannot be written down — see F9.
//
// SMALLEST FIX for both, additive, and it breaks no already-written connector because it changes
// no connector-facing interface at all:
//
//	type StreamConfig struct {
//	    // Node scopes this entry to one graph node. Empty means every node, which is what an
//	    // existing single-sink spec means today, so existing specs are unchanged.
//	    Node record.NodeID `json:"node,omitempty"`
//	    ...
//	}
//	type Spec struct {
//	    // Guarantee is the pipeline-wide REQUESTED tier. A terminal node may override it.
//	    Guarantee  connector.Guarantee            `json:"guarantee"`
//	    PerNode    map[record.NodeID]connector.Guarantee `json:"per_node_guarantee,omitempty"`
//	}
//
// negotiate then folds Min per TERMINAL rather than over the whole sink set, and
// telemetry.Negotiated grows a per-node breakdown (see F11, which is the same edit). Opening.Streams
// is already per-sink at the point the engine builds it, so a sink sees no change whatsoever:
// ConfiguredStream already carries {Stream, Mode, Keys} and the engine simply starts filling it
// from the node-scoped entries. Sink, Source, Buffer and Transform are untouched and stay frozen.
// ============================================================================================

func init() {
	registry.AddSink(Reg, registry.SinkDef[*warehouse]{
		Meta: registry.Meta{
			Name:    "hostile_warehouse",
			Version: "1.0.0",
			Title:   "Hostile warehouse",
			Summary: "Stages to a temp table and publishes on commit. Batches at 30s. Exactly-once.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec().
			Field(config.Field{
				Name: "table", Type: config.TypeString,
				Description: "Destination table. Created by Prepare before any data flows.",
			}).
			Field(config.Field{
				Name: "commit_every", Type: config.TypeDuration, Default: "30s", Optional: true,
				Description: "How often staged data is published. See F7: the engine's checkpoint " +
					"cadence is pipeline-wide, so this field cannot actually be honoured.",
			}),
		Caps: connector.SinkCaps{
			Caps:                   connector.Caps{APIVersion: connector.APIVersion},
			MaxConcurrency:         4,
			MaxRequestRecords:      10000,
			MaxRequestBytes:        64 << 20,
			Idempotent:             true,
			PartialFailure:         true,
			Modes:                  []connector.DestMode{connector.DestUpsert, connector.DestAppend},
			RequiresCompleteImages: true,
			RequiresKey:            true,
			SchemaChanges:          []schema.ChangeKind{schema.CreateStream, schema.AddField},
			Flushes:                true,
			AppliesSchema:          true,
			Commits:                true,
			KeepsState:             true,
			Prepares:               true,
		},
		New: func(_ context.Context, c *config.Config) (*warehouse, error) {
			w := &warehouse{
				table:       config.Must[string](c, "table"),
				commitEvery: config.Must[time.Duration](c, "commit_every"),
				staged:      map[uint64]*stagedBatch{},
			}
			return w, c.Err()
		},
	})
}

// stagedBatch is one temp-table load: what is in it, and whether it has been published.
type stagedBatch struct {
	name      string
	records   []record.RecordID
	firstRec  record.RecordID
	lastRec   record.RecordID
	lanes     map[record.LaneID]struct{}
	published bool
}

type warehouse struct {
	table       string
	commitEvery time.Duration

	rt   connector.SinkRuntime
	mode connector.DestMode
	tier connector.Guarantee

	mu sync.Mutex
	// open is the temp table currently accumulating writes; staged is everything minted by
	// PrepareCommit and not yet published, keyed by the checkpoint id that minted it.
	open       *stagedBatch
	staged     map[uint64]*stagedBatch
	lastCommit time.Time
	nextTemp   uint64
	deferrals  uint64
}

func (w *warehouse) Open(_ context.Context, rt connector.SinkRuntime, o connector.Opening) error {
	w.rt = rt
	w.tier = o.Guarantee

	// A sink MAY assert on the negotiated tier, and this one must: upserting a CDC stream into a
	// mirror table is the only correct behaviour, and being handed DestAppend means the pipeline
	// would produce a table of duplicates.
	//
	// F1 in one branch of one if-statement: with three sinks of three different mode requirements,
	// this assertion is unsatisfiable no matter what the operator configures.
	for _, cs := range o.Streams {
		if cs.Mode != connector.DestUpsert {
			return fault.Contract(fault.OpOpen, fmt.Errorf(
				"stream %q was configured %s; this sink mirrors a changelog and requires upsert",
				cs.Stream, cs.Mode))
		}
		w.mode = cs.Mode
	}

	// F2 made visible at runtime. The Committer machinery below is live either way, so a tier of
	// at_least_once here is the core under-reporting, not the sink under-delivering.
	if o.Guarantee < connector.ExactlyOnce {
		rt.Note(connector.Event{
			At:       time.Now(),
			Kind:     connector.EventDegraded,
			Severity: fault.PermanentContract,
			Message:  "this sink implements Committer but was told the pipeline is " + o.Guarantee.String(),
			Detail: "Spec.Guarantee is one tier for the whole pipeline and negotiate folds Min over " +
				"every sink, so a fan-out whose other branches are weaker misreports this branch",
		})
	}

	if o.Restored != nil {
		rt.Log().Info("resuming", "checkpoint", *o.Restored, "table", w.table)
	}
	w.lastCommit = time.Now()
	return nil
}

func (w *warehouse) Prepare(_ context.Context, streams []connector.ConfiguredStream, ss []schema.Entry) error {
	_ = ss
	for _, s := range streams {
		if len(s.Keys) == 0 {
			return fault.Contract(fault.OpPrepare, fmt.Errorf(
				"stream %q has no keys; this sink declares RequiresKey", s.Stream))
		}
	}
	return nil
}

// ============================================================================================
// F12 — declaring SchemaApplier honestly is what refuses the pipeline.
// ============================================================================================
//
// THE CODE THAT BLOCKS ME:
//
//	// engine/negotiate.go, inside `for id, sk := range res.sinks`
//	if sk.SchemaApp != nil {
//	    for _, k := range driftKinds(s) {
//	        if !hasChangeKind(c.SchemaChanges, k) { ...ERROR... }
//	    }
//	}
//
//	// engine/negotiate.go
//	func driftKinds(s *spec.Spec) []schema.ChangeKind {
//	    ...for every kind: if s.Drift.Permits(k) { out = append(out, k) }...
//	}
//
// This sink can create a table and add a column. It cannot rename a column, alter nullability or
// alter a primary key, because its destination cannot. So it declares
// SchemaChanges: {CreateStream, AddField} — which is the honest, documented use of the field:
// "SchemaChanges declares which change kinds ApplySchemaChange can perform."
//
// Under the DEFAULT drift policy (DriftLenient is the zero value), driftKinds returns every
// non-destructive kind — {CreateStream, AddField, RenameField, AlterNullability, AlterKeys} — and
// Build emits three hard capability errors and REFUSES THE PIPELINE. Measured: 3 refusals under
// lenient, 3 under try_evolve, 7 under evolve.
//
// The incentive is exactly inverted. Delete ApplySchemaChange from this type, set AppliesSchema
// false, and the identical pipeline builds — because `sk.SchemaApp != nil` gates the whole check.
// A sink that says nothing about schema is accepted; a sink that says something true is refused.
// Volunteering a capability is strictly worse than withholding it, which is the opposite of what
// the capability mechanism is for, and a connector author's rational response is to under-declare.
//
// DriftTryEvolve makes it plainly a bug rather than a strict-by-design choice. Its own doc:
// "DriftTryEvolve applies what is supported and emits an event for the rest." That is a policy
// whose stated semantics REQUIRE tolerating a partial SchemaChanges set — and Permits() returns
// the same set for TryEvolve as for Lenient, so negotiate refuses it for not supporting kinds the
// policy has already promised to emit an event about instead of applying. DriftLenient has a
// milder version of the same problem: its doc says destructive kinds are "rewritten as an additive
// pair rather than refused outright", i.e. lenient already contains fallback logic, and the check
// does not know that.
//
// The root confusion is Permits() answering the wrong question. Permits means "may this policy
// apply k at all", and negotiate needs "must the sink be able to apply k for this policy to
// work".
//
// SMALLEST FIX, additive, and it accepts strictly MORE pipelines so no valid spec breaks:
//
//	// Requires reports whether this policy needs a sink to be able to apply k, as opposed to
//	// merely permitting it. TryEvolve requires nothing — it emits an event for what a sink
//	// cannot do — and Ignore and Fail apply nothing at all.
//	func (p DriftPolicy) Requires(k schema.ChangeKind) bool {
//	    switch p {
//	    case DriftIgnore, DriftFail, DriftTryEvolve:
//	        return false
//	    default:
//	        return p.Permits(k)
//	    }
//	}
//
// and negotiate's driftKinds switches from Permits to Requires. A sink that cannot apply everything
// then either chooses TryEvolve and gets events, or chooses Lenient/Evolve and is told at submit
// time — which is what the diagnostic was reaching for. NO ALREADY-WRITTEN CONNECTOR BREAKS, and
// nothing that builds today stops building.
// ============================================================================================

func (w *warehouse) ApplySchemaChange(_ context.Context, ch schema.Change) error {
	switch ch.Kind {
	case schema.CreateStream, schema.AddField:
		return nil
	default:
		return fault.Contract(fault.OpSchemaApply,
			fmt.Errorf("%s is not a change this sink can apply", ch.Kind))
	}
}

func (w *warehouse) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	if req.Count == 0 {
		return connector.WriteResult{}, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.open == nil {
		w.nextTemp++
		w.open = &stagedBatch{
			name:  fmt.Sprintf("%s__stage_%d", w.table, w.nextTemp),
			lanes: map[record.LaneID]struct{}{},
		}
	}
	for i := range req.Records {
		r := req.Records[i]
		if len(w.open.records) == 0 {
			w.open.firstRec = r.ID
		}
		w.open.lastRec = r.ID
		w.open.records = append(w.open.records, r.ID)
		w.open.lanes[r.Lane] = struct{}{}
	}
	// A clean return from a Flusher sink's Write means ACCEPTED, not durable: Flusher's own doc
	// says "The core does not settle records on Write for a Flusher sink; it settles them on the
	// Flush that covers them."
	//
	// Note in passing that Sink.Write's quadrant table says the opposite for the same return value
	// ("every record in the request is DURABLE"). The two contracts are reconcilable only by
	// knowing which interfaces the type implements, which is exactly the mechanism the design
	// wants — but the quadrant table does not say "unless you are a Flusher", and a first-time
	// author reading Write's doc alone would settle for durability it has not achieved. One
	// sentence on Sink.Write fixes it; it is documentation, not interface shape, so it is not
	// counted among the findings.
	return connector.AllWritten(req.Count), nil
}

// ============================================================================================
// F7 + F8 — a 30s durability cadence is inexpressible, and Flush cannot defer.
// ============================================================================================
//
// THE SIGNATURE THAT BLOCKS ME:
//
//	Flush(ctx context.Context, reason FlushReason) (WriteResult, error)
//
// F7. The commission says this sink batches at 30s. Nothing this sink can do makes that happen.
// The core "calls Flush before every checkpoint", and the checkpoint cadence is
// engine.Deps.FlushInterval, which is one value for the whole pipeline (default 1s) and lives on
// the DEPLOYMENT assembly, not on the node. The same interval governs the 1ms queue branch, which
// wants it short to keep its replay window small. One knob, two branches, opposite requirements:
//
//	FlushInterval 1s  -> this sink publishes 30x more often than its destination wants, which for a
//	                     warehouse means 30x the commit cost and 30x the small files.
//	FlushInterval 30s -> the queue branch's measured replay window grows to 30s of traffic.
//
// The stage-standard fields the registry appends per sink node are retry, when_full, codec,
// batching, max_in_flight and dedupe. `batching` is config.BatchPolicy — it sizes the REQUEST, not
// the durability boundary — so there is no per-node lever for this.
//
// SMALLEST FIX, additive, no connector change: add a stage-standard field the ENGINE reads, exactly
// as it already reads retry and when_full per node:
//
//	FieldDurabilityEvery = "durability_every"   // duration, optional, default: the pipeline's
//	                                            // FlushInterval; a node may only LENGTHEN it
//
// The engine calls Flush (and PrepareCommit) for that node on its own multiple of the checkpoint
// cadence and settles that node's records on the flush that covers them, which the ledger already
// models per group. Every existing sink omits the field and behaves identically.
//
// F8. Given F7, the workaround a connector author reaches for is to no-op the flushes it does not
// want. THAT CANNOT BE EXPRESSED. Flush returns (WriteResult, error) and its doc says "A partial
// flush returns (res, err) naming what did not make it, exactly as Write does" — importing Write's
// quadrant table, under which:
//
//	(res, nil) with res.Failed empty          == everything covered is DURABLE. A lie here, and the
//	                                             one that advances the cursor past a temp table.
//	(res, err) with res.Failed empty          == nothing covered is durable, THE WHOLE REQUEST IS
//	                                             RETRIED. Correct about durability and wrong about
//	                                             what to do: the data is already staged, so a
//	                                             retry re-Writes 30s of records every second.
//	(res, err) with res.Failed = every record == same, per record, and each one burns a retry
//	                                             attempt against RetryPolicy.MaxAttempts, so a
//	                                             30s-cadence sink dead-letters its own healthy
//	                                             data on the fourth checkpoint.
//
// And no fault.Class fits: TransientUpstream would violate the retry-safety obligation in
// reverse — the effect DID land, in the staging table — Indeterminate is a lie because this sink
// knows exactly what it holds, and the permanent classes are terminal. There is no class meaning
// "accepted, not yet durable, do not resend".
//
// SMALLEST FIX, additive, breaks nothing: one sentinel, next to the four that already exist in
// fault, plus one sentence on Flusher.
//
//	// ErrFlushDeferred reports that this flush covered nothing new: no record it was given has
//	// become durable, none has failed, and none must be resent. The engine settles nothing and
//	// calls Flush again at the next boundary. It is the honest answer for a sink whose durability
//	// cadence is coarser than the checkpoint cadence.
//	ErrFlushDeferred = New(TransientInternal, OpFlush, errFlushDeferred)
//
// Existing Flusher sinks never return it, so they are unaffected. Taking F7 makes F8 unnecessary
// for THIS case; F8 is still worth having, because a sink whose remote decides the boundary (an
// object store finalising on size) has the same problem and no config value can predict it.
//
// The code below returns the honest-but-wasteful (res, err) shape, and counts the deferrals so the
// cost is measurable rather than asserted.
// ============================================================================================

func (w *warehouse) Flush(_ context.Context, reason connector.FlushReason) (connector.WriteResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	due := time.Since(w.lastCommit) >= w.commitEvery
	forced := reason == connector.FlushEndOfInput || reason == connector.FlushDrain ||
		reason == connector.FlushSchemaChange

	if w.open == nil || len(w.open.records) == 0 {
		return connector.WriteResult{}, nil
	}
	if !due && !forced {
		w.deferrals++
		// See F8. There is no way to say "deferred", so this says "nothing is durable" and pays for
		// it with a full re-delivery of everything staged since the last real commit.
		return connector.WriteResult{Written: 0}, fault.Internal(fault.OpFlush, errors.New(
			"staged but not yet published; this sink's durability boundary is longer than the "+
				"checkpoint interval and Flush cannot express a deferral"))
	}

	n := int64(len(w.open.records))
	w.open.published = false // published by Commit, not here: this sink is a two-phase committer
	w.lastCommit = time.Now()
	return connector.WriteResult{
		Written:   n,
		Bytes:     n * 256,
		DestToken: w.open.name,
	}, nil
}

// ============================================================================================
// F5 — Committable persists RecordIDs that do not survive the restart it exists for.
// ============================================================================================
//
// THE SIGNATURE THAT BLOCKS ME:
//
//	type Committable struct {
//	    Checkpoint uint64
//	    Handle     record.Blob
//	    Lanes      []record.LaneID
//	    FirstRec   record.RecordID   // <-- persisted
//	    LastRec    record.RecordID   // <-- persisted
//	    Records    int64
//	    Expires    time.Time
//	}
//
// Committable's doc: "Every committable names the lanes and the record-id range it covers, so a
// failed commit can be dead-lettered with the records named and the blast radius is exact."
//
// record/ids.go on RecordID: "It is NOT durable and never appears in persisted state. Durable
// identity is Origin.Key plus the lane cursor."
//
// Both cannot be true. Committable is persisted INSIDE engine.Checkpoint (Committables
// map[uint64][]connector.Committable) and record.NewAllocator's doc says firstID and firstGroup
// "are not durable and must never be used as a persisted key". So after the crash — the only
// situation in which a recovered committable is ever examined — FirstRec and LastRec name records
// in a generation that no longer exists, and the "exact blast radius" is empty. Every path that
// needs it (AbortStale raising a degraded condition, DispositionDeadLetter routing the covered
// records) is a path that runs after a restart.
//
// Two further problems in the same three fields, which I hit while writing PrepareCommit:
//
//	*  The set of records in one staged artifact is not an INTERVAL. With MaxConcurrency 4, a
//	   partitioning batcher and per-record retries, the ids in one temp table are a scattered
//	   subset, so [FirstRec, LastRec] over-claims — it names records that went to a different
//	   staging table, or to a different branch of the fan-out entirely.
//	*  There is no Node. Checkpoint.Committables is keyed by checkpoint id only, so with two
//	   Committer sinks in one graph the engine cannot route a recovered committable back to the
//	   sink that minted it. Committer.Commit(ctx, cs) has no "not mine" answer — only AbortStale is
//	   documented as tolerating unrecognised handles. This graph has one Committer so it is not
//	   blocking here, but "multiple sinks" is a stated end-state goal and this is the same field
//	   set one entry short.
//
// SMALLEST FIX, additive, no connector-facing method changes:
//
//	type Committable struct {
//	    ...
//	    // Node is the sink node that minted this committable, so a recovered checkpoint routes it
//	    // back to its author rather than offering it to every Committer.
//	    Node record.NodeID `json:"node"`
//
//	    // Keys are the durable identities the artifact covers (Origin.Key), for the dead-letter
//	    // route. FirstRec and LastRec are generation-local and are DISPLAY ONLY.
//	    Keys [][]byte `json:"keys,omitempty"`
//	}
//
// plus a doc correction on FirstRec/LastRec saying they are display-only. A connector that fills
// only the existing fields still compiles and still commits; it loses only the dead-letter naming
// it never actually had. NO ALREADY-WRITTEN CONNECTOR BREAKS.
// ============================================================================================

func (w *warehouse) PrepareCommit(_ context.Context, p connector.CommitPoint) ([]connector.Committable, error) {
	id := p.ID
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.open == nil || len(w.open.records) == 0 {
		return nil, nil
	}
	b := w.open
	w.open = nil
	w.staged[id] = b

	lanes := make([]record.LaneID, 0, len(b.lanes))
	for l := range b.lanes {
		lanes = append(lanes, l)
	}
	return []connector.Committable{{
		Checkpoint: id,
		Handle:     record.Blob{Version: cursorV1, Bytes: []byte(b.name)},
		Lanes:      lanes,
		// F5: these two are the ONLY identification of the covered records, they are
		// generation-local, and they over-claim because the covered set is not an interval.
		FirstRec: b.firstRec,
		LastRec:  b.lastRec,
		Records:  int64(len(b.records)),
		Expires:  time.Now().Add(2 * time.Hour),
	}}, nil
}

func (w *warehouse) Commit(_ context.Context, cs []connector.Committable) ([]connector.CommitOutcome, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]connector.CommitOutcome, 0, len(cs))
	for _, c := range cs {
		name := string(c.Handle.Bytes)
		b := w.staged[c.Checkpoint]
		switch {
		case b == nil || b.name != name:
			// Not ours, or already retired. The subsuming contract says a higher id subsumes every
			// lower one and an implementation must behave as if a non-confirmed checkpoint never
			// happened, so an unknown handle here is AlreadyCommitted and never an error.
			out = append(out, connector.CommitOutcome{Handle: c.Handle, Disposition: connector.DispositionAlreadyCommitted})
		case b.published:
			out = append(out, connector.CommitOutcome{Handle: c.Handle, Disposition: connector.DispositionAlreadyCommitted})
		default:
			b.published = true
			delete(w.staged, c.Checkpoint)
			out = append(out, connector.CommitOutcome{Handle: c.Handle, Disposition: connector.DispositionCommitted})
		}
	}
	return out, nil
}

// AbortStale discards artifacts a recovered checkpoint mentions and this sink no longer recognises.
// Abort is NOT discard of the data: the artifacts stay, and the next successful checkpoint covers a
// longer span.
func (w *warehouse) AbortStale(_ context.Context, cs []connector.Committable) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, c := range cs {
		if b := w.staged[c.Checkpoint]; b != nil && !b.published {
			delete(w.staged, c.Checkpoint)
		}
	}
	return nil
}

func (w *warehouse) SnapshotState(_ context.Context, id uint64) ([]record.Blob, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = id
	if w.open == nil {
		return nil, nil
	}
	return []record.Blob{{Version: cursorV1, Bytes: []byte(w.open.name)}}, nil
}

// ============================================================================================
// F4 — two WriterState sinks in one graph share one unkeyed slice.
// ============================================================================================
//
// THE SIGNATURES THAT BLOCK ME:
//
//	// connector/sink_optional.go
//	type WriterState interface {
//	    SnapshotState(ctx context.Context, id uint64) ([]record.Blob, error)
//	    RestoreState(ctx context.Context, bs []record.Blob) error
//	}
//
//	// engine/checkpoint.go
//	type Checkpoint struct {
//	    WriterState    []record.Blob                     `json:"writer_state,omitempty"`   // NO KEY
//	    TransformState map[record.NodeID][]record.Blob    `json:"transform_state,omitempty"` // KEYED
//	}
//
// This graph has TWO sinks declaring KeepsState: this warehouse (an open staging table) and the
// queue sink (an in-flight producer batch). Both are asked to SnapshotState at every checkpoint and
// both are handed []record.Blob at RestoreState. There is one flat slice for the whole pipeline, so
// the engine either concatenates and hands each sink the union — and my RestoreState below is
// written to survive that, by tag-checking every blob and ignoring what is not mine, which is
// exactly the "unwrap processor whose only job is to undo the nesting" the design says it does not
// want — or it keeps only one sink's state and silently loses the other's staging table.
//
// The tell that this is an oversight and not a decision is one line up: TransformState is keyed by
// NodeID for precisely this reason. Two representations of one concept, and the doc for LaneState
// on the same page is about not letting differently-lifetimed state share a field.
//
// Note that Committables has the same shape problem one field over (F5), which is what makes this a
// checkpoint-model defect rather than a typo.
//
// SMALLEST FIX, one type change in an internal package, zero connector-facing change:
//
//	WriterState map[record.NodeID][]record.Blob `json:"writer_state,omitempty"`
//
// engine.Checkpoint is internal, so nothing outside the module can be broken by it, and the
// four-part format contract is satisfied because a JSON array and a JSON object under the same key
// need a version bump — which CheckpointFormat exists for and which no shipped state has yet used.
// The WriterState INTERFACE does not change at all: SnapshotState/RestoreState already deal only in
// their own blobs. NO ALREADY-WRITTEN CONNECTOR BREAKS.
// ============================================================================================

func (w *warehouse) RestoreState(_ context.Context, bs []record.Blob) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Defensive tag-check, only because Checkpoint.WriterState is not keyed by node. A connector
	// author should never have to write this, and a reviewer should read it as the finding.
	for _, b := range bs {
		if b.Version != cursorV1 {
			continue
		}
		name := string(b.Bytes)
		if len(name) <= len(w.table) || name[:len(w.table)] != w.table {
			// Somebody else's blob arrived in our slice. Ignoring it is the only safe move, and it
			// means the other sink's state is silently absent from ITS restore too.
			continue
		}
		w.open = &stagedBatch{name: name, lanes: map[record.LaneID]struct{}{}}
	}
	return nil
}

func (w *warehouse) Close(context.Context) error { return nil }

// ============================================================================================
// The at-least-once queue sink. 1ms writes, partial failure, resumable in-flight batch.
// ============================================================================================

func init() {
	registry.AddSink(Reg, registry.SinkDef[*queueSink]{
		Meta: registry.Meta{
			Name:    "hostile_queue",
			Version: "1.0.0",
			Title:   "Hostile queue",
			Summary: "Append-only log at 1ms per write. At-least-once, per-record failure reporting.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec().
			Field(config.Field{
				Name: "topic", Type: config.TypeString,
				Description: "Destination topic. Append-only; this sink cannot upsert.",
			}),
		Caps: connector.SinkCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			MaxConcurrency:    16,
			MaxRequestRecords: 1000,
			Idempotent:        true,
			PartialFailure:    true,
			// Append and nothing else. This is the other half of F1: no configuration of
			// StreamConfig.Write satisfies both this and hostile_warehouse.
			Modes:      []connector.DestMode{connector.DestAppend},
			KeepsState: true,
		},
		New: func(_ context.Context, c *config.Config) (*queueSink, error) {
			q := &queueSink{topic: config.Must[string](c, "topic")}
			return q, c.Err()
		},
	})
}

type queueSink struct {
	topic string
	rt    connector.SinkRuntime

	mu       sync.Mutex
	inFlight []record.RecordID
	sent     uint64
}

func (q *queueSink) Open(_ context.Context, rt connector.SinkRuntime, o connector.Opening) error {
	q.rt = rt
	for _, cs := range o.Streams {
		if cs.Mode != connector.DestAppend {
			return fault.Contract(fault.OpOpen, fmt.Errorf(
				"stream %q was configured %s; a log can only append", cs.Stream, cs.Mode))
		}
	}
	return nil
}

func (q *queueSink) Write(ctx context.Context, req *connector.Request) (connector.WriteResult, error) {
	if req.Count == 0 {
		return connector.WriteResult{}, nil
	}
	q.mu.Lock()
	q.inFlight = q.inFlight[:0]
	for i := range req.Records {
		q.inFlight = append(q.inFlight, req.Records[i].ID)
	}
	q.mu.Unlock()

	if err := ctx.Err(); err != nil {
		// Nothing claimed durable, Failed empty: the whole request is retried. Safe because this
		// sink declares Idempotent and forwards Request.IdempotencyKey.
		return connector.WriteResult{}, fault.Internal(fault.OpWrite, err)
	}

	// One record in every 997 is rejected by the broker for being too large: a genuine per-record
	// permanent mapping failure, which is what PartialFailure exists to report.
	var failed []fault.RecordFault
	for i := range req.Records {
		if (uint64(req.Records[i].ID)+q.sent)%997 == 0 {
			failed = append(failed, fault.RecordFault{
				Record: req.Records[i].ID,
				Class:  fault.PermanentMapping,
				Op:     fault.OpWrite,
				User:   "record exceeds the topic's maximum message size",
				Dev:    "topic=" + q.topic,
			})
		}
	}
	q.sent += uint64(req.Count)

	res := connector.WriteResult{
		Failed:  failed,
		Written: int64(req.Count) - int64(len(failed)),
		Bytes:   int64(req.UncompressedBytes),
	}
	// RECONCILIATION IS MANDATORY, and it is cheap to self-check here rather than to be told by a
	// PermanentContract at runtime.
	if ok, want := res.Reconcile(req.Count); !ok {
		return res, fault.Bug(fault.OpWrite, fmt.Errorf("miscounted: Written=%d want=%d", res.Written, want))
	}
	if len(failed) > 0 {
		return res, fault.Mapping(fault.OpWrite, fmt.Errorf("%d of %d records were rejected", len(failed), req.Count))
	}
	return res, nil
}

// SnapshotState carries the in-flight batch across a restart. It is the second WriterState
// implementation in this graph, and therefore the other half of F4.
func (q *queueSink) SnapshotState(_ context.Context, _ uint64) ([]record.Blob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.inFlight) == 0 {
		return nil, nil
	}
	b := make([]byte, 0, 8*len(q.inFlight))
	for _, id := range q.inFlight {
		var tmp [8]byte
		binary.BigEndian.PutUint64(tmp[:], uint64(id))
		b = append(b, tmp[:]...)
	}
	return []record.Blob{{Version: cursorV1, Bytes: b}}, nil
}

func (q *queueSink) RestoreState(_ context.Context, bs []record.Blob) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.inFlight = q.inFlight[:0]
	for _, b := range bs {
		// F4 again, from the other side, and worse here: this sink's blob is a multiple of 8 bytes
		// and the warehouse's is a table name. There is no shared tag, so "is this mine" is a
		// heuristic. len%8 == 0 is true of plenty of table names.
		if b.Version != cursorV1 || len(b.Bytes)%8 != 0 {
			continue
		}
		for i := 0; i+8 <= len(b.Bytes); i += 8 {
			q.inFlight = append(q.inFlight, record.RecordID(binary.BigEndian.Uint64(b.Bytes[i:i+8])))
		}
	}
	return nil
}

func (q *queueSink) Close(context.Context) error { return nil }

// ============================================================================================
// F6 (part two) + F9 — the best-effort sink, and why it cannot be declared best-effort.
// ============================================================================================
//
// THE SIGNATURES THAT BLOCK ME:
//
//	// spec/node.go — the ENTIRE routing vocabulary
//	type Edge struct {
//	    From   record.NodeID
//	    Select EdgeSelect      // main | failed | all. Nothing about progress.
//	}
//
//	// ledger/ledger.go
//	func (l *Ledger) Expand(g record.GroupID, n uint32) error   // one ref per fan-out edge
//	// and in Settle: `if g.settled >= g.refs { ...retire the group... }`
//
// The commission: "the best-effort sink must never hold back the pipeline". The settlement graph
// says otherwise. Every fan-out edge adds a reference to the source batch's group, and the group —
// and therefore the lane's prefix, and therefore the source's cursor — resolves only when EVERY
// reference has settled. A metrics endpoint that is slow, or down, holds a reference open for as
// long as it is slow or down.
//
// The pipeline is not permanently stuck, and I want to be precise about that because it would be
// easy to overstate: retry exhausts, the node's retry.terminal fires, the record settles Abandoned,
// tracker.Abandon advances the prefix. So the requirement is MECHANICALLY satisfiable. What is not
// available is a way to say it, and the cost of saying it by accident is high:
//
//	*  Every shed record raises Ack.Abandoned, which this pipeline's source is told to treat as a
//	   reason to refuse a destructive commit (F6 part one). The two findings are one defect seen
//	   from both ends.
//	*  Checkpoint.Header.RecordsIn/RecordsOut — "the per-checkpoint reconciliation pair. A
//	   persistent divergence is the only cheap way to notice a sink that silently drops, and it is
//	   CHECKED, not merely recorded" — diverges permanently and by design. The one cheap
//	   detector of silent loss is pinned on by the configuration and is now useless for this
//	   pipeline forever.
//	*  The shed is charged to a fault.Class. The honest answer is "I chose not to deliver this",
//	   and the class set is an OWNERSHIP taxonomy with no such member: TransientUpstream is a lie
//	   about the effect, PermanentUpstream blames a remote that never refused anything, and
//	   PermanentMapping blames the record. I use PermanentUpstream below and it is wrong.
//	*  Latency, not just loss: while the metrics endpoint is merely SLOW rather than failing, its
//	   reference is open and the source's cursor is held for the full retry ladder — up to
//	   MaxAttempts times the backoff ceiling — for records the operator explicitly does not care
//	   about. when_full: drop_newest does not help, because it fires on a full EDGE, not on a slow
//	   sink that is still accepting.
//
// F9, which is the same problem's escape hatch and is also missing. ADR 0024 says the sanctioned
// way to run a pipeline below its requested tier is an operator-signed Downgrade, and
// telemetry.Downgrade is the one type in the whole design that carries a `Node record.NodeID` —
// i.e. it was designed for exactly this per-branch case. But:
//
//	*  spec.Spec has no Downgrades field. There is nowhere to write one down.
//	*  engine.negotiate never mentions Downgrades; telemetry.Negotiated.Downgrades is only ever nil.
//
// So the documented, ADR-blessed answer to "the metrics branch is best-effort and I know it" is
// unreachable from config, and F1/F2's refusal has no override.
//
// SMALLEST FIX. Two additive changes, neither touching a required interface:
//
//  1. One field on spec.Edge, which is where it belongs — best-effort is a property of THIS
//     BRANCH, not of the metrics component, since the same sink is progress-bearing in a
//     single-sink pipeline:
//
//	    // BestEffort means records on this edge do not participate in settlement: the engine does
//	    // not Expand a reference for it, its outcomes never reach Ack.Abandoned, and its
//	    // throughput and drops are counted under their own series. It is refused on an edge that
//	    // is the graph's only route for a record, so it can never be silent loss.
//	    BestEffort bool `json:"best_effort,omitempty"`
//
//     Engine changes are all internal: skip the Expand for that edge; count drops separately;
//     exclude the node from negotiate's Min fold and from the RecordsIn/RecordsOut pair. Zero
//     value is today's behaviour, so every existing spec and every existing connector is
//     unchanged. This alone makes the commissioned pipeline correct.
//
//  2. Wire the waiver that already exists: add `Downgrades []Downgrade` to spec.Spec and have
//     negotiate consume them. Downgrade must move out of telemetry into spec or connector to
//     avoid spec importing telemetry (telemetry imports connector and record only today, so
//     either direction compiles, but a config-store type depending on the read model is the wrong
//     way round). Also additive.
// ============================================================================================

func init() {
	registry.AddSink(Reg, registry.SinkDef[*metricsSink]{
		Meta: registry.Meta{
			Name:    "hostile_metrics",
			Version: "1.0.0",
			Title:   "Hostile metrics endpoint",
			Summary: "Best-effort statsd-shaped endpoint. Sheds load rather than applying backpressure.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec().
			Field(config.Field{
				Name: "endpoint", Type: config.TypeString,
				Description: "Where samples are posted. Loss here is expected and acceptable.",
			}).
			Field(config.Field{
				Name: "shed_above_inflight", Type: config.TypeInt, Default: 256, Optional: true,
				Description: "Requests in flight above which this sink stops trying. See F6: the " +
					"only way to express the shed is to fail records into a terminal disposition.",
			}),
		Caps: connector.SinkCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			MaxConcurrency:    32,
			MaxRequestRecords: 5000,
			Idempotent:        true,
			PartialFailure:    true,
			Modes:             []connector.DestMode{connector.DestAppend},
		},
		New: func(_ context.Context, c *config.Config) (*metricsSink, error) {
			m := &metricsSink{
				endpoint: config.Must[string](c, "endpoint"),
				shedAt:   int(config.Must[int64](c, "shed_above_inflight")),
			}
			return m, c.Err()
		},
	})
}

type metricsSink struct {
	endpoint string
	shedAt   int

	rt       connector.SinkRuntime
	shed     connector.Counter
	mu       sync.Mutex
	inFlight int
	shedRecs uint64
}

func (m *metricsSink) Open(_ context.Context, rt connector.SinkRuntime, o connector.Opening) error {
	m.rt = rt
	_ = o
	c, err := rt.Metrics().Counter("shed_records")
	if err != nil {
		// The core owns metric naming; a rejected name is the core telling me so, and it is not
		// worth failing Open over.
		rt.Log().Warn("could not register the shed counter", "error", err)
	} else {
		m.shed = c
	}
	rt.Note(connector.Event{
		At:      time.Now(),
		Kind:    connector.EventNote,
		Message: "this sink is best-effort, and canal has no way for it to say so",
		Detail: "there is no Edge.BestEffort and no SinkCaps flag, so shedding is expressed as a " +
			"terminal per-record failure, which pollutes Ack.Abandoned and the RecordsIn/RecordsOut " +
			"reconciliation pair for the whole pipeline",
	})
	return nil
}

func (m *metricsSink) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	if req.Count == 0 {
		return connector.WriteResult{}, nil
	}
	m.mu.Lock()
	m.inFlight++
	over := m.inFlight > m.shedAt
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.inFlight--
		m.mu.Unlock()
	}()

	if !over {
		return connector.AllWritten(req.Count), nil
	}

	// THE SHED. Everything below this line is the workaround, and it is the finding.
	//
	// To avoid holding the lane's prefix open I must make every record reach a TERMINAL
	// disposition, which means naming every one of them in Failed with a class whose Terminal() is
	// true. PermanentUpstream is the least-wrong of the four and it is still wrong: the endpoint
	// did not refuse, was not asked, and Blames() will file this under "their system".
	//
	// It also depends on the operator having set this node's retry block to
	// {max_attempts: 1, terminal: drop}. With the stage-standard default (dead_letter) these
	// records travel the failed edge and are written to the DLQ sink, so a best-effort branch under
	// load quietly becomes the pipeline's highest-volume writer.
	failed := make([]fault.RecordFault, 0, len(req.Records))
	for i := range req.Records {
		failed = append(failed, fault.RecordFault{
			Record: req.Records[i].ID,
			Class:  fault.PermanentUpstream,
			Op:     fault.OpWrite,
			User:   "shed under load; this endpoint is best-effort",
			Dev:    "endpoint=" + m.endpoint,
		})
	}
	m.mu.Lock()
	m.shedRecs += uint64(len(failed))
	m.mu.Unlock()
	if m.shed != nil {
		m.shed.Add(float64(len(failed)))
	}
	res := connector.WriteResult{Failed: failed, Written: 0}
	if ok, want := res.Reconcile(req.Count); !ok {
		return res, fault.Bug(fault.OpWrite, fmt.Errorf("miscounted: Written=0 want=%d", want))
	}
	return res, fault.Permanent(fault.OpWrite, fmt.Errorf("shed %d records under load", len(failed)))
}

func (m *metricsSink) Close(context.Context) error { return nil }

// ============================================================================================
// F10 — case (2): two sources merging into one sink.
// ============================================================================================
//
// THE SIGNATURE THAT BLOCKS ME:
//
//	// spec/spec.go
//	type StreamConfig struct {
//	    Stream record.StreamName        // NO NODE SCOPE
//	    Read   []connector.LaneKind
//	    ...
//	}
//
//	// engine/negotiate.go, inside `for id, src := range res.sources`
//	for _, want := range s.Streams {
//	    for _, k := range want.Read {
//	        if !hasLaneKind(c.LaneKinds, k) { ...ERROR... }
//	    }
//	}
//
// WHAT WORKS, first, because most of this case does. The topology is fine: validateGraph puts no
// input-count restriction on a sink node, so one sink with two source inputs passes with no special
// case — only buffers (exactly one input) and non-Regroups transforms (at most one) are
// constrained, both correctly. And the SHARED CHECKPOINT works, in the sense that matters:
// engine.Checkpoint.Lanes is a map over the whole pipeline's lanes, serialised into ONE durable
// record, written through StateStore.SetMany, which is documented as "all or nothing across the
// whole map. One SQL transaction, one bbolt transaction, one etcd transaction." Both sources'
// cursors already advance atomically together. I went looking for the two-independently-committed-
// stores failure and it is genuinely absent.
//
// WHAT BREAKS. Streams are declared pipeline-globally and validated against EVERY source. mergeSrc
// is registered twice below, as hostile_merge_scan (which can do an initial scan and a tail) and
// hostile_merge_tail (a tail only, because its upstream has no snapshot API — a webhook, a socket,
// a metrics scrape). An operator who wants a scan on the first source's stream has to write
//
//	Streams: [{Stream: "left", Read: [Scan, Stream]}, {Stream: "right", Read: [Stream]}]
//
// and negotiate then checks "left"'s Read against hostile_merge_tail, which cannot scan, and emits
//
//	stream "left" asks for a scan lane, which source "hostile_merge_tail" cannot produce
//
// and REFUSES THE PIPELINE. The stream belongs to the other source. Nothing in the spec can say so.
// Renaming does not help, because the check is a cross product and not a lookup. It is worse when
// the two sources share a stream name — which is the DEFAULT case, since record.DefaultStream is
// the constant "default" that "a source with exactly one logical stream announces", so any two
// single-stream sources collide on it — and the collision is silent: one entry configures both.
//
// The same cross product is what produces F1 on the sink side, so one fix serves both.
//
// This is a dual representation in the design's own terms. The core already knows stream identity
// is node-scoped: DedupeConfig's doc states that the durable key is "(tenant, pipeline,
// source-node, stream, layer, identity)" precisely because keying on a bare id let two connectors
// discard each other's events. StreamConfig is the same concept with the node dropped.
//
// SMALLEST FIX: the `Node record.NodeID` field on StreamConfig from F1. Empty means every node,
// which is what a single-source spec means today, so every existing spec keeps working. negotiate
// changes from a cross product to a lookup. No connector-facing type changes; Opening.Streams is
// already built per sink.
//
// NOT REPORTED, deliberately: that source A's cursor cannot be held back to source B's — a
// consistent cut across the two lanes. ADR 0010 lists "fan-in with shared state across lanes" as a
// deliberate rejection with its reasoning, and Snapshot(ctx, id) on stateful nodes is named as the
// insertion point if that ever changes. A documented non-goal is not a breakage.
// ============================================================================================

// mergeSrc is one half of the fan-in pair. The same Go type is registered twice under two names
// with two capability sets, which is legal and is the cheapest way to show the cross-product
// failure: the two halves differ only in what they declare they can read.
type mergeSrc struct {
	stream  record.StreamName
	canScan bool

	rt   connector.SourceRuntime
	mu   sync.Mutex
	lane record.LaneID
	off  uint64
	done bool
}

func mergeSpec() *config.Spec {
	return config.NewSpec().
		Field(config.Field{
			Name: "stream", Type: config.TypeString,
			Description: "Logical stream this half announces. Two halves may legally choose the same " +
				"name, at which point one pipeline-wide Streams entry silently configures both.",
		})
}

func init() {
	scanCaps := connector.SourceCaps{
		Caps:                connector.Caps{APIVersion: connector.APIVersion},
		DefaultOrdering:     connector.OrderingPrefix,
		Boundedness:         []connector.Boundedness{connector.Bounded, connector.Unbounded},
		LaneKinds:           []connector.LaneKind{connector.LaneKindScan, connector.LaneKindStream},
		MaxLanes:            8,
		UpstreamRetention:   connector.RetentionUnbounded,
		UnitAssignment:      connector.UnitsDynamic,
		Heartbeats:          true,
		ProducesEventTime:   true,
		ComparablePositions: true,
		Replayable:          true,
		MidLaneResume:       true,
	}
	tailCaps := scanCaps
	// The whole difference, and the whole problem: this half cannot scan.
	tailCaps.LaneKinds = []connector.LaneKind{connector.LaneKindStream}
	tailCaps.Boundedness = []connector.Boundedness{connector.Unbounded}
	tailCaps.MaxLanes = 1

	registry.AddSource(Reg, registry.SourceDef[*mergeSrc]{
		Meta: registry.Meta{
			Name: "hostile_merge_scan", Version: "1.0.0",
			Title:   "Hostile merge half (scan capable)",
			Summary: "One input of the fan-in pair. Can snapshot then tail.",
			Support: registry.SupportCommunity,
		},
		Spec: mergeSpec(),
		Caps: scanCaps,
		New: func(_ context.Context, c *config.Config) (*mergeSrc, error) {
			return &mergeSrc{stream: record.StreamName(config.Must[string](c, "stream")), canScan: true}, c.Err()
		},
	})
	registry.AddSource(Reg, registry.SourceDef[*mergeSrc]{
		Meta: registry.Meta{
			Name: "hostile_merge_tail", Version: "1.0.0",
			Title:   "Hostile merge half (tail only)",
			Summary: "The other input of the fan-in pair. Its upstream has no snapshot API.",
			Support: registry.SupportCommunity,
		},
		Spec: mergeSpec(),
		Caps: tailCaps,
		New: func(_ context.Context, c *config.Config) (*mergeSrc, error) {
			return &mergeSrc{stream: record.StreamName(config.Must[string](c, "stream"))}, c.Err()
		},
	})
}

func (s *mergeSrc) Open(ctx context.Context, rt connector.SourceRuntime) error {
	s.rt = rt
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil {
		return err
	}
	if len(as) > 0 {
		off, err := decodeOffset(as[0].Cursor.Token)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.lane, s.off = as[0].ID, off
		s.mu.Unlock()
		return nil
	}

	kind := connector.LaneKindStream
	bound := connector.Unbounded
	group := record.LaneGroup("tail")
	if s.canScan {
		kind, bound, group = connector.LaneKindScan, connector.Bounded, record.LaneGroup("scan")
	}
	id, err := rt.Lanes().Announce(ctx, connector.LaneSpec{
		Name:        "merge:" + string(s.stream),
		Stream:      s.stream,
		Kind:        kind,
		Ordering:    connector.OrderingPrefix,
		Boundedness: bound,
		Group:       group,
		Label:       string(s.stream) + " half of the merge",
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.lane = id
	s.mu.Unlock()
	return nil
}

func (s *mergeSrc) Read(ctx context.Context, dst *record.Batch) error {
	s.mu.Lock()
	lane, off, done := s.lane, s.off, s.done
	s.mu.Unlock()
	if done {
		return fault.ErrEndOfInput
	}

	dst.Reset()
	dst.Lane = lane
	for dst.Len() < 32 && ctx.Err() == nil {
		r := dst.Add()
		if r == nil {
			break
		}
		r.Payload = record.StructPayload(record.Map{
			"stream": record.String(string(s.stream)),
			"n":      record.Uint(off + uint64(dst.Len())),
		})
		r.EventTime = time.Now()
	}
	if dst.Len() == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.canScan {
			s.mu.Lock()
			s.done = true
			s.mu.Unlock()
			dst.EndOfLane = true
		}
		return nil
	}

	next := off + uint64(dst.Len())
	s.mu.Lock()
	s.off = next
	s.mu.Unlock()

	var order [8]byte
	binary.BigEndian.PutUint64(order[:], next)
	dst.Position = record.Position{
		Token: encodeOffset(next),
		Order: order[:],
		Safe:  true,
		At:    time.Now(),
		Label: fmt.Sprintf("%s@%d", s.stream, next),
	}
	return nil
}

// Commit is a no-op: this half's progress IS canal's cursor, which the core persisted in phase two
// before calling here, and its upstream retains regardless.
func (s *mergeSrc) Commit(context.Context, connector.Ack) error { return nil }

func (s *mergeSrc) Heartbeat(context.Context, record.LaneID, time.Duration) error { return nil }

func (s *mergeSrc) Close(context.Context) error { return nil }

// ============================================================================================
// The fan-in sink. Two inputs, one destination, one shared checkpoint.
// ============================================================================================
//
// This one fits. It sees no lanes and no positions, it never learns that it has two inputs, and it
// could not get progress wrong if it tried. The only thing it CANNOT do — and this is F10 from the
// sink side rather than a separate finding — is apply a different DestMode per input, because
// Opening.Streams is a flat []ConfiguredStream keyed on stream name with no node, so a merge whose
// two halves want upsert and append respectively is the fan-out problem again in reverse.
// ============================================================================================

func init() {
	registry.AddSink(Reg, registry.SinkDef[*mergeSink]{
		Meta: registry.Meta{
			Name: "hostile_merge_sink", Version: "1.0.0",
			Title:   "Hostile merge destination",
			Summary: "Two inputs, one table, one checkpoint. Exactly-once by two-phase commit.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec().Field(config.Field{
			Name: "table", Type: config.TypeString,
			Description: "Destination table both halves are merged into.",
		}),
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			MaxConcurrency: 2,
			Idempotent:     true,
			PartialFailure: false,
			Modes:          []connector.DestMode{connector.DestAppend, connector.DestUpsert},
			Commits:        true,
		},
		New: func(_ context.Context, c *config.Config) (*mergeSink, error) {
			return &mergeSink{table: config.Must[string](c, "table"), staged: map[uint64][]record.LaneID{}}, c.Err()
		},
	})
}

type mergeSink struct {
	table string

	mu     sync.Mutex
	lanes  map[record.LaneID]int64
	staged map[uint64][]record.LaneID
}

func (k *mergeSink) Open(_ context.Context, _ connector.SinkRuntime, _ connector.Opening) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.lanes = map[record.LaneID]int64{}
	return nil
}

func (k *mergeSink) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	if req.Count == 0 {
		return connector.WriteResult{}, nil
	}
	k.mu.Lock()
	// A Request's Records each carry their own Lane, so the merge is observable here even though
	// the sink is told nothing about topology. This is the design working.
	for i := range req.Records {
		k.lanes[req.Records[i].Lane]++
	}
	k.mu.Unlock()
	return connector.AllWritten(req.Count), nil
}

func (k *mergeSink) PrepareCommit(_ context.Context, p connector.CommitPoint) ([]connector.Committable, error) {
	id := p.ID
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.lanes) == 0 {
		return nil, nil
	}
	var lanes []record.LaneID
	var n int64
	for l, c := range k.lanes {
		lanes = append(lanes, l)
		n += c
	}
	k.staged[id] = lanes
	k.lanes = map[record.LaneID]int64{}
	return []connector.Committable{{
		Checkpoint: id,
		Handle:     record.Blob{Version: cursorV1, Bytes: []byte(fmt.Sprintf("%s@%d", k.table, id))},
		// Both halves' lanes in one committable, which is what "shared checkpoint" means at this
		// layer and which the type expresses cleanly.
		Lanes:   lanes,
		Records: n,
		Expires: time.Now().Add(time.Hour),
	}}, nil
}

func (k *mergeSink) Commit(_ context.Context, cs []connector.Committable) ([]connector.CommitOutcome, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]connector.CommitOutcome, 0, len(cs))
	for _, c := range cs {
		if _, ok := k.staged[c.Checkpoint]; !ok {
			out = append(out, connector.CommitOutcome{Handle: c.Handle, Disposition: connector.DispositionAlreadyCommitted})
			continue
		}
		delete(k.staged, c.Checkpoint)
		out = append(out, connector.CommitOutcome{Handle: c.Handle, Disposition: connector.DispositionCommitted})
	}
	return out, nil
}

func (k *mergeSink) AbortStale(_ context.Context, cs []connector.Committable) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, c := range cs {
		delete(k.staged, c.Checkpoint)
	}
	return nil
}

func (k *mergeSink) Close(context.Context) error { return nil }

// ============================================================================================
// A dead-letter sink, needed only because the stage-standard retry default is dead_letter and
// negotiate refuses a dead-lettering policy with nowhere to dead-letter. It is here to make the
// probe's graphs valid, not because it is interesting.
// ============================================================================================

func init() {
	registry.AddSink(Reg, registry.SinkDef[*dlqSink]{
		Meta: registry.Meta{
			Name: "hostile_dlq", Version: "1.0.0",
			Title: "Hostile dead-letter", Summary: "Terminal destination for the failed edge.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec().Field(config.Field{
			Name: "path", Type: config.TypeString,
			Description: "Where failed records and their envelopes are written.",
		}),
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			MaxConcurrency: 1,
			Idempotent:     true,
			PartialFailure: false,
			Modes:          []connector.DestMode{connector.DestAppend},
		},
		New: func(_ context.Context, c *config.Config) (*dlqSink, error) {
			return &dlqSink{path: config.Must[string](c, "path")}, c.Err()
		},
	})
}

type dlqSink struct{ path string }

func (d *dlqSink) Open(context.Context, connector.SinkRuntime, connector.Opening) error { return nil }

func (d *dlqSink) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	return connector.AllWritten(req.Count), nil
}

func (d *dlqSink) Close(context.Context) error { return nil }
