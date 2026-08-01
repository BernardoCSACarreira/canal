package parallelsnapshot

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/ledger"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

// TestB1_OriginKeyIsSettable is BREAKAGE B1's regression test, inverted.
//
// B1 said: a source cannot populate Origin.Key, so StableKeys, dedupe, RequiresKey,
// Request.IdempotencyKey and every upsert sink are unreachable from any connector, and
// `r.Origin().Key = k` does not even compile. The fix is (*record.Record).SetKey and
// SetUpstream, under SetHandle's existing source-only-before-Read rule.
//
// The assertion that matters is not that the methods exist but that what they write REACHES
// record.Ref — the identity a sink reports outcomes against and the thing SinkCaps.RequiresKey
// demands. A setter writing a field nothing reads would satisfy the compiler and nothing else.
func TestB1_OriginKeyIsSettable(t *testing.T) {
	a := record.NewAllocator("default", "p", "n", "chunk/orders/0", "orders", 1, 1)
	b := record.NewBatch(a, 4)
	r := b.Add()
	if r == nil {
		t.Fatal("Add returned nil on an empty batch")
	}

	r.SetKey([]byte("pk-1"))
	r.SetUpstream([]byte("mysql:binlog:42"))

	if got := string(r.Origin().Key); got != "pk-1" {
		t.Fatalf("Origin().Key is %q, want %q", got, "pk-1")
	}
	if got := string(r.Origin().Upstream); got != "mysql:binlog:42" {
		t.Fatalf("Origin().Upstream is %q, want %q", got, "mysql:binlog:42")
	}
	if got := string(r.Ref().Key); got != "pk-1" {
		t.Fatalf("Ref().Key is %q: the key does not reach the identity a sink reports against", got)
	}

	// The SETTLEMENT half of Origin stays unwritable, which is the property the two setters must
	// not have cost. Origin() still returns a copy and there is still no way to move a record
	// between lanes, groups or reference counts.
	rt := reflect.TypeOf(r)
	for i := 0; i < rt.NumMethod(); i++ {
		switch rt.Method(i).Name {
		case "SetLane", "SetGroup", "SetRefs", "SetID", "SetOrigin", "SetStream":
			t.Fatalf("%s exists: settlement identity became writable, which SetKey must never buy",
				rt.Method(i).Name)
		}
	}
	t.Logf("*record.Record exports %d methods: two write identity, none writes settlement", rt.NumMethod())
}

// TestB2_RetargetingABatchIsRefusedLoudly is BREAKAGE B2's other half, inverted.
//
// It is the whole argument in six lines: the batch's Lane is settable, the record's is
// not, and a source serving more than one lane through one batch mislabels provenance
// silently.
func TestB2_RetargetingABatchIsRefusedLoudly(t *testing.T) {
	const (
		allocLane = record.LaneID("chunk/orders/0")
		otherLane = record.LaneID("chunk/customers/7")
	)

	a := record.NewAllocator("default", "p", "n", allocLane, "orders", 1, 1)
	b := record.NewBatch(a, 4)

	// A source with 32 chunk lanes ready and one batch does exactly this.
	b.Lane = otherLane
	r := b.Add()

	if b.Lane != otherLane {
		t.Fatalf("Batch.Lane is %q; the field stopped being settable", b.Lane)
	}
	if r.Origin().Lane != allocLane {
		t.Fatalf("Origin().Lane is %q, want %q: BREAKAGE B2 is stale", r.Origin().Lane, allocLane)
	}
	if r.Origin().Stream != "orders" {
		t.Fatalf("Origin().Stream is %q, want \"orders\"", r.Origin().Stream)
	}

	t.Logf("Batch.Lane=%q  Origin().Lane=%q  Origin().Stream=%q  Dest=%q",
		b.Lane, r.Origin().Lane, r.Origin().Stream, r.Dest)

	// THE REPAIR: the mislabelling is no longer SILENT. The ledger refuses the batch at the
	// one place every batch passes through, naming both lanes, so a source that retargets
	// Batch.Lane instead of implementing LaneReader fails loudly on its first admission
	// rather than corrupting 33350 of 33500 records' provenance.
	l := ledger.New(ledger.Config{Tenant: "default", Pipeline: "p", DefaultBudget: 64})
	defer l.Close()
	if err := l.Lane(otherLane, connector.OrderingPrefix, 64, connector.WhenFullBlock); err != nil {
		t.Fatalf("Lane: %v", err)
	}
	err := l.Admit(context.Background(), b)
	if err == nil {
		t.Fatal("the ledger accepted a batch whose records were stamped for another lane; " +
			"silent settlement corruption is back")
	}
	if fault.ClassOf(err) != fault.PermanentContract {
		t.Fatalf("the refusal classifies as %s, want permanent_contract: a mislabelled batch is a "+
			"structural disagreement, not a transient one", fault.ClassOf(err))
	}
	if !strings.Contains(err.Error(), string(allocLane)) || !strings.Contains(err.Error(), string(otherLane)) {
		t.Fatalf("the refusal names neither lane: %v", err)
	}
	t.Logf("ledger.Admit refuses it: %v", err)
	t.Log("a source serving several lanes now implements connector.LaneReader and is handed " +
		"one pre-bound batch per lane, so retargeting one batch is never the answer")
}

// TestB3_EmptyPositionedBatchAdvancesTheLane is BREAKAGE B3's regression test, inverted.
//
// B3 said: a prefix lane cannot advance a position without emitting a record, because
// ledger.Admit floored refs and weight at one while Settle is keyed on record ids — so a
// position-only batch opened a group that could never settle and every later position in the
// lane queued behind it forever. A fully filtered tail, a chunk planner and a bounded source's
// own zero-record EndOfLane batch all hit it.
//
// The fix is ledger-side and interface-free: a zero-record batch is admitted with ZERO
// references and resolved at admission — still THROUGH the tracker, so it takes its place in
// the prefix in order rather than jumping ahead of outstanding work. This test asserts both
// halves, because the easy wrong fix is to commit such a position immediately.
func TestB3_EmptyPositionedBatchAdvancesTheLane(t *testing.T) {
	const lane = record.LaneID("tail")

	l := ledger.New(ledger.Config{Tenant: "default", Pipeline: "p", DefaultBudget: 64})
	defer l.Close()

	if err := l.Lane(lane, connector.OrderingPrefix, 64, connector.WhenFullBlock); err != nil {
		t.Fatalf("Lane: %v", err)
	}

	a := record.NewAllocator("default", "p", "n", lane, record.DefaultStream, 1, 1)
	at := func(n uint64, label string) record.Position {
		return record.Position{
			Token: record.Blob{Version: tokenV1, Bytes: []byte(label)},
			Order: orderOfLogPos(n),
			Safe:  true,
			At:    time.Now(),
			Label: label,
		}
	}

	// Batch 1: every event was filtered. Position only. It advances on its own, immediately:
	// there is nothing to wait for.
	filtered := record.NewBatch(a, 8)
	filtered.Position = at(100, "changelog 100")
	if err := l.Admit(context.Background(), filtered); err != nil {
		t.Fatalf("Admit refused a position-only batch: %v", err)
	}
	pos, ok := l.Flushable()[lane]
	if !ok {
		t.Fatal("a position-only batch did not advance the lane; the wedge is back")
	}
	if pos.Label != "changelog 100" {
		t.Fatalf("advanced to %q, want changelog 100", pos.Label)
	}

	// Batch 2: a real record later in the same lane. It must NOT advance the lane until it
	// settles.
	live := record.NewBatch(a, 8)
	r := live.Add()
	r.Payload = record.StructPayload(record.Map{"id": record.String("k1")})
	live.Position = at(200, "changelog 200")
	if err := l.Admit(context.Background(), live); err != nil {
		t.Fatalf("Admit 2: %v", err)
	}
	if pos, ok := l.Flushable()[lane]; ok {
		t.Fatalf("the lane advanced to %q before its record settled", pos.Label)
	}

	// Batch 3: another position-only batch, admitted while batch 2 is still outstanding. It must
	// queue BEHIND it: committing it would commit past an unsettled record, which is the one
	// thing the ledger exists to prevent.
	later := record.NewBatch(a, 8)
	later.Position = at(300, "changelog 300")
	if err := l.Admit(context.Background(), later); err != nil {
		t.Fatalf("Admit 3: %v", err)
	}
	if pos, ok := l.Flushable()[lane]; ok {
		t.Fatalf("a position-only batch jumped the prefix to %q past an unsettled record", pos.Label)
	}

	// Settling batch 2 releases both.
	l.Settle([]ledger.Outcome{{Record: r.Origin().ID, Disposition: ledger.Delivered}})
	pos, ok = l.Flushable()[lane]
	if !ok {
		t.Fatal("nothing flushable after the only outstanding record settled")
	}
	if pos.Label != "changelog 300" {
		t.Fatalf("prefix resolved to %q, want changelog 300", pos.Label)
	}
	st := l.Stats(lane)
	if st.InFlight != 0 {
		t.Fatalf("in-flight weight is %d after everything settled: a position-only batch charged the budget", st.InFlight)
	}
	t.Logf("prefix reached %q with %d pending groups and %d in flight: position-only batches "+
		"advance in order and cost no budget", pos.Label, st.PendingGroups, st.InFlight)
}

// TestRegistrationIsClean asserts the connector registers against the real registry with
// no over-declared capability and no spec-lint defect — so every breakage above is a
// finding about the interfaces and not about this file being wrong.
func TestRegistrationIsClean(t *testing.T) {
	e, ok := registry.Default.Source("stress_parallel_snapshot")
	if !ok {
		t.Fatal("source is not registered")
	}
	if len(e.Descriptor.Warnings) != 0 {
		t.Errorf("registered with warnings: %v", e.Descriptor.Warnings)
	}
	// BREAKAGE B1's repair, asserted at the capability level. Declaring StableKeys is only
	// honest once record.Record.SetKey exists, and registration lint separately requires the
	// derivation to be documented in Notes — so this pair is the whole contract.
	if !e.Caps.StableKeys {
		t.Error("StableKeys is not declared, but this source has stable primary keys and SetKey now exists")
	}
	if e.Meta.Notes == "" {
		t.Error("StableKeys is declared with empty Notes; registration lint requires the key derivation to be documented")
	}
	if e.Caps.MaxLanes != 0 {
		t.Errorf("MaxLanes is %d; BREAKAGE B5 says a data-dependent lane count must declare 0",
			e.Caps.MaxLanes)
	}
}

// TestReadIsAFunnel exercises Read's two settings against a stubbed upstream, so the
// dilemma in BREAKAGE B2 is executed rather than asserted. With honour_batch_lane the
// connector refuses to fill a lane the batch was not allocated for and blocks; without
// it, it fills and counts the mislabelling.
func TestReadIsAFunnel(t *testing.T) {
	s := &Source{
		chunkRows:   4,
		parallelism: 2,
		honourLane:  true,
		up:          stubUpstream{},
		chunks:      map[record.LaneID]*chunkLane{},
		plans:       map[record.LaneID]*planLane{},
		wake:        make(chan struct{}),
	}

	const (
		allocLane = record.LaneID("chunk/orders/0")
		readyLane = record.LaneID("chunk/customers/7")
	)
	s.chunks[readyLane] = &chunkLane{
		id:   readyLane,
		spec: chunkSpec{Table: "customers", Lo: "a", Hi: "b"},
		tok:  chunkToken{Phase: phaseHigh, Low: 10, High: 20},
		rows: []Row{{Key: "a1", Fields: map[string]string{"id": "a1"}, At: time.Now()}},
	}

	a := record.NewAllocator("default", "p", "n", allocLane, "orders", 1, 1)
	dst := record.NewBatch(a, 16)

	n, lane, _, _, _ := s.drainInto(dst, allocLane)
	if n != 0 {
		t.Fatalf("honour_batch_lane=true filled %d records for lane %q from lane %q's batch",
			n, lane, allocLane)
	}
	t.Log("honour_batch_lane=true: provenance correct, 31 other lanes starve behind this one")

	s.honourLane = false
	n, lane, _, _, _ = s.drainInto(dst, allocLane)
	if n != 1 {
		t.Fatalf("honour_batch_lane=false produced %d records, want 1", n)
	}
	if lane != readyLane {
		t.Fatalf("served lane %q, want %q", lane, readyLane)
	}
	if got := dst.Records[0].Origin().Lane; got != allocLane {
		t.Fatalf("Origin().Lane is %q, want the allocator's %q", got, allocLane)
	}
	t.Logf("honour_batch_lane=false: throughput reached, and the record for lane %q carries "+
		"Origin().Lane=%q and Origin().Stream=%q", readyLane, dst.Records[0].Origin().Lane,
		dst.Records[0].Origin().Stream)
}

// TestB2_ReadLanesEmitsForEveryLaneWithCorrectProvenance is BREAKAGE B2's regression test.
//
// B2 said: Read receives ONE batch whose allocator fixes Origin.Lane and Origin.Stream, so
// 32-way concurrent emission across 900 streams is inexpressible, and setting Batch.Lane
// silently mislabels every record. This drives the REAL connector through
// connector.LaneReader with a batch per lane and asserts the property that was impossible:
// every record's Origin.Lane equals the batch it came out of, and more than one lane is
// served by one call.
func TestB2_ReadLanesEmitsForEveryLaneWithCorrectProvenance(t *testing.T) {
	s := &Source{
		chunkRows:   4,
		parallelism: 4,
		up:          stubUpstream{},
		chunks:      map[record.LaneID]*chunkLane{},
		plans:       map[record.LaneID]*planLane{},
		wake:        make(chan struct{}),
	}

	// Four chunk lanes across two tables, each with rows ready. Under the old interface at
	// most one of these could be served per call, and only correctly if the engine happened
	// to allocate for it.
	type spec struct {
		lane   record.LaneID
		stream record.StreamName
		table  string
		key    string
	}
	specs := []spec{
		{"chunk/orders/0", "orders", "orders", "o-1"},
		{"chunk/orders/1", "orders", "orders", "o-2"},
		{"chunk/customers/0", "customers", "customers", "c-1"},
		{"chunk/customers/1", "customers", "customers", "c-2"},
	}
	dst := make([]*record.Batch, 0, len(specs))
	for i, sp := range specs {
		s.chunks[sp.lane] = &chunkLane{
			id:   sp.lane,
			spec: chunkSpec{Table: sp.table, Lo: "a", Hi: "z"},
			tok:  chunkToken{Phase: phaseHigh, Low: 10, High: 20},
			rows: []Row{{Key: sp.key, Fields: map[string]string{"id": sp.key}, At: time.Now()}},
		}
		a := record.NewAllocator("default", "p", "n", sp.lane, sp.stream,
			record.RecordID(1+i*1000), record.GroupID(1+i*1000))
		dst = append(dst, record.NewBatch(a, 16))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.ReadLanes(ctx, dst); err != nil {
		t.Fatalf("ReadLanes: %v", err)
	}

	served := 0
	for i, b := range dst {
		if b.Len() == 0 {
			continue
		}
		served++
		if b.Lane != specs[i].lane {
			t.Fatalf("batch %d reports lane %q, want %q", i, b.Lane, specs[i].lane)
		}
		for _, r := range b.Records {
			o := r.Origin()
			if o.Lane != specs[i].lane {
				t.Fatalf("record %d in batch %d carries Origin.Lane %q, want %q: the mislabelling is back",
					o.ID, i, o.Lane, specs[i].lane)
			}
			if o.Stream != specs[i].stream {
				t.Fatalf("record %d carries Origin.Stream %q, want %q", o.ID, o.Stream, specs[i].stream)
			}
			// B1's repair, observed end to end on the real connector rather than on a
			// synthetic allocator.
			if string(o.Key) != specs[i].key {
				t.Fatalf("record %d carries Origin.Key %q, want %q", o.ID, o.Key, specs[i].key)
			}
			if string(r.Ref().Key) != specs[i].key {
				t.Fatalf("Ref().Key is %q: the key does not reach the sink-facing identity", r.Ref().Key)
			}
		}
		if b.Position.IsZero() {
			t.Fatalf("batch %d for lane %q carries no position", i, b.Lane)
		}
	}
	if served < 2 {
		t.Fatalf("one ReadLanes call served %d lanes; the whole point is that it serves many", served)
	}
	t.Logf("one ReadLanes call served %d of %d lanes, every record stamped for the lane it came from",
		served, len(dst))

	// And the capability is declared, so the engine will actually call it.
	e, ok := registry.Default.Source("stress_parallel_snapshot")
	if !ok {
		t.Fatal("source is not registered")
	}
	if !e.Caps.ReadsLanes || e.Caps.ReadConcurrency != 32 {
		t.Fatalf("caps declare ReadsLanes=%v ReadConcurrency=%d; the interface is implemented but not declared, so the core will never call it",
			e.Caps.ReadsLanes, e.Caps.ReadConcurrency)
	}
	// Guard against the declaration and the implementation drifting apart, which is the
	// exact failure ADR 0031 generalises: the resolved handle is what the engine nil-checks.
	rs, err := registry.ResolveSource(e.Meta.Name, s, e.Caps)
	if err != nil {
		t.Fatalf("ResolveSource: %v", err)
	}
	if rs.LaneReader == nil {
		t.Fatal("ReadsLanes is declared but the resolved LaneReader handle is nil")
	}
	if got := rs.ReadConcurrency(8); got != 8 {
		t.Fatalf("ReadConcurrency(parallelism=8) is %d, want 8: the operator's cap must win downward", got)
	}
}
