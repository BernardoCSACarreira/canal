// The StartAfter gate must not open over records still in flight.
//
// gatedOnLocked has always demanded a DURABLY finished predecessor — Finished set and FinishedAt
// non-zero — and laneCtl.Finish used to write exactly that row the moment a source said finish,
// while the lane's last records were still unsettled. The ledger's half of the finish (the final
// acknowledgement) waited; the durable half did not, and read.go carried a comment naming the gap.
// These tests pin the closed behaviour end to end at the laneCtl seam: a finish with work in flight
// leaves the row unfinished and the gate shut, and the retirement the flush loop performs after the
// last settlement is what opens it.
package engine

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/example/memstore"
	"github.com/BernardoCSACarreira/canal/internal/ledger"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// finishFixture is a STANDALONE worker (no coordinator, every lane held) with one node's lane
// table wired to a real ledger, which is the pairing the deferral crosses.
type finishFixture struct {
	ctl    *laneCtl
	led    *ledger.Ledger
	state  store.StateStore
	fields specRefFields
}

func newFinishFixture(t *testing.T) *finishFixture {
	t.Helper()
	st := memstore.New()
	fields := specRefFields{tenant: "acme", pipeline: "p1"}
	deps := Deps{
		State: st, Worker: "w1",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	led := ledger.New(ledger.Config{Tenant: "acme", Pipeline: "p1", DefaultBudget: 16,
		GroupTTL: time.Minute})
	t.Cleanup(func() { _ = led.Close() })
	go func() {
		for range led.Acks() {
		}
	}()

	ctl := newLaneCtl(deps, fields, "in",
		func(record.LaneID, connector.Ordering, int) error { return nil },
		func(record.LaneID) connector.Admission { return connector.Admission{} },
		led.RequestRetire,
		newLeaseTable(deps, fields, 1))
	return &finishFixture{ctl: ctl, led: led, state: st, fields: fields}
}

// addLane registers a lane in the table and, when ord is given, in the ledger.
func (f *finishFixture) addLane(t *testing.T, id record.LaneID, spec connector.LaneSpec, track bool) {
	t.Helper()
	f.ctl.mu.Lock()
	f.ctl.lanes[id] = &laneRecord{Spec: spec}
	f.ctl.order = append(f.ctl.order, id)
	f.ctl.mu.Unlock()
	if track {
		if err := f.led.Lane(id, connector.OrderingPrefix, 16, connector.WhenFullBlock); err != nil {
			t.Fatalf("ledger.Lane(%s): %v", id, err)
		}
	}
}

// assignedIDs answers which lanes a source would be offered right now.
func (f *finishFixture) assignedIDs(t *testing.T) map[record.LaneID]bool {
	t.Helper()
	as, err := f.ctl.Assigned(context.Background())
	if err != nil {
		t.Fatalf("Assigned: %v", err)
	}
	out := map[record.LaneID]bool{}
	for _, a := range as {
		out[a.ID] = true
	}
	return out
}

// row reads a lane's durable record back from the store.
func (f *finishFixture) row(t *testing.T, id record.LaneID) laneRecord {
	t.Helper()
	key := store.LaneKey(f.fields.tenant, f.fields.pipeline, id)
	got, err := f.state.Get(context.Background(), []store.Key{key})
	if err != nil {
		t.Fatalf("reading lane %s: %v", id, err)
	}
	v, ok := got[key.String()]
	if !ok {
		t.Fatalf("lane %s has no durable row at all", id)
	}
	var rec laneRecord
	if err := json.Unmarshal(v.Value, &rec); err != nil {
		t.Fatalf("decoding lane %s: %v", id, err)
	}
	return rec
}

func TestAFinishWithRecordsInFlightDoesNotOpenTheGate(t *testing.T) {
	f := newFinishFixture(t)
	f.addLane(t, "scan-1", connector.LaneSpec{Name: "scan-1", Stream: "orders",
		Group: "scan", Kind: connector.LaneKindScan}, true)
	f.addLane(t, "tail", connector.LaneSpec{Name: "tail", Stream: "orders",
		Kind: connector.LaneKindStream, StartAfter: []record.LaneGroup{"scan"}}, false)

	// Give the predecessor a durable row to retire against, as announce would have.
	if err := f.ctl.mutate(context.Background(), "scan-1", func(*laneRecord) error { return nil }); err != nil {
		t.Fatalf("seeding scan-1's row: %v", err)
	}

	if ids := f.assignedIDs(t); !ids["scan-1"] || ids["tail"] {
		t.Fatalf("fixture premise broken: want scan-1 offered and tail gated, got %v", ids)
	}

	// The engine's order for a final batch: mark the acknowledgement flag, then admit. The batch
	// stays UNSETTLED — a sink has accepted nothing — which is the window the gate must survive.
	f.led.FinishLane("scan-1")
	a := record.NewAllocator("acme", "p1", "in", "scan-1", "orders", 1, 1)
	b := record.NewBatch(a, 3)
	for i := 0; i < 3; i++ {
		b.Add()
	}
	b.Position = record.Position{Order: []byte{9}, Token: record.Blob{Version: 1, Bytes: []byte{9}}, Safe: true}
	if err := f.led.Admit(context.Background(), b); err != nil {
		t.Fatalf("Admit: %v", err)
	}

	if err := f.ctl.Finish(context.Background(), "scan-1"); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// The finish is visible exactly one way: the lane leaves the read set. Its row stays
	// unfinished, the successor stays gated, and nothing is retirable yet.
	ids := f.assignedIDs(t)
	if ids["scan-1"] {
		t.Error("a finished lane is still offered to the source; Finish did not leave the read set")
	}
	if ids["tail"] {
		t.Error("the StartAfter gate opened while the predecessor's records were in flight — the " +
			"successor can now read over data no sink has accepted")
	}
	if rec := f.row(t, "scan-1"); rec.Finished || !rec.FinishedAt.IsZero() {
		t.Errorf("the durable row was written before settlement (Finished=%v FinishedAt=%v); a "+
			"crash here resumes with the tail skipped rather than re-read", rec.Finished, rec.FinishedAt)
	}
	if got := f.led.Retirable(); len(got) != 0 {
		t.Fatalf("Retirable offered %v before the lane settled", got)
	}

	// The last group settles; the flush loop's half runs; the gate opens.
	outs := make([]ledger.Outcome, 0, 3)
	for _, r := range b.Records {
		outs = append(outs, ledger.Outcome{Record: r.Origin().ID, Node: "out",
			Disposition: ledger.Delivered})
	}
	f.led.Settle(outs)

	retirable := f.led.Retirable()
	if len(retirable) != 1 || retirable[0] != "scan-1" {
		t.Fatalf("Retirable = %v after settlement, want [scan-1]", retirable)
	}
	if err := f.ctl.retire(context.Background(), "scan-1"); err != nil {
		t.Fatalf("retire: %v", err)
	}

	if rec := f.row(t, "scan-1"); !rec.Finished || rec.FinishedAt.IsZero() {
		t.Fatalf("the retirement did not reach the durable row: %+v", rec)
	}
	ids = f.assignedIDs(t)
	if ids["scan-1"] {
		t.Error("a retired lane came back into the read set")
	}
	if !ids["tail"] {
		t.Error("the gate stayed shut after its predecessor settled and retired durably; the " +
			"successor never starts")
	}
}

// A finish with NOTHING in flight retires inline — the ledger will never surface the lane again,
// so deferring it would strand the row unfinished and the gate shut forever.
func TestAFinishWithNothingInFlightRetiresInline(t *testing.T) {
	f := newFinishFixture(t)
	f.addLane(t, "scan-1", connector.LaneSpec{Name: "scan-1", Stream: "orders",
		Group: "scan", Kind: connector.LaneKindScan}, true)
	if err := f.ctl.mutate(context.Background(), "scan-1", func(*laneRecord) error { return nil }); err != nil {
		t.Fatalf("seeding scan-1's row: %v", err)
	}

	if err := f.ctl.Finish(context.Background(), "scan-1"); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if rec := f.row(t, "scan-1"); !rec.Finished || rec.FinishedAt.IsZero() {
		t.Fatalf("an already-drained finish was not made durable inline: %+v", rec)
	}
	if got := f.led.Retirable(); len(got) != 0 {
		t.Fatalf("the inline retirement was also handed to the flush loop: %v", got)
	}
}
