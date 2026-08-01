package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
)

// THE CLOCK CHECK IS TESTED ON THE RECORD, because the record is where its effect lives and nothing
// outside this package can see it: record.Ref carries an id, a group, a lane, a stream and a key,
// and neither the adjusted EventTime nor the noted FieldChange is among them. A sink literally
// cannot observe a clamp.
//
// What IS observable from outside — which records reach which sink under reject, and the counter —
// is asserted end to end in clock_e2e_test.go. This file is the part that would otherwise be
// asserted by squinting at a log line.

func skewBatch(t *testing.T, now time.Time, offsets ...time.Duration) *record.Batch {
	t.Helper()
	a := record.NewAllocator("acme", "clk", "in", "lane-1", "lines", 1, 1)
	b := record.NewBatch(a, len(offsets))
	for _, d := range offsets {
		r := b.Add()
		if r == nil {
			t.Fatal("Add returned nil below the cap")
		}
		r.EventTime = now.Add(d)
	}
	return b
}

// testRunner is the smallest runner applyClock needs: a nil-safe instrument set and no sinks, which
// is enough for every arm except reject.
func testRunner(p spec.ClockPolicy) (*runner, clockCheck) {
	r := &runner{p: &Pipeline{spec: spec.Spec{Tenant: "acme", ID: "clk", Clock: p}, obs: &obs{pipeline: "clk"}}}
	return r, r.clockFor("in")
}

// A TIMESTAMP IN THE PAST IS HISTORY, NOT SKEW. max_skew is one-sided by definition — how far a
// source timestamp may LEAD the local clock — and a batch source replaying last year's data is the
// normal case. Clamping that would destroy the very field the policy claims to protect.
func TestThePastIsNeverSkew(t *testing.T) {
	now := time.Now()
	r, c := testRunner(spec.ClockPolicy{MaxSkew: time.Second, Behaviour: spec.ClockClamp})
	b := skewBatch(t, now, -time.Hour, -365*24*time.Hour, -time.Millisecond)

	out, err := r.applyClock(context.Background(), c, b)
	if err != nil {
		t.Fatalf("applyClock: %v", err)
	}
	if out.Len() != 3 {
		t.Fatalf("%d records survived, want all 3", out.Len())
	}
	for i, rec := range out.Records {
		if !rec.EventTime.Before(now) {
			t.Errorf("record %d's past event time was rewritten to %v", i, rec.EventTime)
		}
		if len(rec.Meta.Changes()) != 0 {
			t.Errorf("record %d was annotated for having a past timestamp: %+v", i, rec.Meta.Changes())
		}
	}
}

// A record exactly AT the boundary is within tolerance. max_skew is the distance a timestamp may
// lead by, not the distance it must stay under.
func TestTheBoundaryIsInclusive(t *testing.T) {
	now := time.Now()
	r, c := testRunner(spec.ClockPolicy{MaxSkew: time.Minute, Behaviour: spec.ClockClamp})
	b := skewBatch(t, now, time.Minute, time.Minute+time.Millisecond)

	out, _ := r.applyClock(context.Background(), c, b)
	if n := len(out.Records[0].Meta.Changes()); n != 0 {
		t.Errorf("a record exactly at max_skew was clamped; the field is how far it MAY lead")
	}
	if n := len(out.Records[1].Meta.Changes()); n != 1 {
		t.Errorf("a record one millisecond past max_skew was not clamped")
	}
}

// CLAMP ADJUSTS THE RECORD AND SAYS SO ON THE RECORD. A counter alone leaves a consumer six months
// later with a timestamp that was altered and no way to know it was.
func TestClampRewritesTheTimestampAndNotesTheChange(t *testing.T) {
	now := time.Now()
	r, c := testRunner(spec.ClockPolicy{MaxSkew: time.Second, Behaviour: spec.ClockClamp})
	b := skewBatch(t, now, -time.Minute, 48*time.Hour)

	out, err := r.applyClock(context.Background(), c, b)
	if err != nil {
		t.Fatalf("applyClock: %v", err)
	}
	if out.Len() != 2 {
		t.Fatalf("clamping dropped a record: %d left of 2", out.Len())
	}
	sane, skewed := out.Records[0], out.Records[1]
	if len(sane.Meta.Changes()) != 0 {
		t.Error("the in-tolerance record was annotated")
	}
	if skewed.EventTime.After(now.Add(time.Second)) {
		t.Errorf("the skewed record still leads the clock: %v", skewed.EventTime)
	}
	if skewed.EventTime.Before(now) {
		t.Errorf("clamping moved the timestamp into the past: %v", skewed.EventTime)
	}

	ch := skewed.Meta.Changes()
	if len(ch) != 1 {
		t.Fatalf("%d changes noted, want 1", len(ch))
	}
	if ch[0].Path != "event_time" || ch[0].Kind != record.FieldRounded {
		t.Errorf("the change is %+v, want event_time/rounded", ch[0])
	}
	if !strings.Contains(ch[0].Detail, "48h") {
		t.Errorf("the detail does not say how far it led: %q", ch[0].Detail)
	}
}

// PASS ACCEPTS THE TIMESTAMP VERBATIM. Verbatim means the record is not touched at all — no
// rewrite, and no annotation claiming a fidelity loss that did not happen.
func TestPassLeavesTheRecordAlone(t *testing.T) {
	now := time.Now()
	want := now.Add(time.Hour)
	r, c := testRunner(spec.ClockPolicy{MaxSkew: time.Second, Behaviour: spec.ClockPass})
	b := skewBatch(t, now, time.Hour)

	out, _ := r.applyClock(context.Background(), c, b)
	if out.Len() != 1 {
		t.Fatalf("pass dropped the record")
	}
	if !out.Records[0].EventTime.Equal(want) {
		t.Errorf("event time is %v, want the source's %v untouched", out.Records[0].EventTime, want)
	}
	if len(out.Records[0].Meta.Changes()) != 0 {
		t.Error("pass annotated a record it accepted verbatim")
	}
}

// A ZERO max_skew DISABLES THE CHECK, which the field documents and which is the default. A pipeline
// that never configured a clock policy must behave exactly as it did before any of this existed.
func TestAZeroMaxSkewChecksNothing(t *testing.T) {
	now := time.Now()
	want := now.Add(72 * time.Hour)
	r, c := testRunner(spec.ClockPolicy{Behaviour: spec.ClockClamp})
	if c.enabled() {
		t.Fatal("a zero max_skew reported the check as enabled")
	}
	b := skewBatch(t, now, 72*time.Hour)

	out, _ := r.applyClock(context.Background(), c, b)
	if !out.Records[0].EventTime.Equal(want) {
		t.Errorf("a disabled check rewrote a timestamp to %v", out.Records[0].EventTime)
	}
	if len(out.Records[0].Meta.Changes()) != 0 {
		t.Error("a disabled check annotated a record")
	}
}

// A record with NO event time at all is not skewed. A source that does not stamp one is the common
// case, and treating the zero time as a value would make it wildly in the past — harmless under this
// one-sided check, but only by accident, so it is skipped explicitly.
func TestAnUnstampedRecordIsSkipped(t *testing.T) {
	r, c := testRunner(spec.ClockPolicy{MaxSkew: time.Second, Behaviour: spec.ClockReject})
	a := record.NewAllocator("acme", "clk", "in", "lane-1", "lines", 1, 1)
	b := record.NewBatch(a, 1)
	b.Add() // EventTime left zero

	out, err := r.applyClock(context.Background(), c, b)
	if err != nil {
		t.Fatalf("applyClock: %v", err)
	}
	if out.Len() != 1 {
		t.Error("a record with no event time was rejected for clock skew")
	}
}
