package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// CLOCK SKEW, WHICH WAS DECLARED POLICY AND NO CHECK AT ALL.
//
// spec.ClockPolicy names a max_skew and one of three behaviours, and the engine read neither, so a
// source could stamp a record a year in the future and every downstream window, retention rule and
// event-time lag computed from it was wrong with nothing to say so. It is the same defect as
// when_full — a policy an operator configures and nothing consults — and it was the last one left
// on spec.Spec.
//
// THE CHECK IS ONE-SIDED, exactly as the field says: max_skew is how far a source timestamp may LEAD
// the local clock. A timestamp in the past is not skew, it is history, and a batch source replaying
// last year's data is the normal case rather than a fault. Clamping that would destroy the very
// field it claims to protect.
//
// It runs at the SOURCE EDGE, before admission, because that is where a timestamp enters canal and
// the only place the record is still the source's alone. Once a record is admitted its event time
// has already been used: the lane's event-time lag reads it, and a downstream transform may have
// keyed a window on it.

// clockCheck is the resolved policy plus the instrument, computed once per source.
type clockCheck struct {
	max      time.Duration
	behave   spec.ClockBehaviour
	node     record.NodeID
	pipeline string
}

// clockFor resolves a source's skew policy. A zero max_skew disables the check entirely, which the
// field documents and which is the default.
func (r *runner) clockFor(id record.NodeID) clockCheck {
	return clockCheck{
		max:      r.p.spec.Clock.MaxSkew,
		behave:   r.p.spec.Clock.Behaviour,
		node:     id,
		pipeline: r.p.obs.pipeline,
	}
}

func (c clockCheck) enabled() bool { return c.max > 0 }

// applyClock enforces the policy over one batch and returns it, possibly shorter.
//
// REJECTED RECORDS LEAVE THE BATCH BEFORE ADMISSION. They are dead-lettered on the failed edge and
// then dropped, so they never take a settlement reference and the batch's position still advances
// over them — which is right, because a record refused at the edge is a record canal has decided
// about, not one it is still holding.
func (r *runner) applyClock(ctx context.Context, c clockCheck, b *record.Batch) (*record.Batch, error) {
	if !c.enabled() || b.Len() == 0 {
		return b, nil
	}
	now := time.Now()
	kept := b.Records[:0]
	var rejected int

	for _, rec := range b.Records {
		et := rec.EventTime
		if et.IsZero() || !et.After(now.Add(c.max)) {
			kept = append(kept, rec)
			continue
		}
		skew := et.Sub(now)

		switch c.behave {
		case spec.ClockReject:
			// The record is routed on the failed edge and then dropped. A dead letter that could not
			// be written stops the pipeline rather than being swallowed, exactly as it does on the
			// write path: discarding a record twice over is worse than stopping.
			err := fault.New(fault.ClockSkew, fault.OpRead, fmt.Errorf(
				"event time is %s ahead of the local clock, past a max_skew of %s",
				skew.Round(time.Millisecond), c.max))
			if derr := r.deadLetter(ctx, rec, err); derr != nil {
				return b, derr
			}
			rejected++
			r.p.obs.clockSkew.Add(1, c.pipeline, string(c.node), telemetry.ClockRejected)

		case spec.ClockPass:
			// Accepted verbatim, and COUNTED — which is the whole of what this arm owes an operator.
			// A pass policy with no counter is a decision to accept implausible timestamps and no way
			// to find out how often it happened.
			kept = append(kept, rec)
			r.p.obs.clockSkew.Add(1, c.pipeline, string(c.node), telemetry.ClockPassed)

		default: // spec.ClockClamp
			// THE ADJUSTMENT IS RECORDED ON THE RECORD, not only in a counter. Meta.NoteChange is the
			// module's existing mechanism for a fidelity loss travelling WITH the data, so a consumer
			// six months later can see that this timestamp was altered and by how much rather than
			// discovering it in a reconciliation.
			rec.EventTime = now
			rec.Meta.NoteChange(record.FieldChange{
				Path: "event_time", Kind: record.FieldRounded,
				Detail: fmt.Sprintf("clamped to now; source value led the local clock by %s",
					skew.Round(time.Millisecond)),
			})
			kept = append(kept, rec)
			r.p.obs.clockSkew.Add(1, c.pipeline, string(c.node), telemetry.ClockClamped)
		}
	}

	if rejected > 0 {
		r.deps.Log.Warn("records were routed to the failed edge for clock skew",
			"node", c.node, "lane", b.Lane, "records", rejected, "max_skew", c.max)
		b.Records = kept
	}
	return b, nil
}
