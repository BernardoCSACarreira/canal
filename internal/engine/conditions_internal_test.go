package engine

import (
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// specApplied is the arithmetic behind CondSpecApplied: two revisions in, one condition out.
//
// It was written before anything could produce divergent revisions and tested here for that reason —
// the comparison had to be right before there was anything to compare. There is now: config.go
// watches store.ConfigStore for the stored revision, config_test.go drives the divergence through a
// real pipeline, and configCondition covers the answers arithmetic cannot give. This stays as the
// table for the arithmetic itself.
func TestSpecAppliedAnswersTheConfigQuestion(t *testing.T) {
	same := specApplied(9, 9)
	if same.Status != telemetry.StatusTrue || same.Reason != telemetry.ReasonApplied {
		t.Errorf("equal revisions gave %s/%s, want true/applied", same.Status, same.Reason)
	}

	// THE QUESTION THE CONDITION EXISTS FOR: a stored revision the process has not applied means the
	// operator's config change silently did not take effect, and a status API that cannot say so
	// leaves them guessing.
	stale := specApplied(10, 9)
	if stale.Status != telemetry.StatusFalse || stale.Reason != telemetry.ReasonPending {
		t.Errorf("a stored revision ahead of the applied one gave %s/%s, want false/pending",
			stale.Status, stale.Reason)
	}
	if stale.Message == same.Message {
		t.Error("both answers carry the same message; the message is what names the two revisions")
	}

	// A revision BEHIND the applied one is also a divergence — a rollback that has not been picked
	// up — and it must not be reported as applied just because the inequality points the other way.
	if got := specApplied(8, 9); got.Status != telemetry.StatusFalse {
		t.Errorf("a stored revision behind the applied one is %s, want false", got.Status)
	}
}

// The progress window is expressed in flush intervals because that is the cadence at which a cursor
// CAN move, with a floor so a sub-second interval does not make the primary health condition flap on
// scheduling noise alone.
func TestProgressWindowScalesWithTheFlushIntervalAndHasAFloor(t *testing.T) {
	r := &runner{}
	r.deps.FlushInterval = 0
	if got := r.progressWindow(); got < 1e9 {
		t.Errorf("an unset flush interval gave a %v window; it must fall back rather than be zero", got)
	}

	r.deps.FlushInterval = 5e6 // 5ms
	if got := r.progressWindow(); got != 1e9 {
		t.Errorf("a 5ms interval gave a %v window, want the 1s floor: three 5ms intervals would make "+
			"the health signal flap on scheduling noise", got)
	}

	r.deps.FlushInterval = 10e9 // 10s
	if got := r.progressWindow(); got != 30e9 {
		t.Errorf("a 10s interval gave a %v window, want 3 intervals: one missed tick is a busy machine "+
			"and three in a row is a pipeline that is not moving", got)
	}
}
