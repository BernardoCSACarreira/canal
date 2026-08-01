package engine

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// configCondition is the four-way part of CondSpecApplied: the answers arithmetic cannot give. The
// arithmetic itself is specApplied and is tested next door.
//
// EACH ARM IS A DIFFERENT OPERATOR RESPONSE, which is the reason they are four answers and not one
// "we could not confirm it". Nothing to compare means the deployment has no control plane and the
// operator should stop looking; not read yet means wait; unreachable means look at the control
// plane; deleted means look at this worker.
func TestConfigConditionSeparatesTheFourWaysThereIsNoComparison(t *testing.T) {
	now := time.Now()

	nostore := configCondition(false, nil, 5)
	if nostore.Status != telemetry.StatusTrue || nostore.Reason != telemetry.ReasonApplied {
		t.Errorf("with no config store: %s/%s, want true/applied", nostore.Status, nostore.Reason)
	}

	// THE ONE THAT MATTERS MOST HERE. A pipeline that has a store and has not read it must not
	// report applied — that is exactly the claim the whole condition was making before it had a
	// reader, and inferring "no store" from a nil view would reintroduce it.
	unread := configCondition(true, nil, 5)
	if unread.Status != telemetry.StatusUnknown || unread.Reason != telemetry.ReasonStarting {
		t.Errorf("with a store that has not been read: %s/%s, want unknown/starting.\n"+
			"  Reporting applied here would mean claiming a comparison was made against a store "+
			"nobody has asked yet", unread.Status, unread.Reason)
	}
	if unread.Message == nostore.Message {
		t.Error("'no store to read' and 'store not read yet' carry the same message; they are " +
			"opposite states and a reader cannot act on either without telling them apart")
	}

	down := configCondition(true, &configView{
		revision: 4, known: true, err: errors.New("boom"),
		okAt: now.Add(-90 * time.Minute), at: now,
	}, 5)
	if down.Status != telemetry.StatusUnknown || down.Reason != telemetry.ReasonConfigStoreUnreachable {
		t.Errorf("with an unreachable store: %s/%s, want unknown/config_store_unreachable",
			down.Status, down.Reason)
	}
	// TIMED FROM THE LAST ANSWER, NOT THE LAST ATTEMPT. The read that failed happened now, so a
	// message timed from it reports every outage as seconds old however long it has been going on —
	// and how long it has been going on is the one thing this message is read for.
	if !strings.Contains(down.Message, "1h30m0s") {
		t.Errorf("the message %q does not say the store has been unreachable for an hour and a "+
			"half; timing from the failed read would make an old outage look brand new", down.Message)
	}
	// A store that has never once answered says so rather than claiming a duration.
	never := configCondition(true, &configView{err: errors.New("boom"), at: now}, 5)
	if !strings.Contains(never.Message, "never") {
		t.Errorf("the message %q invents an age for a store that has never answered", never.Message)
	}

	gone := configCondition(true, &configView{deleted: true, at: now}, 5)
	if gone.Status != telemetry.StatusFalse || gone.Reason != telemetry.ReasonSpecDeleted {
		t.Errorf("with a withdrawn spec: %s/%s, want false/spec_deleted", gone.Status, gone.Reason)
	}

	// And with an actual observation it delegates to the arithmetic rather than reimplementing it.
	if got := configCondition(true, &configView{revision: 5, known: true, at: now}, 5); got != specApplied(5, 5) {
		t.Errorf("an observed matching revision gave %v, not specApplied's own answer", got)
	}
	if got := configCondition(true, &configView{revision: 6, known: true, at: now}, 5); got != specApplied(6, 5) {
		t.Errorf("an observed divergent revision gave %v, not specApplied's own answer", got)
	}
}

// Generation has to hold a number even when nothing was ever observed, and the number it falls back
// to is the running revision — not zero, which would render as "generation 0" and read as a fact.
func TestGenerationFallsBackToTheRunningRevision(t *testing.T) {
	if got := (*configView)(nil).generation(7); got != 7 {
		t.Errorf("a nil view gave generation %d, want the running revision 7", got)
	}
	// A view that failed before it ever succeeded carries no revision, and must fall back too.
	if got := (&configView{err: errors.New("boom")}).generation(7); got != 7 {
		t.Errorf("a view that never got an answer gave generation %d, want 7", got)
	}
	// A view that failed AFTER a good read keeps the last number it knew: "revision 4, four minutes
	// ago" is more useful than nothing, and the condition beside it says the number is stale.
	stale := &configView{revision: 4, known: true, err: errors.New("boom")}
	if got := stale.generation(7); got != 4 {
		t.Errorf("a view holding a stale revision gave generation %d, want the last known 4", got)
	}
	if got := (&configView{revision: 9, known: true}).generation(7); got != 9 {
		t.Errorf("an observed revision gave generation %d, want 9", got)
	}
	// A WITHDRAWN SPEC HAS NO STORED REVISION AT ALL, so the last one it had is not the answer.
	if got := (&configView{revision: 4, deleted: true}).generation(7); got != 7 {
		t.Errorf("a deleted spec reported generation %d; nothing is stored, so the last revision "+
			"that was is not a stored revision", got)
	}

	// ZERO IS A LEGAL STORED REVISION, which is why the view carries a flag and not just a number.
	// cmd/canal's file projection returns whatever the operator wrote, and an operator who never
	// touched the field wrote zero. Inferring "never read" from a zero would report the running
	// revision as the stored one for exactly the pipelines whose config is being ignored.
	if got := (&configView{revision: 0, known: true}).generation(7); got != 0 {
		t.Errorf("an observed revision of 0 reported generation %d, want 0", got)
	}
	if c := configCondition(true, &configView{revision: 0, known: true}, 7); c.Status != telemetry.StatusFalse {
		t.Errorf("a stored revision of 0 against a running 7 is %s, want false", c.Status)
	}
}
