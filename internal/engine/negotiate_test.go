// Regression tests for the audit's fatal R4 finding in the waiver matcher.
package engine

import (
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

func signed(node string, missing ...string) telemetry.Downgrade {
	return telemetry.Downgrade{
		Requested: "exactly_once", Effective: "at_least_once",
		Missing:        missing,
		Node:           record.NodeID(node),
		AcknowledgedBy: "ops@example.com",
		Reason:         "the queue sink cannot upsert and the backfill must run tonight",
	}
}

// TestOneWaiverDoesNotSilenceEveryNode is the fatal finding, stated as a test.
//
// applyWaivers matched only Node/AcknowledgedBy/Reason, and treated an empty Node as a WILDCARD. So
// a single waiver signed for one sink's missing capability downgraded EVERY capability and
// guarantee error in the pipeline to a warning — including ADR 0006's ack-before-persist guard,
// which is the "this configuration loses records" refusal. Signing off a known limitation silently
// signed off data loss.
func TestOneWaiverDoesNotSilenceEveryNode(t *testing.T) {
	s := &spec.Spec{
		Downgrades: []telemetry.Downgrade{signed("", "connector.Committer")},
	}
	d := config.Diagnostics{
		{Node: "queue", Severity: config.SeverityError, Code: config.CodeCapability,
			Message: "sink cannot upsert", Iface: "connector.Committer"},
		{Node: "warehouse", Severity: config.SeverityError, Code: config.CodeCapability,
			Message: "source prunes its upstream on commit, but the state store is not durable"},
	}

	out := applyWaivers(s, d)

	// The pipeline-scoped waiver must not reach either node-anchored refusal.
	for _, x := range out[:2] {
		if x.Severity != config.SeverityError {
			t.Errorf("node %q was downgraded to %s by a waiver anchored to no node; that is the wildcard bug",
				x.Node, x.Severity)
		}
	}
}

// TestWaiverIsScopedToItsNode: a waiver naming one node must not travel to another.
func TestWaiverIsScopedToItsNode(t *testing.T) {
	s := &spec.Spec{Downgrades: []telemetry.Downgrade{signed("queue", "connector.Committer")}}
	d := config.Diagnostics{
		{Node: "queue", Severity: config.SeverityError, Code: config.CodeCapability,
			Message: "queue cannot commit", Iface: "connector.Committer"},
		{Node: "warehouse", Severity: config.SeverityError, Code: config.CodeCapability,
			Message: "warehouse cannot commit either", Iface: "connector.Committer"},
	}

	out := applyWaivers(s, d)

	if out[0].Severity != config.SeverityWarning {
		t.Errorf("the waived node %q stayed %s; a correctly-scoped waiver must still work", out[0].Node, out[0].Severity)
	}
	if out[1].Severity != config.SeverityError {
		t.Errorf("node %q was downgraded by a waiver anchored to %q", out[1].Node, "queue")
	}
}

// TestWaiverMustNameTheCapability: Missing is what makes a waiver checkable at all. ADR 0024 calls
// for capability NAMES; a waiver that lists none, or lists the wrong one, covers nothing.
func TestWaiverMustNameTheCapability(t *testing.T) {
	cases := []struct {
		name    string
		missing []string
		iface   string
		waived  bool
	}{
		{"names the interface", []string{"connector.Committer"}, "connector.Committer", true},
		{"names a different one", []string{"connector.Chunkable"}, "connector.Committer", false},
		{"names none at all", nil, "connector.Committer", false},
		{"refusal has no machine-readable identity", []string{"connector.Committer"}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &spec.Spec{Downgrades: []telemetry.Downgrade{signed("queue", tc.missing...)}}
			d := config.Diagnostics{{
				Node: "queue", Severity: config.SeverityError, Code: config.CodeCapability,
				Message: "refused", Iface: tc.iface,
			}}
			out := applyWaivers(s, d)
			got := out[0].Severity == config.SeverityWarning
			if got != tc.waived {
				t.Errorf("waived=%v, want %v", got, tc.waived)
			}
		})
	}
}

// TestUnsignedWaiverIsItselfAnError preserves the behaviour that was already right: the core can
// never mint a waiver, so an anonymous one is refused rather than ignored.
func TestUnsignedWaiverIsItselfAnError(t *testing.T) {
	s := &spec.Spec{Downgrades: []telemetry.Downgrade{{
		Requested: "exactly_once", Effective: "at_least_once",
		Missing: []string{"connector.Committer"}, Node: "queue",
	}}}
	out := applyWaivers(s, config.Diagnostics{})
	if !out.HasErrors() {
		t.Fatal("an unsigned waiver was accepted silently")
	}
}

// TestWaiverWithNoMissingIsRefused: the new rule needs its own diagnostic, or an operator who omits
// Missing just sees their waiver quietly do nothing.
func TestWaiverWithNoMissingIsRefused(t *testing.T) {
	s := &spec.Spec{Downgrades: []telemetry.Downgrade{{
		Requested: "exactly_once", Effective: "at_least_once", Node: "queue",
		AcknowledgedBy: "ops@example.com", Reason: "known limitation",
	}}}
	out := applyWaivers(s, config.Diagnostics{})
	if !out.HasErrors() {
		t.Fatal("a waiver naming no capability was accepted; it would be unmatchable and silent")
	}
}
