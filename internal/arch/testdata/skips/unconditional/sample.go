// Package unconditional is the POSITIVE fixture for TestTheSkipMatcherWorksInBothDirections: a file
// whose test is declared and never runs. It lives under testdata so the toolchain ignores it.
//
// THE PIN USED TO BE A REAL SKIPPED TEST, and that was a pin with an expiry date. The matcher named
// internal/engine/revocation_test.go, so the day that test stopped skipping — which is the outcome
// the whole guard is FOR — the matcher lost its positive case and failed. A guard that breaks when
// the thing it polices is fixed is a guard people delete.
package unconditional

import "testing"

func TestNeverRuns(t *testing.T) {
	t.Skip("fixture: an unconditional skip, which is what the matcher must find")
	t.Error("unreachable")
}
