// Package guarded is the NEGATIVE fixture for TestTheSkipMatcherWorksInBothDirections: every skip in
// it is a decision about the environment, so the test still runs in the suite that matters.
//
// Reporting this shape would drown the check in every legitimate -short guard in the module, which
// is the failure mode that makes a guard worthless — one that cries about everything gets muted.
package guarded

import "testing"

func TestRunsUnlessShort(t *testing.T) {
	if testing.Short() {
		t.Skip("fixture: a conditional skip, which the matcher must NOT report")
	}
}

func TestRunsUnlessTheFileIsMissing(t *testing.T) {
	if _, err := readFixture(); err != nil {
		t.Skipf("fixture: another conditional skip, this one on a missing input: %v", err)
	}
}

func readFixture() ([]byte, error) { return nil, nil }
