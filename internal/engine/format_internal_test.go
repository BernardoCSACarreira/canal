package engine

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// THE UPGRADE TEST THE FORMAT CONTRACT DEMANDS, and the first one in the module.
//
// CheckpointFormat's own doc says it: "Every change to this format ships with an upgrade test that
// writes with build N, reads with N+1, writes with N+1, reads with N, and asserts a lossless round trip
// of everything both understand. A format change with no upgrade test does not merge." Nothing did —
// CheckpointFormat appeared in exactly one test in the module and not for this.
//
// An older build is simulated by a struct that OMITS the new field, which is what an older binary's
// decoder is. That is the whole mechanism, and it is faithful because encoding/json is what both builds
// use: an unknown key is ignored on the way in and an absent one decodes to the zero value.
//
// WHAT "LOSSLESS" MEANS HERE, precisely, because it does not mean nothing is lost. A round trip THROUGH
// an older build drops the newer field — the old build cannot re-emit a key it has no field for — and
// rule 2 is what makes that safe rather than a defect: absent or zero means legacy, and legacy means
// behave exactly as the previous version did. So these assert that everything BOTH builds understand
// survives, and that the field only one of them understands degrades to zero rather than to nonsense.

// laneRowV0 is the durable lane row as it was before CursorEpoch, byte-for-byte in its JSON tags.
type laneRowV0 struct {
	Spec   connector.LaneSpec `json:"spec"`
	Cursor record.Position    `json:"cursor"`

	Finished   bool      `json:"finished,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

// laneStateV0 is the checkpoint envelope's lane entry as it was before CursorEpoch.
type laneStateV0 struct {
	Spec   record.Blob     `json:"spec"`
	Cursor record.Position `json:"cursor"`

	Group record.LaneGroup   `json:"group,omitempty"`
	After []record.LaneGroup `json:"after,omitempty"`

	Kind     connector.LaneKind `json:"kind"`
	Ordering connector.Ordering `json:"ordering"`
	Bounded  bool               `json:"bounded"`
	Finished bool               `json:"finished"`

	FinishedAt time.Time `json:"finished_at,omitempty"`

	Label  string `json:"label,omitempty"`
	Weight uint64 `json:"weight,omitempty"`

	Version uint64 `json:"version"`
}

func cursorAt(label string) record.Position {
	return record.Position{
		Token: record.Blob{Version: 1, Bytes: []byte{9, 9}},
		Order: []byte{9, 9}, Safe: true, Label: label,
	}
}

// THE WIRE NAME IS PART OF THE FORMAT, so it is pinned literally.
//
// The upgrade tests below CANNOT catch a rename, and finding that out is what this exists for. They
// compare build N against N+1, and N's struct has no such field at all — so renaming the tag leaves both
// directions passing: the absent key still decodes to zero, and the unknown key is still ignored. The
// break is between N+1 and N+2, where one writes cursor_epoch and the other reads cursorEpoch and the
// lease that produced every cursor is silently lost.
//
// A literal assertion is the only thing that holds a persisted name still. It is deliberately ugly to
// change, because changing it IS the incompatible edit.
func TestTheEpochsWireNameIsPinned(t *testing.T) {
	row := laneRecord{Cursor: cursorAt("byte 1"), CursorEpoch: 7}
	body, err := json.Marshal(&row)
	if err != nil {
		t.Fatalf("encoding the row: %v", err)
	}
	if !strings.Contains(string(body), `"cursor_epoch":7`) {
		t.Errorf("the lane row does not encode cursor_epoch:\n  %s\n"+
			"  Renaming a persisted key is an incompatible change that no upgrade test against the "+
			"PREVIOUS version can see, because that version never knew the key", body)
	}

	entry := LaneState{Cursor: cursorAt("byte 1"), CursorEpoch: 7}
	body, err = json.Marshal(&entry)
	if err != nil {
		t.Fatalf("encoding the entry: %v", err)
	}
	if !strings.Contains(string(body), `"cursor_epoch":7`) {
		t.Errorf("the checkpoint's lane entry does not encode cursor_epoch:\n  %s", body)
	}

	// omitempty, so a lane with no recorded lease costs no bytes and reads as absent rather than as a
	// zero somebody wrote on purpose. Rule 1 requires it of every added field.
	body, err = json.Marshal(&laneRecord{Cursor: cursorAt("byte 1")})
	if err != nil {
		t.Fatalf("encoding a row with no epoch: %v", err)
	}
	if strings.Contains(string(body), "cursor_epoch") {
		t.Errorf("a row with no recorded lease still emits the key, so absent and zero stop being "+
			"distinguishable:\n  %s", body)
	}
}

// A LANE ROW WRITTEN BY EITHER BUILD IS READ BY THE OTHER.
func TestTheLaneRowUpgradesAndDowngrades(t *testing.T) {
	finishedAt := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)

	// --- build N writes, build N+1 reads ---
	old := laneRowV0{
		Spec:       connector.LaneSpec{Name: "chunk-7", Stream: "orders"},
		Cursor:     cursorAt("byte 4096"),
		Finished:   true,
		FinishedAt: finishedAt,
	}
	body, err := json.Marshal(&old)
	if err != nil {
		t.Fatalf("encoding the old row: %v", err)
	}

	var upgraded laneRecord
	if err := json.Unmarshal(body, &upgraded); err != nil {
		t.Fatalf("the current build cannot read a row written before CursorEpoch existed: %v", err)
	}
	if upgraded.Spec.Name != "chunk-7" || upgraded.Cursor.Label != "byte 4096" {
		t.Errorf("the shared fields did not survive the upgrade: %+v", upgraded)
	}
	if !upgraded.Finished || !upgraded.FinishedAt.Equal(finishedAt) {
		t.Errorf("the finish state did not survive the upgrade: finished=%v at=%v",
			upgraded.Finished, upgraded.FinishedAt)
	}
	if upgraded.CursorEpoch != 0 {
		t.Errorf("CursorEpoch decoded as %d from a row that never carried it; rule 2 says absent means "+
			"legacy, and every other zero epoch in the module means no lease is claimed",
			upgraded.CursorEpoch)
	}

	// --- build N+1 writes, build N reads ---
	upgraded.CursorEpoch = 7
	upgraded.Cursor = cursorAt("byte 8192")
	body, err = json.Marshal(&upgraded)
	if err != nil {
		t.Fatalf("encoding the current row: %v", err)
	}

	var downgraded laneRowV0
	if err := json.Unmarshal(body, &downgraded); err != nil {
		t.Fatalf("an older build cannot read a row this build wrote: %v\n"+
			"  Rule 3: a newer record stays structurally readable, because rejecting it is what makes a "+
			"binary downgrade unsurvivable", err)
	}
	if downgraded.Spec.Name != "chunk-7" || downgraded.Cursor.Label != "byte 8192" {
		t.Errorf("the shared fields did not survive the downgrade: %+v", downgraded)
	}
	if !downgraded.Finished {
		t.Error("the finish state did not survive the downgrade")
	}
}

// THE SAME, FOR THE CHECKPOINT ENVELOPE'S LANE ENTRY.
func TestTheCheckpointsLaneEntryUpgradesAndDowngrades(t *testing.T) {
	old := laneStateV0{
		Spec:     record.Blob{Version: 2, Bytes: []byte("spec")},
		Cursor:   cursorAt("lsn 0/1A2B"),
		Kind:     connector.LaneKindStream,
		Ordering: connector.OrderingPrefix,
		Bounded:  false,
		Version:  11,
		Label:    "orders tail",
		Weight:   3,
	}
	body, err := json.Marshal(&old)
	if err != nil {
		t.Fatalf("encoding the old entry: %v", err)
	}

	var upgraded LaneState
	if err := json.Unmarshal(body, &upgraded); err != nil {
		t.Fatalf("the current build cannot read an entry written before CursorEpoch existed: %v", err)
	}
	if upgraded.Cursor.Label != "lsn 0/1A2B" || upgraded.Version != 11 || upgraded.Weight != 3 {
		t.Errorf("the shared fields did not survive the upgrade: %+v", upgraded)
	}
	if upgraded.CursorEpoch != 0 {
		t.Errorf("CursorEpoch decoded as %d from an entry that never carried it", upgraded.CursorEpoch)
	}

	upgraded.CursorEpoch = 4
	body, err = json.Marshal(&upgraded)
	if err != nil {
		t.Fatalf("encoding the current entry: %v", err)
	}
	var downgraded laneStateV0
	if err := json.Unmarshal(body, &downgraded); err != nil {
		t.Fatalf("an older build cannot read an entry this build wrote: %v", err)
	}
	if downgraded.Cursor.Label != "lsn 0/1A2B" || downgraded.Version != 11 || downgraded.Label != "orders tail" {
		t.Errorf("the shared fields did not survive the downgrade: %+v", downgraded)
	}

	// AND THE DOCUMENTED LOSS, asserted rather than assumed. A round trip through the older build drops
	// the field it has no name for, which rule 1's additive-only encoding makes safe and rule 2 makes
	// meaningful: what comes back reads as "no lease recorded", not as a wrong lease.
	body, err = json.Marshal(&downgraded)
	if err != nil {
		t.Fatalf("re-encoding through the old build: %v", err)
	}
	var again LaneState
	if err := json.Unmarshal(body, &again); err != nil {
		t.Fatalf("reading back what the old build re-wrote: %v", err)
	}
	if again.CursorEpoch != 0 {
		t.Errorf("CursorEpoch is %d after a round trip through a build that does not know it, want 0; "+
			"anything else would be a value invented by the encoding rather than recorded by a writer",
			again.CursorEpoch)
	}
	if again.Cursor.Label != "lsn 0/1A2B" {
		t.Errorf("the cursor did not survive the full round trip: %+v", again.Cursor)
	}
}
