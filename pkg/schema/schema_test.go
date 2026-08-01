package schema

import (
	"encoding"
	"testing"
)

// pkg/schema is small and two things in it carry real weight. ChangeKind.Destructive is what keeps
// the default drift policy never-destructive WITHOUT any per-sink knowledge: a destructive kind is
// rewritten as an additive pair or refused, never applied. And every enum here crosses a wire, so a
// kind with no name renders as "unknown" and a policy decision is taken on a token that names
// nothing.

var allChangeKinds = []ChangeKind{
	ChangeKindUnknown, CreateStream, AddField, DropField, RenameField,
	AlterFieldType, AlterNullability, AlterKeys, TruncateStream, DropStream, RenameStream,
}

// The names table is a sparse array literal, so a kind declared past its end silently renders as
// "unknown" — and the drift policy would then log, alert and refuse against a token that names
// nothing in particular.
func TestEveryChangeKindHasItsOwnName(t *testing.T) {
	if len(allChangeKinds) != len(changeKindNames) {
		t.Fatalf("%d kinds declared but %d names", len(allChangeKinds), len(changeKindNames))
	}
	seen := map[string]ChangeKind{}
	for _, k := range allChangeKinds {
		name := k.String()
		if k != ChangeKindUnknown && name == "unknown" {
			t.Errorf("kind %d renders as unknown, so it has no entry of its own", k)
		}
		if prev, dup := seen[name]; dup {
			t.Errorf("kinds %d and %d both render as %q", prev, k, name)
		}
		seen[name] = k
	}
}

// The destructive set is the whole basis of the lenient policy. It must be exactly the kinds that
// can LOSE DATA at the destination — no more, because over-marking refuses safe changes, and no
// less, because under-marking applies a data-losing one.
func TestDestructiveIsExactlyTheDataLosingKinds(t *testing.T) {
	want := map[ChangeKind]bool{
		DropField: true, AlterFieldType: true, TruncateStream: true, DropStream: true,
	}
	for _, k := range allChangeKinds {
		if got := k.Destructive(); got != want[k] {
			t.Errorf("%s.Destructive() is %v, want %v", k, got, want[k])
		}
	}
}

// A RENAME IS NOT DESTRUCTIVE, and this is the one entry in that table with a story. Encoded as
// DropStream plus CreateStream it is a destructive pair that the default policy refuses and that
// an evolving policy executes by dropping the destination table and its history: the upstream
// operation lost nothing and the encoding lost everything.
func TestRenamingIsNotDestructive(t *testing.T) {
	if RenameStream.Destructive() {
		t.Error("renaming a stream is marked destructive; a table rename would drop the destination")
	}
	if RenameField.Destructive() {
		t.Error("renaming a field is marked destructive")
	}
	// The pair it replaces is destructive, which is exactly why the dedicated kind has to exist:
	// DropStream is the half that loses the data, and CreateStream cannot undo it.
	if !DropStream.Destructive() {
		t.Error("dropping a stream is not marked destructive")
	}
	if CreateStream.Destructive() {
		t.Error("creating a stream is marked destructive; it is purely additive")
	}
}

// Every enum in this package crosses a wire, so a round trip that loses a value is a checkpoint or
// a descriptor that decodes into something else.
func TestEnumsRoundTripThroughText(t *testing.T) {
	for _, k := range allChangeKinds {
		b, err := k.MarshalText()
		if err != nil {
			t.Fatalf("%v: MarshalText: %v", k, err)
		}
		var back ChangeKind
		if err := back.UnmarshalText(b); err != nil {
			t.Fatalf("%q: UnmarshalText: %v", b, err)
		}
		if back != k {
			t.Errorf("%s round-tripped to %s", k, back)
		}
	}

	// The interfaces are satisfied, which is what makes encoding/json use them at all rather than
	// silently writing an ordinal.
	var _ encoding.TextMarshaler = ChangeKindUnknown
	var _ encoding.TextUnmarshaler = new(ChangeKind)
	var _ encoding.TextMarshaler = Type(0)
	var _ encoding.TextUnmarshaler = new(Type)
	var _ encoding.TextMarshaler = TimeUnit(0)
	var _ encoding.TextUnmarshaler = new(TimeUnit)
}

// An unrecognised token must be refused rather than silently decoded as the zero value, which would
// turn an unknown change into "unknown" and let it through the policy as if it were declared.
func TestUnmarshalRefusesAnUnknownToken(t *testing.T) {
	var k ChangeKind
	if err := k.UnmarshalText([]byte("not_a_change_kind")); err == nil {
		t.Error("an unrecognised token decoded without error")
	}
}
