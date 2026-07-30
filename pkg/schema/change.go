package schema

import "time"

// Change is an ordered, in-band schema change event.
//
// Changes are tracked into the checkpoint whether or not they are emitted
// downstream, and the engine quiesces the affected stream before applying one,
// so records written under the old schema cannot race the ALTER.
type Change struct {
	Kind   ChangeKind `json:"kind"`
	Stream string     `json:"stream"`

	// Field is the path of the field the change concerns. Nil for stream-level
	// kinds.
	Field []string `json:"field,omitempty"`

	From *Field `json:"from,omitempty"`
	To   *Field `json:"to,omitempty"`

	// Rename carries the new stream name for a [RenameStream]; Stream carries the old one.
	Rename string `json:"rename,omitempty"`

	// Result is the WHOLE schema as it stands after this change is applied. Nil only for a
	// [DropStream], which leaves no schema behind.
	//
	// A sink applying a change needs the resulting shape, and From/To described one field.
	// SchemaApplier.ApplySchemaChange for a CreateStream was therefore unappliable — the
	// change named a stream and gave no columns to create it with — so a drifting pipeline's
	// new stream could only be refused wholesale. Carrying the result also means a sink that
	// prefers to reconcile rather than to alter (CREATE OR REPLACE, a MERGE into a new
	// table) can do so from one value.
	Result *Schema `json:"result,omitempty"`

	// Epoch is this change's position in the stream's schema history. It is
	// committed atomically with the lane cursors that follow it, which is why
	// "encountered a change event whose schema isn't known" is structurally
	// absent here.
	Epoch uint64    `json:"epoch"`
	At    time.Time `json:"at"`
}

// ChangeKind is the closed change vocabulary. It is both the sink's declared
// capability set and the drift policy's include/exclude vocabulary — one enum,
// not two, and no cross-map (design rule R9).
type ChangeKind uint8

const (
	// ChangeKindUnknown is the zero value and is never a legal declared change.
	ChangeKindUnknown ChangeKind = iota
	CreateStream
	AddField
	DropField
	RenameField
	AlterFieldType
	AlterNullability
	AlterKeys
	TruncateStream
	DropStream

	// RenameStream is a stream that changed name and kept its data. Change.Stream is the
	// old name, Change.Rename the new one.
	//
	// Without it a table rename encoded as DropStream plus CreateStream — a DESTRUCTIVE
	// pair that the default DriftLenient policy refuses and that DriftEvolve executes by
	// dropping the destination table and its history. The upstream operation lost nothing;
	// the encoding lost everything. It is NOT destructive: a sink that cannot rename
	// atomically may implement it as create-then-copy-then-drop, which is its choice to
	// make and not the vocabulary's.
	RenameStream
)

var changeKindNames = [...]string{
	ChangeKindUnknown: "unknown",
	CreateStream:      "create_stream",
	AddField:          "add_field",
	DropField:         "drop_field",
	RenameField:       "rename_field",
	AlterFieldType:    "alter_field_type",
	AlterNullability:  "alter_nullability",
	AlterKeys:         "alter_keys",
	TruncateStream:    "truncate_stream",
	DropStream:        "drop_stream",
	RenameStream:      "rename_stream",
}

// String returns the stable snake_case token for k.
func (k ChangeKind) String() string {
	if int(k) < len(changeKindNames) {
		return changeKindNames[k]
	}
	return "unknown"
}

// Destructive reports whether applying k can lose data at the destination.
//
// The drift policy uses it to keep DriftLenient never-destructive without any
// per-sink knowledge: a destructive kind is rewritten as an additive pair or
// refused, never applied.
func (k ChangeKind) Destructive() bool {
	switch k {
	case DropField, AlterFieldType, TruncateStream, DropStream:
		return true
	default:
		return false
	}
}
