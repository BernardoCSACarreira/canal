package schema

// Converter is the per-sink conversion function, memoised by the engine.
//
// N formats times M sinks becomes N+M and is cached. A conversion that cannot be
// performed faithfully reports it through [FieldNote] rather than silently
// mangling the value: a lossy conversion is a note, a counted metric and a
// visible event, or it is a refusal (invariant 6).
type Converter interface {
	// Convert maps v, described by f, onto the destination's representation. The
	// returned notes describe every field-level fidelity loss and are attached
	// to the record's metadata by the caller.
	Convert(f Field, v any) (any, []FieldNote, error)
}

// FieldNote is one field-level fidelity loss observed during conversion.
//
// It is deliberately a schema-package type rather than a record-package one: a
// [Converter] is reachable from a codec that holds no record, and the engine is
// what turns a note into a record.FieldChange.
type FieldNote struct {
	// Path is the field path the note concerns.
	Path []string `json:"path"`
	// Kind names what happened. It uses the same closed vocabulary as
	// record.FieldChangeKind, expressed here as its stable token so that schema
	// need not import record.
	Kind string `json:"kind"`
	// Detail is a human-readable explanation. It is developer-facing.
	Detail string `json:"detail,omitempty"`
}
