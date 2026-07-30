package config

// Mapping is a declarative field mapping over the generic record, used by
// [TypeMapping].
//
// It is how a SPECIALISED sink UI — "map these record fields onto these destination
// columns" — is expressed as DATA and therefore needs no core change when a new sink
// wants it. Specialised UI/UX for less-generic connectors is satisfied structurally,
// not by a core switch on connector identity.
type Mapping struct {
	// Target is the destination-side name: a column, a field, a header.
	Target string `json:"target"`
	Source Source `json:"source"`
	// Required means a mapping that resolves to nothing is a per-record mapping fault
	// rather than a null.
	Required bool `json:"required"`
	Default  any  `json:"default,omitempty"`
}

// Source is the CLOSED set of places a mapped value may come from.
//
// Closed deliberately: an embedded expression language is a real dependency with real
// reach, and this design declines it. Ninety percent of sink mappings are one of these
// six; the remainder are a transform.
type Source struct {
	Kind SourceKind `json:"kind"`

	// Path addresses a field of the structured payload, for [SourcePayloadField].
	Path []string `json:"path,omitempty"`

	// Namespace and Key address metadata, for [SourceMetaField].
	Namespace string `json:"namespace,omitempty"`
	Key       string `json:"key,omitempty"`

	// Literal is the constant, for [SourceLiteral].
	Literal any `json:"literal,omitempty"`
}

// SourceKind names which member of the closed set a [Source] is. It is a string for the
// same reason [FieldType] is.
type SourceKind string

const (
	SourcePayloadField SourceKind = "payload_field"
	SourceWholePayload SourceKind = "payload"
	SourceMetaField    SourceKind = "meta_field"
	SourceOriginKey    SourceKind = "origin_key"
	SourceEventTime    SourceKind = "event_time"
	SourceLiteral      SourceKind = "literal"
)
