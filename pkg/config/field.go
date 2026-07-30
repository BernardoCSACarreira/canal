package config

// FieldType is the declared shape of one config field.
//
// It is an open-ended STRING for the same reason registry.Kind is: a closed iota
// used as a persisted discriminator and as a frontend switch means adding a field
// type is a contract change plus a coordinated frontend edit, which is design rule
// R1 violated one level up. The validator checks membership, so an unknown type is a
// diagnostic rather than a compile error.
type FieldType string

const (
	TypeString   FieldType = "string"
	TypeInt      FieldType = "int"
	TypeFloat    FieldType = "float"
	TypeBool     FieldType = "bool"
	TypeDuration FieldType = "duration"
	// TypeSize is a byte size written the way operators write it: "64MiB".
	TypeSize   FieldType = "size"
	TypeEnum   FieldType = "enum"
	TypeObject FieldType = "object"
	TypeArray  FieldType = "array"
	TypeMap    FieldType = "map"
	// TypeUnion is a discriminated union: see [Variant].
	TypeUnion FieldType = "union"
	// TypeMapping is a declarative sink field mapping: see [Mapping].
	TypeMapping FieldType = "mapping"
)

// Field is one declared field.
//
// It is DATA: it serialises to JSON, so the frontend, the docs generator, the linter
// and the validator consume the identical artefact and cannot drift.
type Field struct {
	// Name is the wire name, snake_case.
	Name  string `json:"name"`
	Title string `json:"title,omitempty"`

	// Description is reference prose.
	Description string `json:"description"`

	// Short is inline help for a form UI, distinct from Description.
	Short string `json:"short,omitempty"`

	Type FieldType `json:"type"`

	Default  any  `json:"default,omitempty"`
	Optional bool `json:"optional,omitempty"`
	Advanced bool `json:"advanced,omitempty"`

	// Secret means the core redacts this value EVERYWHERE: logs, metrics, the read
	// model, error messages, config round-trips, the API. Zero connector involvement.
	//
	// The absence of this one boolean turned into a security bug class in a surveyed
	// system: redaction became a per-call-site discipline, a second redaction pass was
	// added at the API boundary, and then one endpoint was missed and returned
	// connector settings unredacted. Discipline failed; a flag does not.
	Secret bool `json:"secret,omitempty"`

	// Deprecated names the replacement, or the literal string "no replacement".
	Deprecated string `json:"deprecated,omitempty"`

	Examples []any       `json:"examples,omitempty"`
	Enum     []EnumValue `json:"enum,omitempty"`

	// Fields describes a [TypeObject]'s members.
	Fields []Field `json:"fields,omitempty"`
	// Item describes a [TypeArray]'s element or a [TypeMap]'s value.
	Item *Field `json:"item,omitempty"`
	// Variants describes a [TypeUnion]'s arms.
	Variants []Variant `json:"variants,omitempty"`

	Min *float64 `json:"min,omitempty"`
	Max *float64 `json:"max,omitempty"`

	Pattern string `json:"pattern,omitempty"`
	// PatternHint is the human explanation of Pattern. A regex is not a message.
	PatternHint string `json:"pattern_hint,omitempty"`

	// ShowIf hides the field unless the predicate holds.
	ShowIf *Predicate `json:"show_if,omitempty"`
	// RequiredIf makes the field conditionally required.
	//
	// ShowIf and RequiredIf are DECLARATIVE so the browser evaluates them without a
	// round trip — which a server-side recommender callback cannot do, and which a
	// specialised sink UI genuinely needs.
	RequiredIf *Predicate `json:"required_if,omitempty"`

	// Choices names a dynamic-choice hook the connector implements through
	// connector.ChoiceProvider. The frontend calls
	// GET /v1/connectors/{name}/choices/{hook} with the partial config. This is how
	// "pick a table from this database" works with no core knowledge that tables
	// exist.
	Choices string `json:"choices,omitempty"`
}

// EnumValue is one permitted value of a [TypeEnum] field, with its own title and
// description so a form can explain each option rather than only listing it.
type EnumValue struct {
	Value       string `json:"value"`
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
	Deprecated  bool   `json:"deprecated,omitempty"`
}

// Variant is one arm of a tagged union: a discriminator constant plus its fields.
//
// This is what a flat key-value config definition cannot express and fakes with
// dotted prefixes.
type Variant struct {
	Tag         string  `json:"tag"`
	Title       string  `json:"title"`
	Description string  `json:"description,omitempty"`
	Fields      []Field `json:"fields"`
}

// UnionTagKey is the key under which a [TypeUnion] value carries its discriminator.
// It is a named constant so that the Go validator, the JSON Schema generator and the
// browser cannot disagree about it.
const UnionTagKey = "type"
