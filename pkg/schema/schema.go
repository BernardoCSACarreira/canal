package schema

// Schema is a structural description of a stream's records.
//
// Identity is the structural fingerprint of the normalised form, so two
// independently-discovered identical schemas are the same schema and the
// pipeline's schema table deduplicates them.
type Schema struct {
	Fields []Field `json:"fields"`

	// Keys are the field paths forming the record key, in order. [][]string,
	// not []string, so composite and nested keys need no later breaking change.
	Keys [][]string `json:"keys,omitempty"`

	// Open means additional undeclared fields may appear. A closed schema plus
	// an undeclared field is a drift event; an open one is not.
	Open bool `json:"open"`
}

// Field is one declared field of a [Schema].
type Field struct {
	Name     string `json:"name"`
	Type     Type   `json:"type"`
	Nullable bool   `json:"nullable"`

	// Fields describes a [TypeStruct]'s members.
	Fields []Field `json:"fields,omitempty"`

	// Item describes a [TypeList]'s element or a [TypeMap]'s value.
	Item *Field `json:"item,omitempty"`

	// Variants is the alternative shapes a per-record union field may take, in no
	// particular order. Non-empty only for a field whose Type is [TypeAny].
	//
	// It exists because a genuine union — a payload column that is an int for one record
	// and a struct for the next — was expressible only two ways, both bad. TypeAny alone
	// ERASES the variant set, so a sink that could handle {int64, string} but not
	// {int64, struct} could not be refused at submit time and discovered the struct at three
	// in the morning. Logical.Name is the magic-name convention this package's own
	// documentation condemns.
	//
	// Declaring the set keeps the refusal where every other capability mismatch lives: at
	// submit time, as a diagnostic naming both sides.
	Variants []Field `json:"variants,omitempty"`

	// Logical carries parameterised type detail. Nil when the canonical Type is
	// fully descriptive.
	Logical *Logical `json:"logical,omitempty"`

	Doc string `json:"doc,omitempty"`
}

// Entry is one schema in the pipeline's table, as handed to a sink at Open so it
// can create or alter the destination before the first record needing it.
type Entry struct {
	Ref    Ref     `json:"ref"`
	Schema *Schema `json:"schema"`
}
