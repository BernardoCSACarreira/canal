package config

// Spec is a component's config declaration. Built once at init, frozen thereafter,
// exported as DATA. Nothing on it is a callback.
//
// One Spec is simultaneously the validator, the defaulter, the docs source, the JSON
// Schema for editors, and the UI form model. That is the whole reason a specialised
// connector UI needs no core change: the specialisation is in the data.
type Spec struct {
	Summary     string `json:"summary"`
	Description string `json:"description"`
	Version     string `json:"version"`
	Deprecated  string `json:"deprecated,omitempty"`

	Fields []Field `json:"fields"`

	// Examples are COMPLETE, VALID configs. The conformance kit parses and validates
	// every one, so a stale example fails CI (design rule R10).
	Examples []Example `json:"examples,omitempty"`

	// Lints are declarative cross-field rules, evaluated offline with no I/O.
	Lints []Lint `json:"lints,omitempty"`
}

// NewSpec returns an empty spec ready for the builder methods.
func NewSpec() *Spec { return &Spec{} }

// Field appends a declared field and returns s, so a spec reads as one expression.
func (s *Spec) Field(f Field) *Spec {
	s.Fields = append(s.Fields, f)
	return s
}

// Lint appends a declarative cross-field rule and returns s.
func (s *Spec) Lint(l Lint) *Spec {
	s.Lints = append(s.Lints, l)
	return s
}

// Example appends a complete, valid config and returns s.
func (s *Spec) Example(e Example) *Spec {
	s.Examples = append(s.Examples, e)
	return s
}

// Describe sets the summary and description and returns s.
func (s *Spec) Describe(summary, description string) *Spec {
	s.Summary, s.Description = summary, description
	return s
}

// Find returns the declared field at path, walking objects, arrays, maps and union
// variants. It is what the predicate cross-check and the spec-path conformance case
// use, and it is the reason a mistyped path is a registration-time lint failure
// rather than a silent zero value in production.
func (s *Spec) Find(path ...string) (*Field, bool) {
	if s == nil || len(path) == 0 {
		return nil, false
	}
	return findField(s.Fields, path)
}

func findField(fields []Field, path []string) (*Field, bool) {
	if len(path) == 0 {
		return nil, false
	}
	for i := range fields {
		f := &fields[i]
		if f.Name != path[0] {
			continue
		}
		if len(path) == 1 {
			return f, true
		}
		rest := path[1:]
		switch f.Type {
		case TypeObject:
			return findField(f.Fields, rest)
		case TypeArray, TypeMap:
			if f.Item == nil {
				return nil, false
			}
			// An array or map element is addressed by the element's own field name,
			// so the item's sub-fields are the next hop.
			if f.Item.Type == TypeObject {
				return findField(f.Item.Fields, rest)
			}
			return nil, false
		case TypeUnion:
			for vi := range f.Variants {
				if found, ok := findField(f.Variants[vi].Fields, rest); ok {
					return found, true
				}
			}
			return nil, false
		default:
			return nil, false
		}
	}
	return nil, false
}

// Example is one complete, valid configuration for a component, with the prose that
// explains when to use it.
//
// Scaffolding is labelled and tested against what it stands in for (design rule
// R10), and an Example is the labelled form: the conformance kit validates every one,
// so an example that has drifted from the spec fails CI instead of misleading an
// operator.
type Example struct {
	Title       string         `json:"title"`
	Description string         `json:"description,omitempty"`
	Config      map[string]any `json:"config"`
}

// Lint is a declarative cross-field rule. It is data, not a closure, so the browser
// evaluates it without a round trip and the docs generator can print it.
type Lint struct {
	// When is the condition under which the rule applies. Nil means always.
	When *Predicate `json:"when,omitempty"`
	// Require is the condition that must then hold.
	Require Predicate `json:"require"`

	Severity Severity `json:"severity"`
	Code     Code     `json:"code"`
	Message  string   `json:"message"`
	Hint     string   `json:"hint,omitempty"`
	// Path anchors the diagnostic so a form renders it inline.
	Path []string `json:"path,omitempty"`
}
