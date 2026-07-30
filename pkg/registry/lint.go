package registry

import (
	"fmt"

	"github.com/BernardoCSACarreira/canal/pkg/config"
)

// lintSpec reports every structural defect in a component's own config declaration.
//
// These are AUTHOR mistakes, caught at init, because the alternative is a form whose conditional
// fields never appear or an accessor that silently returns a zero value for a path the spec never
// declared.
func lintSpec(kind Kind, name string, s *config.Spec) error {
	if s == nil {
		// A component with no configurable fields declares an empty spec, not a nil one: the
		// registry appends stage-standard fields to it, and a nil spec has nowhere to put them.
		return fmt.Errorf("%s %q has a nil config.Spec; use config.NewSpec()", kind, name)
	}
	var errs []error
	errs = append(errs, lintFields(kind, name, s.Fields, nil, s)...)
	for i := range s.Lints {
		for _, p := range s.Lints[i].Require.Paths() {
			if _, ok := s.Find(p...); !ok {
				errs = append(errs, fmt.Errorf(
					"%s %q: lint %q requires path %v, which the spec does not declare",
					kind, name, s.Lints[i].Message, p))
			}
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return joinErrs(errs)
}

func lintFields(kind Kind, name string, fields []config.Field, at []string, root *config.Spec) []error {
	var errs []error
	seen := map[string]struct{}{}
	for i := range fields {
		f := &fields[i]
		path := append(append([]string{}, at...), f.Name)
		if f.Name == "" {
			errs = append(errs, fmt.Errorf("%s %q: a field at %v has no name", kind, name, at))
			continue
		}
		if _, dup := seen[f.Name]; dup {
			errs = append(errs, fmt.Errorf("%s %q: duplicate field %v", kind, name, path))
		}
		seen[f.Name] = struct{}{}

		if f.Description == "" && f.Title == "" {
			errs = append(errs, fmt.Errorf(
				"%s %q: field %v has neither Title nor Description; the UI would render a bare key",
				kind, name, path))
		}
		if f.Type == config.TypeEnum && len(f.Enum) == 0 {
			errs = append(errs, fmt.Errorf("%s %q: enum field %v declares no values", kind, name, path))
		}
		if f.Type == config.TypeUnion && len(f.Variants) == 0 {
			errs = append(errs, fmt.Errorf("%s %q: union field %v declares no variants", kind, name, path))
		}
		if (f.Type == config.TypeArray || f.Type == config.TypeMap) && f.Item == nil {
			errs = append(errs, fmt.Errorf("%s %q: %s field %v declares no item", kind, name, f.Type, path))
		}
		if f.Pattern != "" && f.PatternHint == "" {
			errs = append(errs, fmt.Errorf(
				"%s %q: field %v declares a pattern with no PatternHint; a regex is not a message",
				kind, name, path))
		}
		for _, p := range f.ShowIf.Paths() {
			if _, ok := root.Find(p...); !ok {
				errs = append(errs, fmt.Errorf(
					"%s %q: field %v has a show_if referencing %v, which the spec does not declare",
					kind, name, path, p))
			}
		}
		for _, p := range f.RequiredIf.Paths() {
			if _, ok := root.Find(p...); !ok {
				errs = append(errs, fmt.Errorf(
					"%s %q: field %v has a required_if referencing %v, which the spec does not declare",
					kind, name, path, p))
			}
		}
		switch f.Type {
		case config.TypeObject:
			errs = append(errs, lintFields(kind, name, f.Fields, path, root)...)
		case config.TypeUnion:
			for j := range f.Variants {
				errs = append(errs, lintFields(kind, name, f.Variants[j].Fields, path, root)...)
			}
		}
	}
	return errs
}

// lintExamples asserts every declared example parses and validates against the spec, so a stale
// example fails at init rather than misleading an operator (design rule R10).
func lintExamples(kind Kind, name string, s *config.Spec) error {
	var errs []error
	for i := range s.Examples {
		_, d := s.Validate(s.Examples[i].Config)
		if d.HasErrors() {
			errs = append(errs, fmt.Errorf(
				"%s %q: example %q does not validate against its own spec:\n%s",
				kind, name, s.Examples[i].Title, d.Error()))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return joinErrs(errs)
}

type multiErr []error

func (m multiErr) Error() string {
	out := ""
	for i, e := range m {
		if i > 0 {
			out += "\n"
		}
		out += e.Error()
	}
	return out
}

func (m multiErr) Unwrap() []error { return m }

func joinErrs(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return multiErr(errs)
}
