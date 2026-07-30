package config

import (
	"fmt"
	"regexp"
)

// Validate is tier one of two-tier validation: structure, types, ranges, enums,
// UNKNOWN FIELDS and the declared lints, with no I/O whatsoever.
//
// It returns every problem at once and it returns a usable *Config alongside them, so a
// caller can report diagnostics and still inspect what parsed. Tier two is
// connector.Validator, which may do I/O; tier three is engine.Build, which negotiates
// capabilities.
//
// [CodeUnknownField] is the diagnostic that matters most in practice: a typo'd key that
// silently does nothing is the classic failure of every config-driven tool.
func (s *Spec) Validate(raw map[string]any) (*Config, Diagnostics) {
	if raw == nil {
		raw = map[string]any{}
	}
	c := NewConfig(s, raw)
	var d Diagnostics
	if s == nil {
		return c, d
	}
	d = validateObject(s.Fields, raw, nil, c, d)
	d = s.lint(c, d)
	return c, d
}

func validateObject(fields []Field, raw map[string]any, at []string, c *Config, d Diagnostics) Diagnostics {
	declared := make(map[string]struct{}, len(fields))
	for i := range fields {
		declared[fields[i].Name] = struct{}{}
	}
	for k := range raw {
		if _, ok := declared[k]; !ok {
			d = d.Errorf(CodeUnknownField, append(append([]string{}, at...), k),
				fmt.Sprintf("unknown field %q", k),
				"remove it, or check the spelling against the component's documented fields")
		}
	}
	for i := range fields {
		f := &fields[i]
		path := append(append([]string{}, at...), f.Name)
		v, present := raw[f.Name]

		if f.ShowIf != nil && !f.ShowIf.Eval(c) {
			// The field is not applicable to this configuration. Supplying it anyway is
			// a warning rather than an error: it is inert, not wrong.
			if present {
				d = d.Warnf(CodeUnknownField, path,
					fmt.Sprintf("%q does not apply to this configuration", f.Name),
					"it is ignored; remove it to avoid confusion")
			}
			continue
		}

		if !present {
			required := !f.Optional && f.Default == nil
			if f.RequiredIf != nil && f.RequiredIf.Eval(c) {
				required = true
			}
			if required {
				d = d.Errorf(CodeMissingField, path,
					fmt.Sprintf("%q is required", f.Name), f.Short)
			}
			continue
		}

		if f.Deprecated != "" {
			d = d.Warnf(CodeDeprecated, path,
				fmt.Sprintf("%q is deprecated", f.Name), "use "+f.Deprecated)
		}
		d = validateValue(f, v, path, c, d)
	}
	return d
}

func validateValue(f *Field, v any, path []string, c *Config, d Diagnostics) Diagnostics {
	switch f.Type {
	case TypeString:
		s, ok := v.(string)
		if !ok {
			return wrongType(d, path, "string", v)
		}
		if f.Pattern != "" {
			re, err := regexp.Compile(f.Pattern)
			switch {
			case err != nil:
				d = d.Errorf(CodeInvalidPattern, path,
					fmt.Sprintf("the declared pattern for %q does not compile", f.Name),
					"this is a connector defect; report it")
			case !re.MatchString(s):
				hint := f.PatternHint
				if hint == "" {
					hint = "must match " + f.Pattern
				}
				d = d.Errorf(CodeInvalidPattern, path,
					fmt.Sprintf("%q is not in the expected form", f.Name), hint)
			}
		}
	case TypeEnum:
		s, ok := v.(string)
		if !ok {
			return wrongType(d, path, "string", v)
		}
		found := false
		for i := range f.Enum {
			if f.Enum[i].Value == s {
				found = true
				if f.Enum[i].Deprecated {
					d = d.Warnf(CodeDeprecated, path,
						fmt.Sprintf("%q is a deprecated value of %q", s, f.Name), "")
				}
				break
			}
		}
		if !found {
			d = d.Errorf(CodeInvalidEnum, path,
				fmt.Sprintf("%q is not a permitted value of %q", s, f.Name),
				"permitted values: "+enumList(f.Enum))
		}
	case TypeBool:
		if _, ok := v.(bool); !ok {
			return wrongType(d, path, "bool", v)
		}
	case TypeInt:
		n, ok := asInt64(v)
		if !ok {
			return wrongType(d, path, "integer", v)
		}
		d = checkRange(f, float64(n), path, d)
	case TypeFloat:
		fl, ok := asFloat(v)
		if !ok {
			return wrongType(d, path, "number", v)
		}
		d = checkRange(f, fl, path, d)
	case TypeDuration:
		if _, err := ParseDuration(v); err != nil {
			d = d.Errorf(CodeWrongType, path, err.Error(), "for example \"250ms\" or \"1m\"")
		}
	case TypeSize:
		if _, err := ParseSize(v); err != nil {
			d = d.Errorf(CodeWrongType, path, err.Error(), "for example \"64MiB\"")
		}
	case TypeObject:
		m, ok := v.(map[string]any)
		if !ok {
			return wrongType(d, path, "object", v)
		}
		d = validateObject(f.Fields, m, path, c, d)
	case TypeArray:
		list, ok := v.([]any)
		if !ok {
			return wrongType(d, path, "array", v)
		}
		if f.Item != nil {
			for i, e := range list {
				d = validateValue(f.Item, e, append(path, fmt.Sprintf("[%d]", i)), c, d)
			}
		}
	case TypeMap:
		m, ok := v.(map[string]any)
		if !ok {
			return wrongType(d, path, "object", v)
		}
		if f.Item != nil {
			for k, e := range m {
				d = validateValue(f.Item, e, append(path, k), c, d)
			}
		}
	case TypeUnion:
		m, ok := v.(map[string]any)
		if !ok {
			return wrongType(d, path, "object", v)
		}
		tag, _ := m[UnionTagKey].(string)
		var arm *Variant
		for i := range f.Variants {
			if f.Variants[i].Tag == tag {
				arm = &f.Variants[i]
				break
			}
		}
		if arm == nil {
			d = d.Errorf(CodeInvalidEnum, append(path, UnionTagKey),
				fmt.Sprintf("%q is not a known variant of %q", tag, f.Name),
				"permitted variants: "+variantList(f.Variants))
			return d
		}
		rest := make(map[string]any, len(m))
		for k, e := range m {
			if k != UnionTagKey {
				rest[k] = e
			}
		}
		d = validateObject(arm.Fields, rest, path, c, d)
	case TypeMapping:
		// A mapping is validated structurally by the engine, which knows the record
		// model; here it must merely be an array of objects.
		if _, ok := v.([]any); !ok {
			return wrongType(d, path, "array", v)
		}
	default:
		d = d.Errorf(CodeWrongType, path,
			fmt.Sprintf("field %q declares unknown type %q", f.Name, f.Type),
			"this is a connector defect; report it")
	}
	return d
}

func (s *Spec) lint(c *Config, d Diagnostics) Diagnostics {
	for i := range s.Lints {
		l := &s.Lints[i]
		if l.When != nil && !l.When.Eval(c) {
			continue
		}
		if l.Require.Eval(c) {
			continue
		}
		code := l.Code
		if code == "" {
			code = CodeCustom
		}
		d = append(d, Diagnostic{
			Path:     l.Path,
			Severity: l.Severity,
			Code:     code,
			Message:  l.Message,
			Hint:     l.Hint,
		})
	}
	return d
}

func checkRange(f *Field, v float64, path []string, d Diagnostics) Diagnostics {
	if f.Min != nil && v < *f.Min {
		d = d.Errorf(CodeOutOfRange, path,
			fmt.Sprintf("%q must be at least %g", f.Name, *f.Min), "")
	}
	if f.Max != nil && v > *f.Max {
		d = d.Errorf(CodeOutOfRange, path,
			fmt.Sprintf("%q must be at most %g", f.Name, *f.Max), "")
	}
	return d
}

func wrongType(d Diagnostics, path []string, want string, got any) Diagnostics {
	return d.Errorf(CodeWrongType, path,
		fmt.Sprintf("expected a %s, got %T", want, got), "")
}

func enumList(vs []EnumValue) string {
	out := ""
	for i := range vs {
		if i > 0 {
			out += ", "
		}
		out += vs[i].Value
	}
	return out
}

func variantList(vs []Variant) string {
	out := ""
	for i := range vs {
		if i > 0 {
			out += ", "
		}
		out += vs[i].Tag
	}
	return out
}
