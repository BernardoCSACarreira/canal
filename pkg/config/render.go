package config

import (
	"encoding/json"
	"fmt"
	"strings"
)

// JSONSchema generates a JSON Schema for s, for editor completion and for any
// non-canal validator.
//
// It is GENERATED, never hand-maintained: a hand-written schema beside a Go
// declaration is two representations of one entity, which is design rule R9's
// modelling error and design rule R8's structural drift in one move.
func (s *Spec) JSONSchema() ([]byte, error) {
	if s == nil {
		return json.Marshal(map[string]any{"type": "object"})
	}
	return json.MarshalIndent(objectSchema(s.Fields, s.Description), "", "  ")
}

func objectSchema(fields []Field, description string) map[string]any {
	props := make(map[string]any, len(fields))
	var required []string
	for i := range fields {
		f := &fields[i]
		props[f.Name] = fieldSchema(f)
		if !f.Optional && f.Default == nil {
			required = append(required, f.Name)
		}
	}
	out := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
	}
	if description != "" {
		out["description"] = description
	}
	if len(required) > 0 {
		out["required"] = required
	}
	return out
}

func fieldSchema(f *Field) map[string]any {
	out := map[string]any{}
	if f.Title != "" {
		out["title"] = f.Title
	}
	if f.Description != "" {
		out["description"] = f.Description
	}
	if f.Default != nil {
		out["default"] = f.Default
	}
	if f.Deprecated != "" {
		out["deprecated"] = true
	}
	if len(f.Examples) > 0 {
		out["examples"] = f.Examples
	}
	if f.Secret {
		// A schema consumer that honours writeOnly will not echo the value back.
		out["writeOnly"] = true
	}

	switch f.Type {
	case TypeString:
		out["type"] = "string"
		if f.Pattern != "" {
			out["pattern"] = f.Pattern
		}
	case TypeEnum:
		out["type"] = "string"
		vals := make([]string, 0, len(f.Enum))
		for i := range f.Enum {
			vals = append(vals, f.Enum[i].Value)
		}
		out["enum"] = vals
	case TypeBool:
		out["type"] = "boolean"
	case TypeInt:
		out["type"] = "integer"
		if f.Min != nil {
			out["minimum"] = *f.Min
		}
		if f.Max != nil {
			out["maximum"] = *f.Max
		}
	case TypeFloat:
		out["type"] = "number"
		if f.Min != nil {
			out["minimum"] = *f.Min
		}
		if f.Max != nil {
			out["maximum"] = *f.Max
		}
	case TypeDuration:
		out["type"] = []string{"string", "number"}
		out["examples"] = []any{"250ms", "1m"}
	case TypeSize:
		out["type"] = []string{"string", "integer"}
		out["examples"] = []any{"64MiB"}
	case TypeObject:
		return mergeSchema(out, objectSchema(f.Fields, ""))
	case TypeArray:
		out["type"] = "array"
		if f.Item != nil {
			out["items"] = fieldSchema(f.Item)
		}
	case TypeMap:
		out["type"] = "object"
		if f.Item != nil {
			out["additionalProperties"] = fieldSchema(f.Item)
		}
	case TypeUnion:
		arms := make([]any, 0, len(f.Variants))
		for i := range f.Variants {
			arm := objectSchema(f.Variants[i].Fields, f.Variants[i].Description)
			props, _ := arm["properties"].(map[string]any)
			props[UnionTagKey] = map[string]any{"const": f.Variants[i].Tag}
			req, _ := arm["required"].([]string)
			arm["required"] = append(req, UnionTagKey)
			arm["title"] = f.Variants[i].Title
			arms = append(arms, arm)
		}
		out["oneOf"] = arms
	case TypeMapping:
		out["type"] = "array"
		out["items"] = map[string]any{"type": "object"}
	default:
		out["type"] = "string"
	}
	return out
}

func mergeSchema(into, from map[string]any) map[string]any {
	for k, v := range from {
		if _, exists := into[k]; !exists {
			into[k] = v
		}
	}
	return into
}

// Docs renders s as Markdown reference documentation.
//
// It is generated from the same declaration the validator and the form use, which is
// how "the docs say one thing and the code does another" is prevented structurally
// rather than by review.
func (s *Spec) Docs() ([]byte, error) {
	var b strings.Builder
	if s == nil {
		return []byte(nil), nil
	}
	if s.Summary != "" {
		fmt.Fprintf(&b, "%s\n\n", s.Summary)
	}
	if s.Description != "" {
		fmt.Fprintf(&b, "%s\n\n", s.Description)
	}
	if s.Deprecated != "" {
		fmt.Fprintf(&b, "> Deprecated: %s\n\n", s.Deprecated)
	}
	docFields(&b, s.Fields, nil)
	if len(s.Examples) > 0 {
		b.WriteString("## Examples\n\n")
		for i := range s.Examples {
			fmt.Fprintf(&b, "### %s\n\n", s.Examples[i].Title)
			if s.Examples[i].Description != "" {
				fmt.Fprintf(&b, "%s\n\n", s.Examples[i].Description)
			}
			enc, err := json.MarshalIndent(s.Examples[i].Config, "", "  ")
			if err != nil {
				return nil, err
			}
			fmt.Fprintf(&b, "```json\n%s\n```\n\n", enc)
		}
	}
	return []byte(b.String()), nil
}

func docFields(b *strings.Builder, fields []Field, at []string) {
	for i := range fields {
		f := &fields[i]
		path := append(append([]string{}, at...), f.Name)
		fmt.Fprintf(b, "### `%s`\n\n", joinPath(path))
		fmt.Fprintf(b, "Type: `%s`", f.Type)
		if f.Optional {
			b.WriteString(" · optional")
		}
		if f.Advanced {
			b.WriteString(" · advanced")
		}
		if f.Secret {
			b.WriteString(" · secret")
		}
		if f.Default != nil {
			fmt.Fprintf(b, " · default `%v`", f.Default)
		}
		b.WriteString("\n\n")
		if f.Deprecated != "" {
			fmt.Fprintf(b, "> Deprecated: %s\n\n", f.Deprecated)
		}
		if f.Description != "" {
			fmt.Fprintf(b, "%s\n\n", f.Description)
		}
		for j := range f.Enum {
			fmt.Fprintf(b, "- `%s` — %s\n", f.Enum[j].Value, f.Enum[j].Title)
		}
		if len(f.Enum) > 0 {
			b.WriteString("\n")
		}
		switch f.Type {
		case TypeObject:
			docFields(b, f.Fields, path)
		case TypeUnion:
			for j := range f.Variants {
				fmt.Fprintf(b, "#### variant `%s`\n\n", f.Variants[j].Tag)
				docFields(b, f.Variants[j].Fields, path)
			}
		}
	}
}
