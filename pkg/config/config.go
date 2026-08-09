package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// RedactedMarker is what a secret becomes in every serialised form. It is a named
// constant so that no call site spells it, and so a fixture test can assert the real
// value never appears anywhere.
const RedactedMarker = "«redacted»"

// Config is a parsed, defaulted, validated config tree handed to a constructor.
//
// There is no Configure callback and no map re-parsed inside the connector: by the
// time a connector exists, its config is correct. That is why a connector's New must
// do no I/O and can be a three-line struct literal.
//
// Accessor errors accumulate on the Config so a constructor can check once at the
// end, which removes the twenty-line error ladder that a (T, error) accessor per field
// otherwise forces on every connector author.
type Config struct {
	spec *Spec
	raw  map[string]any
	errs []error
}

// NewConfig wraps an already-validated raw tree against its spec. [Spec.Validate] is
// the normal path and the only caller in the module today; it stays exported for a
// caller that arrives with a tree validated elsewhere — the connector conformance kit
// (ADR 0023, not yet built) is the intended one.
func NewConfig(s *Spec, raw map[string]any) *Config {
	if raw == nil {
		raw = map[string]any{}
	}
	return &Config{spec: s, raw: raw}
}

// Spec returns the declaration this config was parsed against. It may be nil for a
// hand-built test config.
func (c *Config) Spec() *Spec {
	if c == nil {
		return nil
	}
	return c.spec
}

// Raw returns the underlying tree. It is exported for the engine's redaction and
// round-trip paths; a connector uses the typed accessors.
func (c *Config) Raw() map[string]any {
	if c == nil {
		return nil
	}
	return c.raw
}

// Err returns the accumulated accessor errors, joined, or nil.
//
// A constructor therefore reads:
//
//	s := &src{
//	    path:  config.Must[string](c, "path"),
//	    limit: config.Must[int](c, "limit"),
//	}
//	return s, c.Err()
func (c *Config) Err() error {
	if c == nil || len(c.errs) == 0 {
		return nil
	}
	return errors.Join(c.errs...)
}

func (c *Config) note(err error) {
	if err != nil {
		c.errs = append(c.errs, err)
	}
}

// Has reports whether a value is present at path in the config itself, ignoring
// defaults.
func (c *Config) Has(path ...string) bool {
	if c == nil {
		return false
	}
	_, ok := c.lookup(path)
	return ok
}

// Get is the single generic accessor.
//
// It is named Get, not Field, because [Field] is the declaration TYPE in this same
// package: declaring both `type Field struct` and `func Field[T any]` is a
// redeclaration error, and the shortest way to never make that mistake is to name the
// accessor for what it does.
//
// A path that does not exist in the spec is a PROGRAMMING error, caught by the
// registry's spec cross-check rather than by returning a silent zero value. A path
// that exists but is absent from the config returns the declared default.
func Get[T any](c *Config, path ...string) (T, error) {
	var zero T
	if c == nil {
		return zero, errors.New("config: nil config")
	}
	raw, ok := c.lookup(path)
	if !ok {
		raw, ok = c.defaultFor(path)
	}
	if !ok {
		err := fmt.Errorf("config: %s is absent and declares no default", joinPath(path))
		c.note(err)
		return zero, err
	}
	out, err := coerce[T](raw)
	if err != nil {
		err = fmt.Errorf("config: %s: %w", joinPath(path), err)
		c.note(err)
		return zero, err
	}
	return out, nil
}

// Must is [Get] without the error return: the error is accumulated on c and read once
// through [Config.Err]. It returns the zero value on failure.
func Must[T any](c *Config, path ...string) T {
	v, _ := Get[T](c, path...)
	return v
}

// Object returns a sub-config rooted at path, so a component can hand a nested block
// to a helper without re-parsing.
func (c *Config) Object(path ...string) (*Config, error) {
	if c == nil {
		return nil, errors.New("config: nil config")
	}
	raw, ok := c.lookup(path)
	if !ok {
		if raw, ok = c.defaultFor(path); !ok {
			return NewConfig(c.spec, nil), nil
		}
	}
	m, ok := raw.(map[string]any)
	if !ok {
		err := fmt.Errorf("config: %s is not an object", joinPath(path))
		c.note(err)
		return nil, err
	}
	return &Config{spec: c.spec, raw: m}, nil
}

// List returns one sub-config per element of an array-of-objects field.
func (c *Config) List(path ...string) ([]*Config, error) {
	if c == nil {
		return nil, errors.New("config: nil config")
	}
	raw, ok := c.lookup(path)
	if !ok {
		if raw, ok = c.defaultFor(path); !ok {
			return nil, nil
		}
	}
	list, ok := raw.([]any)
	if !ok {
		err := fmt.Errorf("config: %s is not an array", joinPath(path))
		c.note(err)
		return nil, err
	}
	out := make([]*Config, 0, len(list))
	for i, e := range list {
		m, ok := e.(map[string]any)
		if !ok {
			err := fmt.Errorf("config: %s[%d] is not an object", joinPath(path), i)
			c.note(err)
			return nil, err
		}
		out = append(out, &Config{spec: c.spec, raw: m})
	}
	return out, nil
}

// Union returns the discriminator tag and the arm's config for a [TypeUnion] field.
func (c *Config) Union(path ...string) (tag string, cfg *Config, err error) {
	sub, err := c.Object(path...)
	if err != nil {
		return "", nil, err
	}
	t, ok := sub.lookup([]string{UnionTagKey})
	if !ok {
		err := fmt.Errorf("config: %s has no %q discriminator", joinPath(path), UnionTagKey)
		c.note(err)
		return "", nil, err
	}
	s, ok := t.(string)
	if !ok {
		err := fmt.Errorf("config: %s discriminator is not a string", joinPath(path))
		c.note(err)
		return "", nil, err
	}
	return s, sub, nil
}

// Secret is a DISTINCT accessor so a code review can grep every place a secret is
// read and so the core can count reads. It refuses to read a field the spec did not
// mark [Field.Secret], because a secret read through the ordinary accessor is a secret
// that will end up in a log.
func (c *Config) Secret(path ...string) (string, error) {
	if c == nil {
		return "", errors.New("config: nil config")
	}
	if f, ok := c.spec.Find(path...); ok && !f.Secret {
		err := fmt.Errorf("config: %s is not declared secret; use Get[string]", joinPath(path))
		c.note(err)
		return "", err
	}
	return Get[string](c, path...)
}

// Redacted returns a JSON-serialisable tree with every declared secret replaced by
// [RedactedMarker]. Wherever a config tree is serialised, this is what is serialised.
//
// TODAY THAT IS ONE PLACE: telemetry.PipelineStatus.Config, which the engine's status builder fills
// from here and which /status returns. Nothing else emits a config tree at all — /metrics carries a
// closed label set, no log line includes one, and the only other caller of [Config.Raw] is a connector
// reading its own field. So the rule holds because there is one path rather than because many paths
// remember, which is the difference between a structural guarantee and a convention.
//
// It said "the read model, every log line and every API response use this and nothing else" while
// having no caller outside its own tests, and PipelineStatus.Config had no writer. A redactor and a
// field, each documented as connected to the other, connected to nothing.
//
// It returns values, not pointers into state (design rule R13): mutating the result
// cannot reach the live config.
func (c *Config) Redacted() map[string]any {
	if c == nil {
		return map[string]any{}
	}
	var fields []Field
	if c.spec != nil {
		fields = c.spec.Fields
	}
	out, _ := redactTree(c.raw, fields).(map[string]any)
	if out == nil {
		out = map[string]any{}
	}
	return out
}

func redactTree(v any, fields []Field) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			f := fieldNamed(fields, k)
			switch {
			case f != nil && f.Secret:
				out[k] = RedactedMarker
			case f != nil && f.Type == TypeObject:
				out[k] = redactTree(e, f.Fields)
			case f != nil && (f.Type == TypeArray || f.Type == TypeMap) && f.Item != nil:
				out[k] = redactTree(e, f.Item.Fields)
			case f != nil && f.Type == TypeUnion:
				out[k] = redactTree(e, unionFields(f, e))
			default:
				out[k] = redactTree(e, nil)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = redactTree(x[i], fields)
		}
		return out
	default:
		return v
	}
}

func unionFields(f *Field, v any) []Field {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	tag, _ := m[UnionTagKey].(string)
	for i := range f.Variants {
		if f.Variants[i].Tag == tag {
			return f.Variants[i].Fields
		}
	}
	return nil
}

func fieldNamed(fields []Field, name string) *Field {
	for i := range fields {
		if fields[i].Name == name {
			return &fields[i]
		}
	}
	return nil
}

func (c *Config) lookup(path []string) (any, bool) {
	if c == nil || len(path) == 0 {
		return nil, false
	}
	var cur any = c.raw
	for _, seg := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[seg]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func (c *Config) defaultFor(path []string) (any, bool) {
	f, ok := c.spec.Find(path...)
	if !ok || f.Default == nil {
		return nil, false
	}
	return f.Default, true
}

func joinPath(path []string) string { return strings.Join(path, ".") }

// coerce converts a raw config value, as produced by a YAML or JSON decoder, into the
// requested Go type. The accepted conversions are deliberately narrow: a config that
// needs a surprising coercion is a config the spec should have declared differently.
func coerce[T any](v any) (T, error) {
	var zero T
	if t, ok := v.(T); ok {
		return t, nil
	}
	switch any(zero).(type) {
	case string:
		s, ok := v.(string)
		if !ok {
			return zero, fmt.Errorf("expected a string, got %T", v)
		}
		return any(s).(T), nil
	case bool:
		b, ok := v.(bool)
		if !ok {
			return zero, fmt.Errorf("expected a bool, got %T", v)
		}
		return any(b).(T), nil
	case int:
		n, ok := asInt64(v)
		if !ok {
			return zero, fmt.Errorf("expected an integer, got %T", v)
		}
		return any(int(n)).(T), nil
	case int64:
		n, ok := asInt64(v)
		if !ok {
			return zero, fmt.Errorf("expected an integer, got %T", v)
		}
		return any(n).(T), nil
	case uint64:
		n, ok := asInt64(v)
		if !ok || n < 0 {
			return zero, fmt.Errorf("expected a non-negative integer, got %T", v)
		}
		return any(uint64(n)).(T), nil
	case float64:
		f, ok := asFloat(v)
		if !ok {
			return zero, fmt.Errorf("expected a number, got %T", v)
		}
		return any(f).(T), nil
	case time.Duration:
		d, err := ParseDuration(v)
		if err != nil {
			return zero, err
		}
		return any(d).(T), nil
	case []string:
		list, ok := v.([]any)
		if !ok {
			return zero, fmt.Errorf("expected an array, got %T", v)
		}
		out := make([]string, 0, len(list))
		for _, e := range list {
			s, ok := e.(string)
			if !ok {
				return zero, fmt.Errorf("expected an array of strings, got a %T element", e)
			}
			out = append(out, s)
		}
		return any(out).(T), nil
	case map[string]any:
		m, ok := v.(map[string]any)
		if !ok {
			return zero, fmt.Errorf("expected an object, got %T", v)
		}
		return any(m).(T), nil
	case []any:
		l, ok := v.([]any)
		if !ok {
			return zero, fmt.Errorf("expected an array, got %T", v)
		}
		return any(l).(T), nil
	default:
		return zero, fmt.Errorf("cannot represent %T as %T", v, zero)
	}
}

// ParseDuration accepts a Go duration string, a number of seconds, or an already-typed
// time.Duration. It is exported because the engine parses the same
// stage-standard duration fields.
func ParseDuration(v any) (time.Duration, error) {
	switch x := v.(type) {
	case time.Duration:
		return x, nil
	case string:
		d, err := time.ParseDuration(x)
		if err != nil {
			return 0, fmt.Errorf("expected a duration such as \"1s\" or \"500ms\": %w", err)
		}
		return d, nil
	default:
		if f, ok := asFloat(v); ok {
			return time.Duration(f * float64(time.Second)), nil
		}
		return 0, fmt.Errorf("expected a duration, got %T", v)
	}
}

// ParseSize accepts a size written the way operators write it — "64MiB", "1GB",
// "512" — and returns bytes. Binary and decimal prefixes are both honoured and mean
// different things, because silently treating MiB as MB is how a request-size cap ends
// up five percent wrong.
func ParseSize(v any) (int64, error) {
	switch x := v.(type) {
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, errors.New("expected a size such as \"64MiB\"")
		}
		i := 0
		for i < len(s) && (s[i] == '.' || s[i] == '-' || (s[i] >= '0' && s[i] <= '9')) {
			i++
		}
		num, unit := s[:i], strings.ToLower(strings.TrimSpace(s[i:]))
		f, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return 0, fmt.Errorf("expected a size such as \"64MiB\": %w", err)
		}
		mult := int64(1)
		switch unit {
		case "", "b":
		case "kib":
			mult = 1 << 10
		case "mib":
			mult = 1 << 20
		case "gib":
			mult = 1 << 30
		case "tib":
			mult = 1 << 40
		case "kb":
			mult = 1000
		case "mb":
			mult = 1000 * 1000
		case "gb":
			mult = 1000 * 1000 * 1000
		case "tb":
			mult = 1000 * 1000 * 1000 * 1000
		default:
			return 0, fmt.Errorf("unknown size unit %q", unit)
		}
		return int64(f * float64(mult)), nil
	default:
		if n, ok := asInt64(v); ok {
			return n, nil
		}
		return 0, fmt.Errorf("expected a size, got %T", v)
	}
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		return int64(x), true
	case float32:
		return int64(x), float64(int64(x)) == float64(x)
	case float64:
		return int64(x), float64(int64(x)) == x
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func asFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	default:
		if n, ok := asInt64(v); ok {
			return float64(n), true
		}
		return 0, false
	}
}

func writeAny(b *strings.Builder, v any) {
	switch x := v.(type) {
	case nil:
	case string:
		b.WriteString(x)
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case float64:
		b.WriteString(strconv.FormatFloat(x, 'g', -1, 64))
	default:
		if n, ok := asInt64(v); ok {
			b.WriteString(strconv.FormatInt(n, 10))
			return
		}
		fmt.Fprintf(b, "%v", v)
	}
}
