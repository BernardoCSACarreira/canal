package config

import (
	"regexp"
	"strings"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Predicate is a declarative, side-effect-free condition. Its operator set is
// CLOSED, so it is trivially evaluable in Go and in the browser and cannot become an
// embedded language by accident.
//
// An embedded expression language is a real dependency with reach into lint rules,
// batch predicates, partition templates and sink mappings, and it must also be
// evaluable in a browser. canal declines it: about ten percent of mappings will need
// code, and those are a transform.
//
// One predicate type, two evaluation contexts — [Predicate.Eval] over a config being
// edited and [Predicate.EvalRecord] over a record in flight — because a second
// predicate vocabulary would be design rule R9 violated.
type Predicate struct {
	// Path is addressed as segments, never as a dotted string, so a field name
	// containing a dot cannot forge a path.
	Path []string `json:"path,omitempty"`
	Op   PredOp   `json:"op"`

	// Value is the literal to compare Path against. Exactly one of Value and Other is
	// meaningful; Other wins when both are set.
	Value any `json:"value,omitempty"`

	// Other is a SECOND PATH to compare Path against, for a field-to-field condition.
	//
	// Without it a predicate could only compare a path to a literal, so the whole class
	// of lint rule that relates two fields was unwritable: "min_batch_bytes exceeds
	// max_batch_bytes", "the flush interval is longer than the lease TTL", "the
	// chunk-size floor is above its ceiling". Those are the lints an operator most needs
	// at submit time, and every one of them is a comparison between two things the
	// operator typed.
	Other []string `json:"other,omitempty"`

	All []Predicate `json:"all,omitempty"`
	Any []Predicate `json:"any,omitempty"`
	Not *Predicate  `json:"not,omitempty"`
}

// PredOp is the closed operator set.
type PredOp string

const (
	PredEquals      PredOp = "equals"
	PredNotEquals   PredOp = "not_equals"
	PredIn          PredOp = "in"
	PredPresent     PredOp = "present"
	PredTruthy      PredOp = "truthy"
	PredGreaterThan PredOp = "gt"
	PredLessThan    PredOp = "lt"
	PredMatches     PredOp = "matches"
	// PredAll, PredAny and PredNot combine sub-predicates. They are named operators
	// rather than implicit structure so that a serialised predicate is unambiguous.
	PredAll PredOp = "all"
	PredAny PredOp = "any"
	PredNot PredOp = "not"
)

// Eval reports whether p holds for the config being edited. A nil predicate holds.
func (p *Predicate) Eval(c *Config) bool {
	if p == nil {
		return true
	}
	return p.eval(func(path []string) (any, bool) {
		if c == nil {
			return nil, false
		}
		return c.lookup(path)
	})
}

// EvalRecord reports whether p holds for a record in flight. It is what a batch
// policy's flush predicate and a route transform evaluate, and it deliberately reuses
// the same closed operator set: there is no second language for records.
//
// Paths resolve against the record's structured payload. A path of the form
// ["meta", ns, key] resolves against metadata instead, and the reserved single-
// segment paths "dest", "stream", "key" and "op" resolve against the envelope. A path
// that cannot be resolved is treated as absent, never as zero.
func (p *Predicate) EvalRecord(r *record.Record) bool {
	if p == nil {
		return true
	}
	return p.eval(func(path []string) (any, bool) { return resolveRecordPath(r, path) })
}

func (p *Predicate) eval(lookup func([]string) (any, bool)) bool {
	switch p.Op {
	case PredAll:
		for i := range p.All {
			if !p.All[i].eval(lookup) {
				return false
			}
		}
		return true
	case PredAny:
		for i := range p.Any {
			if p.Any[i].eval(lookup) {
				return true
			}
		}
		return len(p.Any) == 0
	case PredNot:
		return p.Not == nil || !p.Not.eval(lookup)
	}

	// A predicate may also carry combinators alongside a leaf op; every one present
	// must hold, which keeps `all` expressible without nesting.
	for i := range p.All {
		if !p.All[i].eval(lookup) {
			return false
		}
	}
	if len(p.Any) > 0 {
		ok := false
		for i := range p.Any {
			if p.Any[i].eval(lookup) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if p.Not != nil && p.Not.eval(lookup) {
		return false
	}
	if p.Op == "" {
		return true
	}

	got, present := lookup(p.Path)

	// The right-hand side is either a literal or a second path. A second path that does
	// not resolve makes the whole comparison false rather than zero: a lint comparing two
	// fields must not fire because one of them is absent.
	want := p.Value
	if len(p.Other) > 0 {
		v, ok := lookup(p.Other)
		if !ok {
			return false
		}
		want = v
	}

	switch p.Op {
	case PredPresent:
		return present
	case PredTruthy:
		return present && truthy(got)
	case PredEquals:
		return present && looseEqual(got, want)
	case PredNotEquals:
		return !present || !looseEqual(got, want)
	case PredIn:
		if !present {
			return false
		}
		list, ok := want.([]any)
		if !ok {
			return false
		}
		for _, w := range list {
			if looseEqual(got, w) {
				return true
			}
		}
		return false
	case PredGreaterThan:
		if !present {
			return false
		}
		a, aok := asQuantity(got)
		b, bok := asQuantity(want)
		return aok && bok && a > b
	case PredLessThan:
		if !present {
			return false
		}
		a, aok := asQuantity(got)
		b, bok := asQuantity(want)
		return aok && bok && a < b
	case PredMatches:
		if !present {
			return false
		}
		pat, ok := want.(string)
		if !ok {
			return false
		}
		re, err := regexp.Compile(pat)
		if err != nil {
			return false
		}
		return re.MatchString(asString(got))
	default:
		return false
	}
}

// asQuantity is asFloat widened to the two string-shaped scalar kinds config actually
// holds: durations like "5m" and byte sizes like "128MiB".
//
// It exists because gt and lt evaluated through asFloat alone, which cannot parse either,
// so both operands failed to convert and every duration or size comparison returned
// FALSE UNCONDITIONALLY. A lint written as "warn when the flush interval exceeds 5m"
// therefore never fired, and a lint written as its negation fired always — a predicate
// that is quietly constant is worse than one that is missing, because the form looks
// validated.
func asQuantity(v any) (float64, bool) {
	if f, ok := asFloat(v); ok {
		return f, true
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return 0, false
	}
	if d, err := ParseDuration(s); err == nil {
		return float64(d), true
	}
	if n, err := ParseSize(s); err == nil {
		return float64(n), true
	}
	return 0, false
}

// Paths walks p and every sub-predicate, yielding each referenced path. The registry
// uses it to fail a spec whose predicate references a nonexistent field: a form whose
// conditional fields never appear is a form nobody can fill in.
func (p *Predicate) Paths() [][]string {
	if p == nil {
		return nil
	}
	var out [][]string
	if len(p.Path) > 0 {
		out = append(out, p.Path)
	}
	if len(p.Other) > 0 {
		out = append(out, p.Other)
	}
	for i := range p.All {
		out = append(out, p.All[i].Paths()...)
	}
	for i := range p.Any {
		out = append(out, p.Any[i].Paths()...)
	}
	out = append(out, p.Not.Paths()...)
	return out
}

func resolveRecordPath(r *record.Record, path []string) (any, bool) {
	if r == nil || len(path) == 0 {
		return nil, false
	}
	if path[0] == "meta" && len(path) == 3 {
		v, ok := r.Meta.Get(path[1], path[2])
		if !ok {
			return nil, false
		}
		return valueToAny(v), true
	}
	if len(path) == 1 {
		switch path[0] {
		case "dest":
			return string(r.Dest), true
		case "stream":
			return string(r.Origin().Stream), true
		case "key":
			return string(r.Origin().Key), r.Origin().Key != nil
		case "op":
			if r.Change == nil {
				return nil, false
			}
			return r.Change.Op.String(), true
		case "tx_id":
			if r.Change == nil {
				return nil, false
			}
			return r.Change.TxID, true
		}
	}
	v, ok := r.Payload.Structured()
	if !ok {
		return nil, false
	}
	return walkValue(v, path)
}

func walkValue(v record.Value, path []string) (any, bool) {
	cur := v
	for _, seg := range path {
		m, ok := cur.(record.Map)
		if !ok {
			return nil, false
		}
		next, ok := m[seg]
		if !ok {
			return nil, false
		}
		cur = next
	}
	return valueToAny(cur), true
}

func valueToAny(v record.Value) any {
	switch x := v.(type) {
	case nil:
		return nil
	case record.Null:
		return nil
	case record.Bool:
		return bool(x)
	case record.Int:
		return int64(x)
	case record.Uint:
		return x
	case record.Float:
		return float64(x)
	case record.String:
		return string(x)
	case record.Bytes:
		return string(x)
	default:
		return x
	}
}

func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	default:
		if f, ok := asFloat(v); ok {
			return f != 0
		}
		return true
	}
}

func looseEqual(a, b any) bool {
	if af, aok := asFloat(a); aok {
		if bf, bok := asFloat(b); bok {
			return af == bf
		}
	}
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		return as == bs
	}
	ab, aok2 := a.(bool)
	bb, bok2 := b.(bool)
	if aok2 && bok2 {
		return ab == bb
	}
	return asString(a) == asString(b)
}

func asString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	default:
		var b strings.Builder
		writeAny(&b, x)
		return b.String()
	}
}
