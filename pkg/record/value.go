package record

import (
	"bytes"
	"time"
)

// Value is canal's field value type: a sealed sum with a closed member set.
//
// It is an interface with an unexported method rather than `any` plus a documented
// type set, because a third party widening the set would be a checkpoint-format
// break and a codec break simultaneously. It is not a type parameter because one
// record holds heterogeneous fields.
//
// Note what is absent: there is no stream or lazy member. A Value must be fully
// materialised, because a record must be encodable, bufferable,
// dead-letterable and wire-shippable at every instant of its life. A lazily-read
// nested stream inside a Value makes all four impossible.
//
// Nil-vs-Null: a nil Value means "no value was supplied". [Null] means "the value
// is explicitly null". They are different facts and canal never collapses them; a
// codec that cannot express the distinction says so in its capabilities.
type Value interface {
	isValue()

	// Kind reports which member of the closed set this is. It is safe as a metric
	// label.
	Kind() Kind
}

// Kind enumerates the closed value set. Safe as a metric label.
type Kind uint8

const (
	KindNull Kind = iota
	KindBool
	KindInt
	KindUint
	KindFloat
	KindString
	KindBytes
	KindTime
	KindDecimal
	KindList
	KindMap
)

var kindNames = [...]string{
	KindNull:    "null",
	KindBool:    "bool",
	KindInt:     "int",
	KindUint:    "uint",
	KindFloat:   "float",
	KindString:  "string",
	KindBytes:   "bytes",
	KindTime:    "time",
	KindDecimal: "decimal",
	KindList:    "list",
	KindMap:     "map",
}

// String returns the stable snake_case token for k. It is simultaneously the wire
// form, the metric label value and the i18n key suffix (design rule R9).
func (k Kind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "null"
}

// The closed member set of [Value].
type (
	// Null is an explicitly null value, distinct from a nil Value.
	Null struct{}

	Bool   bool
	Int    int64
	Uint   uint64
	Float  float64
	String string
	Bytes  []byte
	Time   time.Time

	// Decimal is arbitrary-precision and transport-neutral. A source that cannot
	// report precision needs exactly this escape hatch; the untyped-JSON
	// alternative became the worst part of one surveyed product and was
	// eventually deleted.
	Decimal struct {
		// Unscaled is the two's-complement, big-endian unscaled value.
		Unscaled []byte
		Scale    int32
	}

	// List is an ordered sequence. A nil element means "no value supplied";
	// Null{} means "explicitly null".
	List []Value

	// Map is a keyed structure. Key order is not significant.
	Map map[string]Value
)

func (Null) isValue()    {}
func (Bool) isValue()    {}
func (Int) isValue()     {}
func (Uint) isValue()    {}
func (Float) isValue()   {}
func (String) isValue()  {}
func (Bytes) isValue()   {}
func (Time) isValue()    {}
func (Decimal) isValue() {}
func (List) isValue()    {}
func (Map) isValue()     {}

// Kind reports the closed-set member.
func (Null) Kind() Kind { return KindNull }

// Kind reports the closed-set member.
func (Bool) Kind() Kind { return KindBool }

// Kind reports the closed-set member.
func (Int) Kind() Kind { return KindInt }

// Kind reports the closed-set member.
func (Uint) Kind() Kind { return KindUint }

// Kind reports the closed-set member.
func (Float) Kind() Kind { return KindFloat }

// Kind reports the closed-set member.
func (String) Kind() Kind { return KindString }

// Kind reports the closed-set member.
func (Bytes) Kind() Kind { return KindBytes }

// Kind reports the closed-set member.
func (Time) Kind() Kind { return KindTime }

// Kind reports the closed-set member.
func (Decimal) Kind() Kind { return KindDecimal }

// Kind reports the closed-set member.
func (List) Kind() Kind { return KindList }

// Kind reports the closed-set member.
func (Map) Kind() Kind { return KindMap }

// Equal is a deep structural comparison. It exists as a function rather than as
// `==` because [Map] and [List] are not comparable and `a == b` on two Values
// panics at runtime — a trap this function's existence removes.
//
// Two nil Values are equal. A nil Value is never equal to Null{}.
func Equal(a, b Value) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Kind() != b.Kind() {
		return false
	}
	switch x := a.(type) {
	case Null:
		return true
	case Bool:
		return x == b.(Bool)
	case Int:
		return x == b.(Int)
	case Uint:
		return x == b.(Uint)
	case Float:
		return x == b.(Float)
	case String:
		return x == b.(String)
	case Bytes:
		return bytes.Equal(x, b.(Bytes))
	case Time:
		return time.Time(x).Equal(time.Time(b.(Time)))
	case Decimal:
		y := b.(Decimal)
		return x.Scale == y.Scale && bytes.Equal(x.Unscaled, y.Unscaled)
	case List:
		y := b.(List)
		if len(x) != len(y) {
			return false
		}
		for i := range x {
			if !Equal(x[i], y[i]) {
				return false
			}
		}
		return true
	case Map:
		y := b.(Map)
		if len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !Equal(v, w) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// CloneValue returns a deep copy of v. Fan-out and derivation deep-copy every
// structured view, because a value type containing a reference field is how
// branches end up sharing mutable state.
func CloneValue(v Value) Value {
	switch x := v.(type) {
	case nil:
		return nil
	case Bytes:
		if x == nil {
			return Bytes(nil)
		}
		c := make(Bytes, len(x))
		copy(c, x)
		return c
	case Decimal:
		return Decimal{Unscaled: append([]byte(nil), x.Unscaled...), Scale: x.Scale}
	case List:
		if x == nil {
			return List(nil)
		}
		c := make(List, len(x))
		for i := range x {
			c[i] = CloneValue(x[i])
		}
		return c
	case Map:
		if x == nil {
			return Map(nil)
		}
		c := make(Map, len(x))
		for k, e := range x {
			c[k] = CloneValue(e)
		}
		return c
	default:
		// Every remaining member is an immutable scalar.
		return v
	}
}
