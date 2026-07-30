package schema

// Type is the canonical type set: the lossless intersection of the formats canal
// intends to support. It is closed, it is a metric label, and it is versioned by
// [Ref].
type Type uint8

const (
	// TypeUnknown is the zero value and is always a defect in a discovered
	// schema. A source that genuinely cannot report a type uses [TypeAny].
	TypeUnknown Type = iota
	TypeBool
	TypeInt64
	TypeUint64
	TypeFloat64
	TypeString
	TypeBytes
	TypeTimestamp
	TypeDate
	TypeTime
	TypeDecimal
	TypeStruct
	TypeList
	TypeMap

	// TypeAny is the explicit escape hatch for a source that genuinely cannot
	// report a type. It is LABELLED as such and a sink may refuse it, rather
	// than being silently coerced to string.
	TypeAny
)

// typeNames are the stable snake_case wire tokens. They are simultaneously the
// wire form, the metric label value and the i18n key suffix (design rule R9):
// there is no second vocabulary and no display map.
var typeNames = [...]string{
	TypeUnknown:   "unknown",
	TypeBool:      "bool",
	TypeInt64:     "int64",
	TypeUint64:    "uint64",
	TypeFloat64:   "float64",
	TypeString:    "string",
	TypeBytes:     "bytes",
	TypeTimestamp: "timestamp",
	TypeDate:      "date",
	TypeTime:      "time",
	TypeDecimal:   "decimal",
	TypeStruct:    "struct",
	TypeList:      "list",
	TypeMap:       "map",
	TypeAny:       "any",
}

// String returns the stable snake_case token for t.
func (t Type) String() string {
	if int(t) < len(typeNames) {
		return typeNames[t]
	}
	return "unknown"
}

// Logical carries parameterised type detail in a separate struct, so adding a
// parameterised type does not widen [Type].
type Logical struct {
	// Precision and Scale describe a [TypeDecimal].
	Precision int `json:"precision,omitempty"`
	Scale     int `json:"scale,omitempty"`

	// UnknownPrecision is the explicit escape hatch for a source that cannot
	// report precision — an arbitrary-precision decimal — rather than a silent
	// zero. Every unknown is typed as unknown.
	UnknownPrecision bool `json:"unknown_precision,omitempty"`

	// TimeUnit is the resolution of a [TypeTimestamp] or [TypeTime].
	TimeUnit TimeUnit `json:"time_unit,omitempty"`

	// TimeZone is an IANA location name when the source knows one. Empty means
	// unknown, never UTC by assumption.
	TimeZone string `json:"time_zone,omitempty"`

	// Name is a named logical type the canonical set does not model
	// structurally: "uuid", "json", "geography".
	Name string `json:"name,omitempty"`
}

// TimeUnit is the resolution of a temporal field. Closed set; a metric label.
type TimeUnit uint8

const (
	// TimeUnitUnknown is the zero value and means the source did not say.
	TimeUnitUnknown TimeUnit = iota
	TimeUnitSecond
	TimeUnitMilli
	TimeUnitMicro
	TimeUnitNano
)

var timeUnitNames = [...]string{
	TimeUnitUnknown: "unknown",
	TimeUnitSecond:  "second",
	TimeUnitMilli:   "milli",
	TimeUnitMicro:   "micro",
	TimeUnitNano:    "nano",
}

// String returns the stable snake_case token for u.
func (u TimeUnit) String() string {
	if int(u) < len(timeUnitNames) {
		return timeUnitNames[u]
	}
	return "unknown"
}
