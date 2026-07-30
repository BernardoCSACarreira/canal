package record

import (
	"fmt"
	"iter"
)

// Reserved metadata namespaces. Namespaces are reserved WITH A CHECK, not by
// convention: one surveyed system shipped six source-shaped keys into a
// connector-agnostic namespace precisely because nothing enforced it.
const (
	// NSCanal is core-owned and read-only to connectors. Meta.Set returns an error
	// for it.
	NSCanal = "canal"
	// NSSource is the source connector's namespace.
	NSSource = "source"
	// NSSink is the sink connector's namespace.
	NSSink = "sink"
	// NSUser is the operator's namespace, set by transforms and config.
	NSUser = "user"
)

type metaEntry struct {
	ns, key string
	val     Value
}

type secretEntry struct {
	key, val string
}

// Meta is a namespaced metadata sidecar, addressable separately from the payload
// so a transform touching metadata never rewrites the body and a codec encoding
// the body never has to decide what to do with metadata.
//
// It is backed by a SLICE of triples, not a map. Two reasons: a typical record
// carries zero to five entries, so a slice is smaller and faster to deep-copy; and
// a map inside a value type that gets copied on fan-out is shared mutable state
// that produces a concurrent map write — an unrecoverable process-wide fatal
// error — on the first fan-out pipeline. Meta is deep-copied by [Meta.Clone] and
// by every derivation.
//
// Secrets live in a fifth, unlisted compartment that is never serialised, never
// logged, never exported to the read model, and never visible to a codec.
//
// Meta is not safe for concurrent use. A record is owned by exactly one node at a
// time.
type Meta struct {
	kv      []metaEntry
	secrets []secretEntry
	changes []FieldChange
}

func validNS(ns string) bool {
	switch ns {
	case NSCanal, NSSource, NSSink, NSUser:
		return true
	}
	return false
}

// Get returns the value stored under (ns, key).
func (m *Meta) Get(ns, key string) (Value, bool) {
	for i := range m.kv {
		if m.kv[i].ns == ns && m.kv[i].key == key {
			return m.kv[i].val, true
		}
	}
	return nil, false
}

// Set stores a value. It returns an error for ns == [NSCanal] and for an unknown
// namespace.
//
// Note that setting the empty string stores the empty string: canal does NOT adopt
// the "empty string deletes the key" convention, because "present but empty" must
// be representable.
func (m *Meta) Set(ns, key string, v Value) error {
	if ns == NSCanal {
		return fmt.Errorf("record: metadata namespace %q is core-owned and read-only", ns)
	}
	if !validNS(ns) {
		return fmt.Errorf("record: unknown metadata namespace %q", ns)
	}
	m.set(ns, key, v)
	return nil
}

// set is the unchecked writer the core uses for the canal namespace.
func (m *Meta) set(ns, key string, v Value) {
	for i := range m.kv {
		if m.kv[i].ns == ns && m.kv[i].key == key {
			m.kv[i].val = v
			return
		}
	}
	m.kv = append(m.kv, metaEntry{ns: ns, key: key, val: v})
}

// Delete removes (ns, key) if present. Deleting an absent key is not an error.
func (m *Meta) Delete(ns, key string) {
	for i := range m.kv {
		if m.kv[i].ns == ns && m.kv[i].key == key {
			m.kv = append(m.kv[:i], m.kv[i+1:]...)
			return
		}
	}
}

// All iterates every key in ns, in insertion order.
func (m *Meta) All(ns string) iter.Seq2[string, Value] {
	return func(yield func(string, Value) bool) {
		for i := range m.kv {
			if m.kv[i].ns != ns {
				continue
			}
			if !yield(m.kv[i].key, m.kv[i].val) {
				return
			}
		}
	}
}

// Len reports how many non-secret entries are present, across every namespace.
func (m *Meta) Len() int { return len(m.kv) }

// Clone returns a Meta sharing nothing with m.
func (m *Meta) Clone() Meta {
	c := Meta{}
	if m.kv != nil {
		c.kv = make([]metaEntry, len(m.kv))
		for i := range m.kv {
			c.kv[i] = metaEntry{ns: m.kv[i].ns, key: m.kv[i].key, val: CloneValue(m.kv[i].val)}
		}
	}
	if m.secrets != nil {
		c.secrets = make([]secretEntry, len(m.secrets))
		copy(c.secrets, m.secrets)
	}
	if m.changes != nil {
		c.changes = make([]FieldChange, len(m.changes))
		copy(c.changes, m.changes)
	}
	return c
}

// SetSecret stores a value the core guarantees will not appear in any serialised
// form, log line, metric label or API response.
func (m *Meta) SetSecret(key, v string) {
	for i := range m.secrets {
		if m.secrets[i].key == key {
			m.secrets[i].val = v
			return
		}
	}
	m.secrets = append(m.secrets, secretEntry{key: key, val: v})
}

// Secret reads back a value stored with [Meta.SetSecret].
func (m *Meta) Secret(key string) (string, bool) {
	for i := range m.secrets {
		if m.secrets[i].key == key {
			return m.secrets[i].val, true
		}
	}
	return "", false
}

// NoteChange records that a field could not be carried faithfully. This turns a
// silent lossy conversion into a countable, per-field, testable fact, and it is the
// mechanism that makes "the sink rounded your decimals" visible instead of
// discovered in a reconciliation six months later.
func (m *Meta) NoteChange(fc FieldChange) { m.changes = append(m.changes, fc) }

// Changes returns the recorded fidelity losses, in the order they were noted. The
// returned slice must not be modified.
func (m *Meta) Changes() []FieldChange { return m.changes }

// FieldChange is one recorded fidelity loss.
type FieldChange struct {
	Path   string          `json:"path"`
	Kind   FieldChangeKind `json:"kind"`
	Detail string          `json:"detail,omitempty"`
}

// FieldChangeKind is the closed set of ways a field can fail to be carried
// faithfully. It is a metric label.
type FieldChangeKind uint8

const (
	// FieldChangeUnknown is the zero value and is always a defect.
	FieldChangeUnknown FieldChangeKind = iota
	FieldNulled
	FieldTruncated
	FieldRounded
	FieldRedacted
	FieldUnavailable
)

var fieldChangeNames = [...]string{
	FieldChangeUnknown: "unknown",
	FieldNulled:        "nulled",
	FieldTruncated:     "truncated",
	FieldRounded:       "rounded",
	FieldRedacted:      "redacted",
	FieldUnavailable:   "unavailable",
}

// String returns the stable snake_case token for k.
func (k FieldChangeKind) String() string {
	if int(k) < len(fieldChangeNames) {
		return fieldChangeNames[k]
	}
	return "unknown"
}
