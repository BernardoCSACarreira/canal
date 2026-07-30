package schema

import (
	"crypto/sha256"
	"strconv"
	"strings"
)

// Ref names a schema by content fingerprint plus an epoch that orders schema
// changes within a stream. The body lives in the pipeline's schema table,
// deduplicated, so a record carries 32 bytes rather than a schema.
//
// A schema carried on every record was Kafka Connect's choice and it is a real
// per-record cost; a schema carried on every lane was Flink CDC's, and it needed
// a schemaless snapshot-split variant plus a rehydration step because thousands
// of chunk splits blew up the checkpoint. A content-addressed reference plus one
// table is both systems' fix, applied first.
type Ref struct {
	Fingerprint [16]byte `json:"fp"`
	Epoch       uint64   `json:"epoch"`
	Stream      string   `json:"stream"`
}

// IsZero reports whether r names nothing.
func (r Ref) IsZero() bool {
	return r.Epoch == 0 && r.Stream == "" && r.Fingerprint == [16]byte{}
}

// Fingerprint is the SHA-256 of the normalised structural encoding, truncated to
// 16 bytes.
//
// Normalisation sorts nothing — field order is semantic — but canonicalises
// whitespace, optional fields and logical defaults, so two schemas that differ
// only in an omitted-versus-zero logical parameter fingerprint identically.
//
// A nil schema fingerprints to the zero value, so an absent schema and an empty
// schema are distinguishable.
func Fingerprint(s *Schema) [16]byte {
	var out [16]byte
	if s == nil {
		return out
	}
	var b strings.Builder
	normalise(&b, s)
	sum := sha256.Sum256([]byte(b.String()))
	copy(out[:], sum[:16])
	return out
}

func normalise(b *strings.Builder, s *Schema) {
	b.WriteString("s{")
	if s.Open {
		b.WriteString("open;")
	}
	for _, k := range s.Keys {
		b.WriteString("k:")
		b.WriteString(strings.Join(k, "."))
		b.WriteByte(';')
	}
	for i := range s.Fields {
		normaliseField(b, &s.Fields[i])
	}
	b.WriteByte('}')
}

func normaliseField(b *strings.Builder, f *Field) {
	b.WriteString("f{")
	b.WriteString(f.Name)
	b.WriteByte(':')
	b.WriteString(f.Type.String())
	if f.Nullable {
		b.WriteString(";null")
	}
	if l := f.Logical; l != nil {
		b.WriteString(";l")
		if l.UnknownPrecision {
			b.WriteString(":p?")
		} else if l.Precision != 0 || l.Scale != 0 {
			b.WriteString(":p")
			b.WriteString(strconv.Itoa(l.Precision))
			b.WriteByte(',')
			b.WriteString(strconv.Itoa(l.Scale))
		}
		if l.TimeUnit != TimeUnitUnknown {
			b.WriteString(":u")
			b.WriteString(l.TimeUnit.String())
		}
		if l.TimeZone != "" {
			b.WriteString(":z")
			b.WriteString(l.TimeZone)
		}
		if l.Name != "" {
			b.WriteString(":n")
			b.WriteString(l.Name)
		}
	}
	for i := range f.Fields {
		normaliseField(b, &f.Fields[i])
	}
	if f.Item != nil {
		b.WriteString(";i")
		normaliseField(b, f.Item)
	}
	b.WriteByte('}')
}
