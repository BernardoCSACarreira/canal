package codec

import (
	"encoding/base64"
	"errors"
	"math"
	"math/big"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

var (
	errNoBytes = fault.Contract(fault.OpEncode, errors.New(
		"codec: the raw encoder was given a record with no byte payload; a structured record needs the json encoder"))
	errNoStructure = fault.Contract(fault.OpEncode, errors.New(
		"codec: the json encoder was given a record with no structured payload; a byte record needs the raw encoder"))
)

// appendValue writes v to dst as JSON.
//
// Written by hand rather than through encoding/json for two reasons. record.Value is a closed
// interface whose members are not what encoding/json would produce — Map is map[string]Value, so
// reflection would recurse into interface boxes — and the two lossy kinds need a deliberate
// decision rather than a default. Appending also keeps the engine's one-buffer-per-node reuse
// intact, which a json.Marshal per record would not.
func appendValue(dst []byte, v record.Value) ([]byte, error) {
	// A nil Value means "no value supplied", which is DISTINCT from record.Null{} meaning
	// "explicitly null". At the top level of a payload there is nowhere to put that distinction, so
	// it collapses to null; inside a map it does not, and appendMap omits the key instead.
	if v == nil {
		return append(dst, "null"...), nil
	}

	switch x := v.(type) {
	case record.Null:
		return append(dst, "null"...), nil
	case record.Bool:
		return strconv.AppendBool(dst, bool(x)), nil
	case record.Int:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case record.Uint:
		return strconv.AppendUint(dst, uint64(x), 10), nil

	case record.Float:
		f := float64(x)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			// JSON has no NaN or Infinity. Emitting a bare NaN token produces a document most
			// parsers reject, and emitting null would silently turn a measurement into a missing
			// value. Refusing names the problem where it happened.
			return dst, fault.Contract(fault.OpEncode, errors.New(
				"codec: JSON cannot represent NaN or Infinity; the source produced a non-finite float"))
		}
		// 'g' with -1 precision round-trips a float64 exactly and avoids 1e+06 style output for
		// integral values where JSON readers expect 1000000.
		return strconv.AppendFloat(dst, f, 'g', -1, 64), nil

	case record.String:
		return appendString(dst, string(x)), nil

	case record.Bytes:
		// Base64, which is lossy in the sense that a reader cannot tell it from a string that
		// happens to be base64. CodecCaps.Produces says so.
		dst = append(dst, '"')
		dst = base64.StdEncoding.AppendEncode(dst, []byte(x))
		return append(dst, '"'), nil

	case record.Time:
		dst = append(dst, '"')
		dst = time.Time(x).UTC().AppendFormat(dst, time.RFC3339Nano)
		return append(dst, '"'), nil

	case record.Decimal:
		// A STRING, deliberately. A JSON number is a float64 to almost every reader, so emitting
		// 12345678901234567890.12 as a number silently rounds it — which defeats the entire reason
		// record.Decimal exists rather than a float.
		return appendString(dst, decimalString(x)), nil

	case record.List:
		dst = append(dst, '[')
		for i, e := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			if dst, err = appendValue(dst, e); err != nil {
				return dst, err
			}
		}
		return append(dst, ']'), nil

	case record.Map:
		return appendMap(dst, x)

	default:
		// record.Value is a closed set with an unexported marker method, so this is unreachable
		// from outside the module. It is here so that adding a member without updating this
		// function fails loudly rather than emitting nothing.
		return dst, fault.Contract(fault.OpEncode, errors.New(
			"codec: unknown record.Value member "+v.Kind().String()))
	}
}

func appendMap(dst []byte, m record.Map) ([]byte, error) {
	// Sorted, because Map's own doc says key order is not significant — and an encoder whose output
	// depends on Go's map iteration order makes every golden-file test flaky and every byte-identical
	// re-encode impossible.
	keys := make([]string, 0, len(m))
	for k, v := range m {
		// A nil Value means the field was not supplied, which in JSON is an ABSENT key. Writing
		// null here would erase the distinction between "not supplied" and record.Null{}
		// "explicitly null" — the distinction the record model keeps two representations for.
		if v == nil {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	dst = append(dst, '{')
	for i, k := range keys {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendString(dst, k)
		dst = append(dst, ':')
		var err error
		if dst, err = appendValue(dst, m[k]); err != nil {
			return dst, err
		}
	}
	return append(dst, '}'), nil
}

// decimalString renders an arbitrary-precision decimal without going through a float.
func decimalString(d record.Decimal) string {
	// Unscaled is two's-complement big-endian, which is what big.Int.SetBytes plus a sign fix-up
	// reads. An empty slice is zero.
	n := new(big.Int)
	if len(d.Unscaled) > 0 {
		n.SetBytes(d.Unscaled)
		if d.Unscaled[0]&0x80 != 0 {
			// Negative: subtract 2^(8*len) to recover the signed value.
			shift := new(big.Int).Lsh(big.NewInt(1), uint(8*len(d.Unscaled)))
			n.Sub(n, shift)
		}
	}
	if d.Scale == 0 {
		return n.String()
	}
	if d.Scale < 0 {
		// A negative scale multiplies: unscaled × 10^-scale.
		mul := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-d.Scale)), nil)
		return new(big.Int).Mul(n, mul).String()
	}

	neg := n.Sign() < 0
	digits := new(big.Int).Abs(n).String()
	scale := int(d.Scale)
	if len(digits) <= scale {
		digits = leftPad(digits, scale+1)
	}
	point := len(digits) - scale
	out := digits[:point] + "." + digits[point:]
	if neg {
		out = "-" + out
	}
	return out
}

func leftPad(s string, n int) string {
	if len(s) >= n {
		return s
	}
	b := make([]byte, n-len(s), n)
	for i := range b {
		b[i] = '0'
	}
	return string(append(b, s...))
}

const hexDigits = "0123456789abcdef"

// appendString writes a JSON string literal.
//
// It escapes what RFC 8259 requires plus U+2028 and U+2029, which are legal in JSON and illegal in
// JavaScript source — the reason encoding/json escapes them too, and a real hazard for any output
// that reaches a browser.
func appendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			// RFC 8259 defines exactly seven short escapes: " \ / b f n r t. All of them except
			// the optional / are used here, because the alternative \u00XX form is longer and
			// because encoding/json emits the short form — and a cross-check against the standard
			// library is only worth having if this side does not quietly differ from it. \b and \f
			// were missing from a first version of this switch and the cross-check caught them.
			switch c {
			case '"':
				dst = append(dst, '\\', '"')
			case '\\':
				dst = append(dst, '\\', '\\')
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
			}
			i++
			start = i
			continue
		}

		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			// Invalid UTF-8. Emitting the raw byte would produce a document that is not valid JSON
			// at all, so it becomes U+FFFD — the same choice encoding/json makes.
			dst = append(dst, s[start:i]...)
			dst = utf8.AppendRune(dst, utf8.RuneError)
			i += size
			start = i
			continue
		}
		if r == '\u2028' || r == '\u2029' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[r&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
