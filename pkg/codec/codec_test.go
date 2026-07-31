package codec

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

func enc(t *testing.T, e connector.Encoder, r *record.Record) (string, error) {
	t.Helper()
	out, err := e.Encode(context.Background(), nil, r)
	return string(out), err
}

func structured(v record.Value) *record.Record {
	r := &record.Record{}
	r.Payload = record.StructPayload(v)
	return r
}

func rawRec(b string) *record.Record {
	r := &record.Record{}
	r.Payload = record.BytesPayload([]byte(b))
	return r
}

// --- values -------------------------------------------------------------------

func TestJSONValues(t *testing.T) {
	ts := time.Date(2026, 8, 1, 12, 30, 45, 123456789, time.UTC)

	cases := []struct {
		name string
		in   record.Value
		want string
	}{
		{"null", record.Null{}, `null`},
		{"bool", record.Bool(true), `true`},
		{"int", record.Int(-42), `-42`},
		{"uint", record.Uint(42), `42`},
		{"float", record.Float(1.5), `1.5`},
		{"float integral", record.Float(1000000), `1e+06`},
		{"string", record.String("hi"), `"hi"`},
		{"bytes", record.Bytes("hi"), `"aGk="`},
		{"time", record.Time(ts), `"2026-08-01T12:30:45.123456789Z"`},
		{"empty list", record.List{}, `[]`},
		{"list", record.List{record.Int(1), record.String("a")}, `[1,"a"]`},
		{"empty map", record.Map{}, `{}`},
		{"map is sorted", record.Map{"b": record.Int(2), "a": record.Int(1)}, `{"a":1,"b":2}`},
		{"nested", record.Map{"l": record.List{record.Map{"k": record.Null{}}}}, `{"l":[{"k":null}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := enc(t, &jsonEncoder{}, structured(tc.in))
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
			if !json.Valid([]byte(got)) {
				t.Errorf("output is not valid JSON: %s", got)
			}
		})
	}
}

// TestNilValueVersusExplicitNull is the distinction the record model keeps two representations for,
// and the one an encoder is most likely to quietly destroy.
//
// A nil Value means "no value supplied"; record.Null{} means "explicitly null". In a JSON object
// those are an ABSENT key and a null-valued key, and collapsing them would make a partial update
// indistinguishable from a field being cleared.
func TestNilValueVersusExplicitNull(t *testing.T) {
	got, err := enc(t, &jsonEncoder{}, structured(record.Map{
		"supplied":     record.Int(1),
		"explicitNull": record.Null{},
		"notSupplied":  nil,
	}))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	want := `{"explicitNull":null,"supplied":1}`
	if got != want {
		t.Errorf("got %s, want %s\n  a nil Value must be an absent key, not null", got, want)
	}
}

func TestMapOrderIsStable(t *testing.T) {
	m := record.Map{}
	for _, k := range strings.Split("q w e r t y u i o p a s d f", " ") {
		m[k] = record.Int(1)
	}
	first, err := enc(t, &jsonEncoder{}, structured(m))
	if err != nil {
		t.Fatal(err)
	}
	// Go randomises map iteration, so a hundred passes over the same map is a real test of whether
	// the output depends on it. An encoder that is not stable makes every golden file flaky.
	for i := 0; i < 100; i++ {
		again, err := enc(t, &jsonEncoder{}, structured(m))
		if err != nil {
			t.Fatal(err)
		}
		if again != first {
			t.Fatalf("output changed between runs:\n %s\n %s", first, again)
		}
	}
}

// TestDecimalKeepsItsPrecision is why Decimal is a string in the output.
func TestDecimalKeepsItsPrecision(t *testing.T) {
	cases := []struct {
		name     string
		unscaled []byte
		scale    int32
		want     string
	}{
		{"zero", nil, 0, `"0"`},
		{"integer", []byte{0x7B}, 0, `"123"`},                        // 123
		{"two places", []byte{0x30, 0x39}, 2, `"123.45"`},            // 12345 scale 2
		{"leading zero", []byte{0x05}, 3, `"0.005"`},                 // 5 scale 3
		{"negative", []byte{0xFF, 0xFF, 0xCF, 0xC7}, 2, `"-123.45"`}, // -12345 scale 2
		{"negative scale", []byte{0x0C}, -2, `"1200"`},               // 12 × 10^2
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := enc(t, &jsonEncoder{}, structured(record.Decimal{Unscaled: tc.unscaled, Scale: tc.scale}))
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}

	// The point of the string: a value no float64 can hold survives exactly.
	big := record.Decimal{Unscaled: []byte{0x0A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC}, Scale: 4}
	got, err := enc(t, &jsonEncoder{}, structured(big))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "e") || strings.Contains(got, "E") {
		t.Errorf("a large decimal was rendered in exponent form and lost precision: %s", got)
	}
}

func TestNonFiniteFloatIsRefused(t *testing.T) {
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := enc(t, &jsonEncoder{}, structured(record.Float(f)))
		if err == nil {
			t.Errorf("%v was encoded; JSON has no representation for it", f)
		}
	}
}

// TestStringEscaping checks the cases that produce invalid JSON or unsafe JavaScript.
func TestStringEscaping(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"quote", `a"b`, `"a\"b"`},
		{"backslash", `a\b`, `"a\\b"`},
		{"newline", "a\nb", `"a\nb"`},
		{"tab", "a\tb", `"a\tb"`},
		{"control", "a\x00b", `"a\u0000b"`},
		{"del is not escaped", "a\x7fb", "\"a\x7fb\""},
		{"line separator", "a\u2028b", `"a\u2028b"`},
		{"paragraph separator", "a\u2029b", `"a\u2029b"`},
		{"unicode passes through", "héllo → 日本", `"héllo → 日本"`},
		{"space is not escaped", "a b", `"a b"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := enc(t, &jsonEncoder{}, structured(record.String(tc.in)))
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
			if !json.Valid([]byte(got)) {
				t.Errorf("not valid JSON: %s", got)
			}
		})
	}
}

func TestInvalidUTF8BecomesReplacement(t *testing.T) {
	got, err := enc(t, &jsonEncoder{}, structured(record.String("a\xffb")))
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid([]byte(got)) {
		t.Fatalf("invalid UTF-8 produced invalid JSON: %q", got)
	}
	if !strings.Contains(got, "\uFFFD") {
		t.Errorf("got %q, want the replacement character", got)
	}
}

// TestAgainstEncodingJSON cross-checks the hand-written writer against the standard library, so a
// hand-rolled encoder is not also a hand-rolled interpretation of JSON.
//
// The reference is an encoder with SetEscapeHTML(false), not json.Marshal. Marshal escapes <, > and
// & by default because its original audience was HTML templates, and comparing against it would
// either fail here or push this encoder into mangling every URL and ampersand it ever carries. See
// TestHTMLIsNotEscaped for that decision stated on its own.
func TestAgainstEncodingJSON(t *testing.T) {
	reference := func(s string) string {
		var b strings.Builder
		e := json.NewEncoder(&b)
		e.SetEscapeHTML(false)
		if err := e.Encode(s); err != nil {
			t.Fatal(err)
		}
		return strings.TrimSuffix(b.String(), "\n") // Encode appends one
	}

	for _, s := range []string{
		"", "plain", `with "quotes"`, "with\nnewline", "tab\there", "\x00\x01\x1f",
		"héllo", "日本語", "emoji 🎉", "a\u2028b", "b\u2029c", "back\\slash",
		"a<b>c", "x&y", "</script>", "\x7f", "control\x0b\x0c",
	} {
		want := reference(s)
		got := string(appendString(nil, s))
		if got != want {
			t.Errorf("string %q:\n got %s\nwant %s", s, got, want)
		}
	}
}

// TestHTMLIsNotEscaped states the one deliberate divergence from json.Marshal's default.
//
// Marshal turns < > & into \u003c \u003e \u0026 so its output is safe to drop inside a <script>
// tag. canal writes data files and pipes, where that turns every URL query string and every
// ampersand into noise a downstream reader has to undo. The stdlib offers SetEscapeHTML(false) for
// exactly this audience, and this encoder behaves as though it is always set.
//
// The escaping that IS security-relevant here — U+2028 and U+2029, which are legal JSON and illegal
// JavaScript — is still applied. See TestStringEscaping.
func TestHTMLIsNotEscaped(t *testing.T) {
	got, err := enc(t, &jsonEncoder{}, structured(record.String("https://x.test/?a=1&b=2<3")))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, `\u0026`) || strings.Contains(got, `\u003c`) {
		t.Errorf("HTML escaping leaked into a data pipeline: %s", got)
	}
	if got != `"https://x.test/?a=1&b=2<3"` {
		t.Errorf("got %s", got)
	}
	if !json.Valid([]byte(got)) {
		t.Errorf("not valid JSON: %s", got)
	}
}

// --- encoders and framer --------------------------------------------------------

func TestRawPassesBytesThrough(t *testing.T) {
	got, err := enc(t, &rawEncoder{}, rawRec("a line, unmodified"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "a line, unmodified" {
		t.Errorf("got %q", got)
	}
}

// TestEncodersRefuseTheWrongPayload: each says which encoder the record actually needs, rather than
// emitting something plausible and wrong.
func TestEncodersRefuseTheWrongPayload(t *testing.T) {
	if _, err := enc(t, &rawEncoder{}, structured(record.Int(1))); err == nil {
		t.Error("the raw encoder accepted a structured-only payload")
	} else if !strings.Contains(err.Error(), "json encoder") {
		t.Errorf("the error does not say which encoder to use: %v", err)
	}

	if _, err := enc(t, &jsonEncoder{}, rawRec("not json")); err == nil {
		t.Error("the json encoder accepted a bytes-only payload and would have emitted invalid JSON")
	} else if !strings.Contains(err.Error(), "raw encoder") {
		t.Errorf("the error does not say which encoder to use: %v", err)
	}
}

// TestRefusalsAreContractFaults: a mispaired codec is a configuration error that retrying cannot
// fix, so it must never be classified as transient.
func TestRefusalsAreContractFaults(t *testing.T) {
	_, err := enc(t, &jsonEncoder{}, rawRec("x"))
	var f *fault.Fault
	if !errors.As(err, &f) {
		t.Fatalf("not a fault.Fault: %v", err)
	}
	if !f.Class.Terminal() {
		t.Errorf("class is %s, which is retriable; a mispaired codec never becomes correct by retrying", f.Class)
	}
}

func TestEncodeAppendsToTheCallersBuffer(t *testing.T) {
	// The engine reuses one buffer per node, so Encode must extend rather than allocate afresh.
	dst := []byte("PREFIX:")
	out, err := (&jsonEncoder{}).Encode(context.Background(), dst, structured(record.Int(7)))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "PREFIX:7" {
		t.Errorf("got %q, want %q", out, "PREFIX:7")
	}
}

func TestNewlineFramerTerminates(t *testing.T) {
	f := &newlineFramer{}
	out, err := f.Frame(nil, []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	out, err = f.Frame(out, []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	// Terminated, not separated: the last record has a delimiter too, so a reader can tell a
	// complete final record from a truncated one.
	if string(out) != "one\ntwo\n" {
		t.Errorf("got %q, want %q", out, "one\ntwo\n")
	}
	if f.Terminator() != nil {
		t.Error("the newline framer declared a per-request terminator; its delimiter is per-payload")
	}
}

// TestNdjson is the composition that matters: json + newline is ndjson, and every line parses.
func TestNdjson(t *testing.T) {
	e, f := &jsonEncoder{}, &newlineFramer{}
	var out []byte
	for i := 0; i < 3; i++ {
		payload, err := e.Encode(context.Background(), nil, structured(record.Map{
			"i": record.Int(int64(i)), "name": record.String("row"),
		}))
		if err != nil {
			t.Fatal(err)
		}
		if out, err = f.Frame(out, payload); err != nil {
			t.Fatal(err)
		}
	}

	lines := strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines from 3 records: %q", len(lines), out)
	}
	for i, ln := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			t.Errorf("line %d is not valid JSON: %v", i, err)
		}
		if m["i"] != float64(i) {
			t.Errorf("line %d has i=%v", i, m["i"])
		}
	}
}

// --- registration ----------------------------------------------------------------

// TestRegistrationIsAccepted proves the three components satisfy the registry's init-time lints,
// which are strict enough that passing them is most of the contract.
func TestRegistrationIsAccepted(t *testing.T) {
	r := registry.New()
	Register(r) // panics on any lint failure

	for _, name := range []string{"raw", "json"} {
		if _, ok := r.Encoder(name); !ok {
			t.Errorf("encoder %q did not register", name)
		}
	}
	if _, ok := r.Framer("newline"); !ok {
		t.Error("framer \"newline\" did not register")
	}
	for _, d := range r.Descriptors() {
		if len(d.Warnings) != 0 {
			t.Errorf("%s/%s registered with warnings: %v", d.Kind, d.Name, d.Warnings)
		}
	}
}

func TestContentTypes(t *testing.T) {
	if got := (&jsonEncoder{}).ContentType(); got != "application/json" {
		t.Errorf("json ContentType is %q", got)
	}
	if got := (&rawEncoder{}).ContentType(); got != "application/octet-stream" {
		t.Errorf("raw ContentType is %q", got)
	}
}

// TestEveryControlCharacterMatchesTheStdlib sweeps 0x00-0x20 rather than sampling.
//
// Sampling is what let \b and \f ship wrong in the first version of appendString: both are RFC 8259
// short escapes, both were emitted as \u00XX, and the hand-picked cases in the cross-check happened
// to miss them. A sweep cannot miss.
func TestEveryControlCharacterMatchesTheStdlib(t *testing.T) {
	for c := 0; c <= 0x20; c++ {
		var b strings.Builder
		e := json.NewEncoder(&b)
		e.SetEscapeHTML(false)
		if err := e.Encode(string(rune(c))); err != nil {
			t.Fatal(err)
		}
		want := strings.TrimSuffix(b.String(), "\n")
		got := string(appendString(nil, string(rune(c))))
		if got != want {
			t.Errorf("0x%02x: got %s, want %s", c, got, want)
		}
	}
}
