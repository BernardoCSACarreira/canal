package connector

import (
	"encoding"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// TestNoEnumSliceMarshalsAsBase64 is the audit's finding stated against the real bytes.
//
// A []T over a uint8 base type is a []byte to encoding/json, so it was emitted as base64:
// `"lane_kinds":"AQ=="` shipped in both example connectors' registration descriptors. The fix is
// that the element implements encoding.TextMarshaler, which is exactly the condition under which
// encoding/json declines to take the byte-slice path.
func TestNoEnumSliceMarshalsAsBase64(t *testing.T) {
	caps := SourceCaps{
		Caps:              Caps{APIVersion: APIVersion},
		LaneKinds:         []LaneKind{LaneKindScan, LaneKindStream},
		Boundedness:       []Boundedness{Bounded, Unbounded},
		DefaultOrdering:   OrderingPrefix,
		UpstreamRetention: RetentionUnbounded,
	}
	b, err := json.Marshal(caps)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(b)

	for _, want := range []string{
		`"lane_kinds":["scan","stream"]`,
		`"boundedness":["bounded","unbounded"]`,
		`"default_ordering":"prefix"`,
		`"upstream_retention":"unbounded"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in\n%s", want, got)
		}
	}
	if strings.Contains(got, `"lane_kinds":"`) {
		t.Errorf(`lane_kinds marshalled as a string, which is the base64 defect: %s`, got)
	}

	var back SourceCaps
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("caps cannot be decoded from their own encoding: %v", err)
	}
	if !reflect.DeepEqual(caps, back) {
		t.Errorf("round-trip changed the value:\n have %+v\n want %+v", back, caps)
	}
}

// TestEveryEnumRoundTrips walks every token of every closed enum in this package.
//
// The table is written out rather than derived, so adding an enum without adding it here is a
// visible omission rather than silent coverage loss.
func TestEveryEnumRoundTrips(t *testing.T) {
	cases := []struct {
		name string
		vals []encoding.TextMarshaler
		make func() encoding.TextUnmarshaler
	}{
		{"Durability", []encoding.TextMarshaler{DurabilityNone, DurabilityProcess, DurabilityNode, DurabilityCluster},
			func() encoding.TextUnmarshaler { return new(Durability) }},
		{"WhenFull", []encoding.TextMarshaler{WhenFullBlock, WhenFullReject},
			func() encoding.TextUnmarshaler { return new(WhenFull) }},
		{"Guarantee", []encoding.TextMarshaler{AtMostOnce, AtLeastOnce, EffectivelyOnce, ExactlyOnce},
			func() encoding.TextUnmarshaler { return new(Guarantee) }},
		{"Ordering", []encoding.TextMarshaler{OrderingPrefix, OrderingDiscrete},
			func() encoding.TextUnmarshaler { return new(Ordering) }},
		{"Boundedness", []encoding.TextMarshaler{Bounded, Unbounded},
			func() encoding.TextUnmarshaler { return new(Boundedness) }},
		{"LaneKind", []encoding.TextMarshaler{LaneKindStream, LaneKindScan, LaneKindBackfill},
			func() encoding.TextUnmarshaler { return new(LaneKind) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seen := map[string]bool{}
			for _, v := range tc.vals {
				text, err := v.MarshalText()
				if err != nil {
					t.Fatalf("MarshalText: %v", err)
				}
				tok := string(text)
				if tok == "" {
					t.Errorf("%v marshalled to the empty token", v)
				}
				if seen[tok] {
					t.Errorf("token %q is produced by two distinct values; the wire form is ambiguous", tok)
				}
				seen[tok] = true

				back := tc.make()
				if err := back.UnmarshalText(text); err != nil {
					t.Errorf("token %q does not parse back: %v", tok, err)
					continue
				}
				got := reflect.ValueOf(back).Elem().Interface()
				if got != v {
					t.Errorf("round-trip of %q gave %v, want %v", tok, got, v)
				}
			}
		})
	}
}

// TestUnknownTokenIsAnError: silently reading an unrecognised token as the zero value is how a
// config typo becomes a delivery tier nobody chose.
func TestUnknownTokenIsAnError(t *testing.T) {
	var g Guarantee
	if err := g.UnmarshalText([]byte("exactly_twice")); err == nil {
		t.Fatal("an unknown guarantee token was accepted; it would silently mean at_most_once")
	}
	var k LaneKind
	if err := k.UnmarshalText([]byte("")); err == nil {
		t.Fatal("the empty token was accepted as a LaneKind")
	}
}
