// R8 says conformance is asserted against real responses, not against the schema and not by reading
// the struct tags. This file walks the real types and marshals the real documents.
//
// The defect it pins is the one design-rules.md R8 describes verbatim: a nil slice marshalled
// straight to `null` against a field a consumer reads as an array, with the Go-side test
// (`len(x) != 0`) passing for the nil that produced it. The compliance audit found it again here, in
// six fields of PipelineStatus and two of Negotiated.
package telemetry

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// documents are every type the frontend reads. Adding one here is the whole cost of keeping this
// guarantee as the read model grows.
func documents() []any {
	return []any{
		PipelineStatus{}, Negotiated{}, NodeStatus{}, LaneStatus{},
		BufferStatus{}, WorkerStatus{}, Throughput{}, ScanProgress{},
		Condition{}, Event{}, FaultInfo{}, DefaultNote{}, Downgrade{}, NodeContract{},
	}
}

// TestNoCollectionCanMarshalToNull is the structural half: every SLICE or MAP field with a JSON tag
// must carry omitempty, so nil cannot reach the wire as null.
//
// Pointers are deliberately exempt. readmodel.go's nil-pointer rule makes a nil pointer mean "not
// known", which is a real and distinct state from zero — a rate of nil is not a rate of 0.0, and
// collapsing them is how a UI ends up drawing a confident 0 for a pipeline it has heard nothing
// from. Collections have no such story: absent and empty mean the same thing, and null is a third
// state every consumer would have to special-case.
func TestNoCollectionCanMarshalToNull(t *testing.T) {
	for _, doc := range documents() {
		rt := reflect.TypeOf(doc)
		t.Run(rt.Name(), func(t *testing.T) {
			for i := 0; i < rt.NumField(); i++ {
				f := rt.Field(i)
				tag := f.Tag.Get("json")
				if tag == "" || tag == "-" {
					continue
				}
				switch f.Type.Kind() {
				case reflect.Slice, reflect.Map:
				default:
					continue
				}
				// []byte marshals to a base64 string, not an array, so it is not a collection here.
				if f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Uint8 {
					continue
				}
				if !strings.Contains(tag, ",omitempty") {
					t.Errorf("%s.%s is a %s with json tag %q and no omitempty, so a nil marshals to null\n"+
						"  R8 names this exact defect; add omitempty or populate the field at construction",
						rt.Name(), f.Name, f.Type.Kind(), tag)
				}
			}
		})
	}
}

// TestZeroDocumentsEmitNoNullCollections is the behavioural half: marshal the zero value — what a
// pipeline looks like before it has produced anything, which is when an operator is most likely to
// be watching — and confirm no array or object field came out as null.
func TestZeroDocumentsEmitNoNullCollections(t *testing.T) {
	for _, doc := range documents() {
		rt := reflect.TypeOf(doc)
		t.Run(rt.Name(), func(t *testing.T) {
			b, err := json.Marshal(doc)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(b, &raw); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			for i := 0; i < rt.NumField(); i++ {
				f := rt.Field(i)
				switch f.Type.Kind() {
				case reflect.Slice, reflect.Map:
				default:
					continue
				}
				if f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Uint8 {
					continue
				}
				name := strings.Split(f.Tag.Get("json"), ",")[0]
				if v, present := raw[name]; present && string(v) == "null" {
					t.Errorf("%s.%s marshalled to null in a zero document: %s", rt.Name(), f.Name, b)
				}
			}
		})
	}
}

// TestPopulatedCollectionsStillAppear is the guard on the fix itself. omitempty removes the field
// when empty, which is only correct if a populated one still marshals — a change that silently
// dropped real data would be worse than the null it replaced.
func TestPopulatedCollectionsStillAppear(t *testing.T) {
	s := PipelineStatus{
		Missing:      []string{"worker-3"},
		Conditions:   []Condition{{}},
		Nodes:        []NodeStatus{{}},
		Lanes:        []LaneStatus{{}},
		Buffers:      []BufferStatus{{}},
		Workers:      []WorkerStatus{{}},
		RecentEvents: []Event{{}},
		Negotiated:   Negotiated{Why: []string{"because"}, Defaults: []DefaultNote{{}}},
	}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{
		`"missing":`, `"conditions":`, `"nodes":`, `"lanes":`,
		`"buffers":`, `"workers":`, `"recentEvents":`, `"why":`, `"defaults":`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("populated collection %s vanished from the document: %s", want, b)
		}
	}
}
