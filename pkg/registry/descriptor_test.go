package registry

import (
	"encoding/json"
	"strings"
	"testing"

	"context"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// probeSource is the smallest thing that satisfies connector.Source. It exists only so the registry
// has something real to build a Descriptor from.
type probeSource struct{}

func (*probeSource) Open(context.Context, connector.SourceRuntime) error { return nil }
func (*probeSource) Read(context.Context, *record.Batch) error           { return nil }
func (*probeSource) Commit(context.Context, connector.Ack) error         { return nil }
func (*probeSource) Close(context.Context) error                         { return nil }

// TestDescriptorRoundTrips pins the audit's fatal finding: a Descriptor could be written and not
// read back.
//
// Support and CapSource each had a MarshalText and no UnmarshalText, so decoding a descriptor the
// registry itself had produced failed with
//
//	json: cannot unmarshal string into Go struct field Descriptor.support of type registry.Support
//
// The Descriptor is the document a config-driven UI is built from, so "cannot be decoded by a Go
// client" is not a cosmetic defect.
func TestDescriptorRoundTrips(t *testing.T) {
	r := New()
	AddSource(r, SourceDef[*probeSource]{
		Meta: Meta{
			Name: "probe", Version: "1.0.0", Title: "Probe",
			Summary: "a fixture", Support: SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SourceCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering:   connector.OrderingPrefix,
			Boundedness:       []connector.Boundedness{connector.Bounded},
			LaneKinds:         []connector.LaneKind{connector.LaneKindScan},
			MaxLanes:          1,
			UpstreamRetention: connector.RetentionUnbounded,
			UnitAssignment:    connector.UnitsStatic,
		},
		New: func(context.Context, *config.Config) (*probeSource, error) { return &probeSource{}, nil },
	})

	ds := r.Descriptors()
	if len(ds) != 1 {
		t.Fatalf("expected one descriptor, got %d", len(ds))
	}

	b, err := json.Marshal(ds[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// The enum fields must be tokens, not ordinals and not base64.
	got := string(b)
	if !strings.Contains(got, `"support":"community"`) {
		t.Errorf(`support did not marshal as a token: %s`, got)
	}
	if strings.Contains(got, `"lane_kinds":"`) {
		t.Errorf(`lane_kinds marshalled as base64, the defect this fixes: %s`, got)
	}

	var back Descriptor
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("a Descriptor cannot be decoded from its own encoding: %v", err)
	}
	if back.Support != ds[0].Support {
		t.Errorf("Support round-tripped to %v, want %v", back.Support, ds[0].Support)
	}
	if back.Name != ds[0].Name || back.Kind != ds[0].Kind {
		t.Errorf("identity did not survive the round trip: %+v", back)
	}
}
