package enterprise

import (
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

func TestRegistrationAccepts(t *testing.T) {
	se, ok := registry.Default.Source("stress_shardlog")
	if !ok {
		t.Fatal("source not registered")
	}
	if len(se.Descriptor.Warnings) > 0 {
		t.Errorf("source warnings: %v", se.Descriptor.Warnings)
	}
	ke, ok := registry.Default.Sink("stress_staged")
	if !ok {
		t.Fatal("sink not registered")
	}
	if len(ke.Descriptor.Warnings) > 0 {
		t.Errorf("sink warnings: %v", ke.Descriptor.Warnings)
	}
	t.Logf("source caps: %d entries", len(se.Descriptor.Capabilities))
}
