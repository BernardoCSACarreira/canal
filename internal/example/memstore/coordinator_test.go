package memstore_test

import (
	"testing"

	"github.com/BernardoCSACarreira/canal/internal/example/memstore"
	"github.com/BernardoCSACarreira/canal/pkg/coordtest"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// THE PLACEMENT PROTOCOL'S RULES LIVE IN pkg/coordtest NOW. They started here — pinned against
// this in-memory coordinator before the engine or any other implementation existed — and were
// promoted verbatim when a second implementation arrived, for storetest's reason: a contract two
// implementations each prove separately gets proved wrong twice. This file keeps only the wiring.
func TestCoordinatorConformance(t *testing.T) {
	coordtest.Run(t, coordtest.Subject{
		Name: "memstore",
		New: func(_ *testing.T, clk *coordtest.Clock) store.Coordinator {
			c := memstore.NewCoordinator()
			c.Now = clk.Now
			return c
		},
		// One process IS this coordinator's whole domain, so the same instance is the honest
		// second handle: the suite's cross-instance case then pins the semantics — the domain is
		// what is shared, never the handle — without pretending this scaffolding is durable.
		Attach: func(_ *testing.T, _ *coordtest.Clock, c store.Coordinator) store.Coordinator {
			return c
		},
	})
}
