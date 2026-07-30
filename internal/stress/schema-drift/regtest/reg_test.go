package regtest

import (
	"context"
	"testing"

	_ "github.com/BernardoCSACarreira/canal/internal/stress/schema-drift"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/connectortest"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

func TestRegistrationDoesNotPanic(t *testing.T) {
	for _, n := range []string{"stress_schema_drift"} {
		if _, ok := registry.Default.Source(n); !ok {
			t.Fatalf("source %q not registered", n)
		}
	}
	for _, n := range []string{"stress_ddl_sink", "stress_absorb_sink", "stress_dlq_sink"} {
		if _, ok := registry.Default.Sink(n); !ok {
			t.Fatalf("sink %q not registered", n)
		}
	}
}

// TestB1_SchemaDeclarationTakesTheWorkingPath is BREAKAGE B1's regression test.
//
// B1 said: a source has no channel through which to publish a schema body or an ordered
// schema.Change, so the entire drift subsystem — which the core consumes in the checkpoint,
// the quiesce, the sink negotiation and a five-mode policy — had no producer.
//
// The connector anticipated the fix as a STRUCTURAL PROBE (`proposedSchemaRuntime`) rather
// than as prose, so this test asserts the thing that actually matters: connector.SourceRuntime
// satisfies that probe, which means declareOrDegrade takes the working path and not the
// degraded one that halts the pipeline at the first schema change.
func TestB1_SchemaDeclarationTakesTheWorkingPath(t *testing.T) {
	// The probe, restated here so this test fails if either side's signature drifts.
	type schemaChannel interface {
		Schemas() connector.SchemaLookup
		Declare(ctx context.Context, ch schema.Change, result *schema.Schema) (schema.Ref, error)
	}
	var rt connector.SourceRuntime = &connectortest.SourceRuntime{}
	pr, ok := rt.(schemaChannel)
	if !ok {
		t.Fatal("connector.SourceRuntime does not satisfy the schema channel; the drift subsystem " +
			"is a consumer with no producer again")
	}

	// And it works end to end: a declared change returns a resolvable ref.
	body := &schema.Schema{Fields: []schema.Field{
		{Name: "id", Type: schema.TypeInt64},
		{Name: "email", Type: schema.TypeString, Nullable: true},
	}}
	ref, err := pr.Declare(context.Background(), schema.Change{
		Kind:   schema.AddField,
		Stream: "orders",
		Field:  []string{"email"},
		To:     &body.Fields[1],
		Result: body,
	}, body)
	if err != nil {
		t.Fatalf("Declare: %v", err)
	}
	got, err := pr.Schemas().Get(context.Background(), ref)
	if err != nil {
		t.Fatalf("the ref Declare returned does not resolve: %v", err)
	}
	if len(got.Fields) != 2 {
		t.Fatalf("resolved schema has %d fields, want 2", len(got.Fields))
	}

	// The sink half: a ref minted AFTER Open must be resolvable by the sink too, or
	// ApplySchemaChange for a CreateStream is unappliable.
	var sk connector.SinkRuntime = &connectortest.SinkRuntime{}
	if _, ok := sk.(interface {
		Schemas() connector.SchemaLookup
	}); !ok {
		t.Fatal("connector.SinkRuntime cannot resolve a schema ref minted after Open")
	}

	// And the capability is declarable, which it was not before.
	e, ok := registry.Default.Source("stress_schema_drift")
	if !ok {
		t.Fatal("source is not registered")
	}
	if !e.Caps.ProducesSchema {
		t.Error("ProducesSchema is still false on a source that does nothing but produce schemas")
	}
}
