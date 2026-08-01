package engine_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"

	// Registers stress_txn_warehouse, which implements Committer, WriterState and Flusher. It is
	// the only sink in the module that earns its acknowledgement anywhere other than Write.
	_ "github.com/BernardoCSACarreira/canal/internal/stress/txn-sink"
)

// This is the defect that running a hostile connector through the engine turned up, and it is the
// worst class of defect this project can have: canal promising a guarantee it does not implement.
//
// Negotiation is a pure function of COMPONENT capabilities. It asks what the source, the sink and
// the store can promise between them; it never asks whether the engine can drive the answer. So a
// source declaring StableKeys and durable retention, paired with a sink implementing Committer,
// negotiated exactly_once with ack_point=commit — while internal/engine settles on Write returning
// cleanly and has never called Committer.
//
// Left alone, that pipeline would have built, reported exactly-once to the operator, settled every
// record the moment Write returned, advanced the source cursor past it, and left the sink holding
// staged data nothing would ever commit. Loss, under the strongest promise the system can make.

// committerSource is a source with capabilities strong enough that nothing on the source side caps
// the negotiation. It reads nothing: the point is what Build promises, not what moves.
type committerSource struct{}

func (*committerSource) Open(context.Context, connector.SourceRuntime) error { return nil }
func (*committerSource) Read(context.Context, *record.Batch) error           { return fault.ErrEndOfInput }
func (*committerSource) Commit(context.Context, connector.Ack) error         { return nil }
func (*committerSource) Close(context.Context) error                         { return nil }

func init() {
	registry.AddSource(registry.Default, registry.SourceDef[*committerSource]{
		Meta: registry.Meta{
			Name: "exactly_once_capable", Version: "1.0.0", Title: "Strong source",
			Summary: "Declares everything the source side of exactly-once needs.",
			Notes:   "Origin.Key is the upstream row id, stable across re-reads.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SourceCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering:   connector.OrderingPrefix,
			Boundedness:       []connector.Boundedness{connector.Bounded},
			LaneKinds:         []connector.LaneKind{connector.LaneKindScan},
			MaxLanes:          1,
			StableKeys:        true,
			Replayable:        true,
			UpstreamRetention: connector.RetentionUnbounded,
		},
		New: func(context.Context, *config.Config) (*committerSource, error) {
			return &committerSource{}, nil
		},
	})
}

func exactlyOnceSpec(dir string) spec.Spec {
	return spec.Spec{
		Tenant: "acme", ID: "eo",
		Guarantee: connector.ExactlyOnce,
		Retry:     fault.DefaultRetry,
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: "exactly_once_capable"},
			{ID: "out", Kind: registry.KindSink, Name: "stress_txn_warehouse",
				Config: map[string]any{"table": "t", "stage_uri": "file://" + dir},
				Inputs: []spec.Edge{{From: "in"}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "lines",
			Read:   []connector.LaneKind{connector.LaneKindScan},
			Write:  connector.DestAppend,
		}},
	}
}

func TestRunRefusesAContractThisBuildCannotHonour(t *testing.T) {
	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, neg, diags := engine.Build(context.Background(), registry.Default, exactlyOnceSpec(dir),
		engine.Deps{State: st, Worker: "test", FlushInterval: 10 * time.Millisecond, GracePeriod: time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	// The negotiation itself is honest — the components really can do this between them. It is the
	// ENGINE that cannot, and that is a different fact with a different lifetime.
	if neg.Guarantee != connector.ExactlyOnce || neg.AckPoint != "commit" {
		t.Fatalf("negotiated %s at %q; this test needs a commit ack point to be meaningful",
			neg.Guarantee, neg.AckPoint)
	}

	// Build WARNS rather than erroring, so it stays usable as the negotiation entry point.
	var warned bool
	for _, d := range diags {
		if strings.Contains(d.Message, "never calls connector.Committer") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("Build did not warn that the contract is unexecutable:\n%v", diags)
	}

	// RUN REFUSES. This is the assertion that matters: nothing may move under a promise this build
	// cannot keep.
	err = p.Run(context.Background())
	if err == nil {
		t.Fatal("Run executed a pipeline promising exactly-once through a Committer the engine never calls")
	}
	for _, want := range []string{"exactly_once", "commit", "connector.Committer"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q, so it does not say what would have to change:\n%v", want, err)
		}
	}
	if c := fault.ClassOf(err); c != fault.PermanentContract {
		t.Errorf("the refusal is class %s, want permanent_contract: retrying cannot help", c)
	}
}

// The complement, and the reason the check is a whitelist of one rather than a blacklist: an
// ordinary sink that is durable when Write returns is executable, and must not be caught by it.
func TestAWriteDurableSinkIsExecutable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 3)

	c := &collector{}
	sinkName := registerCollector(t, "collector_executable", c)
	d, closeStore := deps(t, filepath.Join(dir, "state"))
	defer closeStore()

	p, neg, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(sinkName, path), d)
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	if err := engine.Executable(neg); err != nil {
		t.Fatalf("a Write-durable sink was reported unexecutable: %v", err)
	}
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(c.got()) != 3 {
		t.Errorf("got %d records, want 3", len(c.got()))
	}
}
