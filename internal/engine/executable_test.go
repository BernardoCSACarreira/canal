package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/BernardoCSACarreira/canal/pkg/store"
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
type committerSource struct{ done bool }

func (c *committerSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil {
		return err
	}
	if len(as) > 0 {
		c.done = true // warm start: this fixture has nothing more to produce
		return nil
	}
	_, err = rt.Lanes().Announce(ctx, connector.LaneSpec{
		Name: "rows", Stream: "lines", Kind: connector.LaneKindScan,
		Ordering: connector.OrderingPrefix, Boundedness: connector.Bounded, Group: "scan",
	})
	return err
}

// Read produces one batch and then ends, so the commit protocol has something real to publish.
func (c *committerSource) Read(_ context.Context, dst *record.Batch) error {
	if c.done {
		return fault.ErrEndOfInput
	}
	c.done = true
	dst.Reset()
	for i := 0; i < 8; i++ {
		r := dst.Add()
		if r == nil {
			break
		}
		r.Payload = record.BytesPayload([]byte(fmt.Sprintf("row-%d", i)))
	}
	var b [8]byte
	b[7] = 8
	dst.Position = record.Position{Token: record.Blob{Version: 1, Bytes: b[:]}, Safe: true, At: time.Now()}
	return nil
}
func (*committerSource) Commit(context.Context, connector.Ack) error { return nil }
func (*committerSource) Close(context.Context) error                 { return nil }

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
				Config: map[string]any{"table": "t", "stage_uri": "file://" + dir,
					"codec": map[string]any{"encoder": "raw", "framer": "newline"}},
				Inputs: []spec.Edge{{From: "in"}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "lines",
			Read:   []connector.LaneKind{connector.LaneKindScan},
			Write:  connector.DestAppend,
		}},
	}
}

func TestACommitterPipelineRunsThroughTwoPhaseCommit(t *testing.T) {
	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}

	p, neg, diags := engine.Build(context.Background(), registry.Default, exactlyOnceSpec(dir),
		engine.Deps{State: st, Worker: "test", FlushInterval: 10 * time.Millisecond, GracePeriod: 5 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}

	// The negotiation reaches the strongest tier canal offers, and — the part that is new — the
	// engine no longer refuses to run it.
	if neg.Guarantee != connector.ExactlyOnce || neg.AckPoint != "commit" {
		t.Fatalf("negotiated %s at %q, want exactly_once at commit", neg.Guarantee, neg.AckPoint)
	}
	if err := engine.Executable(neg); err != nil {
		t.Fatalf("a commit ack point is implemented now, but Executable refused it: %v", err)
	}
	for _, d := range diags {
		if strings.Contains(d.Message, "never calls connector.Committer") {
			t.Errorf("Build still warns that Committer is unimplemented: %v", d)
		}
	}

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_ = p.Close(context.Background())
	_ = st.Close()

	// A checkpoint exists. Nothing in this module had ever constructed one.
	cp := readCheckpoint(t, filepath.Join(dir, "state"))
	if cp.Header.Format != engine.CheckpointFormat {
		t.Errorf("checkpoint format is %d, want %d", cp.Header.Format, engine.CheckpointFormat)
	}
	if cp.Header.ID == 0 {
		t.Error("the checkpoint carries no id; a higher id must subsume every lower one")
	}
	if cp.Header.Pipeline != "eo" || cp.Header.Worker != "test" {
		t.Errorf("the header does not identify the writer: %+v", cp.Header)
	}
}

// readCheckpoint decodes the durable checkpoint record.
func readCheckpoint(t *testing.T, stateDir string) engine.Checkpoint {
	t.Helper()
	st, err := wal.Open(stateDir)
	if err != nil {
		t.Fatalf("reopening the store: %v", err)
	}
	defer st.Close()
	key := store.CheckpointKey("acme", "eo")
	got, err := st.Get(context.Background(), []store.Key{key})
	if err != nil {
		t.Fatalf("reading the checkpoint: %v", err)
	}
	v, ok := got[key.String()]
	if !ok {
		t.Fatal("no checkpoint was ever written")
	}
	var cp engine.Checkpoint
	if err := json.Unmarshal(v.Value, &cp); err != nil {
		t.Fatalf("decoding the checkpoint: %v", err)
	}
	return cp
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
