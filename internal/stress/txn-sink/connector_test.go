// Scenario harness for the hostile two-phase-commit case. It exists as EVIDENCE that the
// findings in connector.go were produced by running code rather than by reading
// signatures: TestHostileFlow already caught one real defect in the commit path, which is
// argued as BREAKAGE 6.
package txnsink

import (
	"context"
	"errors"
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/connectortest"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

// The runtime fake is connectortest.SinkRuntime, embedded rather than hand-written.
//
// This test used to carry 23 lines of fake implementing SinkRuntime method by method, and
// adding Schemas(), Streams() and Config() to that interface broke it — which contradicted
// the whole claim that a runtime is canal's free growth path. The claim is now true of tests
// as well as of connectors.
type fakeRT struct{ connectortest.SinkRuntime }

// flaky answers the first Commit with an indeterminate error AFTER having published,
// which is the exact hostile behaviour: the transaction landed and the client cannot know.
type flaky struct {
	stubWarehouse
	lied bool
}

// resolvable is stubWarehouse with a switchable Resolve failure, so the IN-DOUBT branch of
// ResolveStale can be exercised: the artifact is staged, the warehouse cannot say whether it
// landed, and the only honest answer is "keep it pending".
type resolvable struct {
	stubWarehouse
	failResolve bool
}

func (r *resolvable) Resolve(ctx context.Context, table string, files []string) (map[string]bool, error) {
	if r.failResolve {
		return nil, errIndeterminate
	}
	return r.stubWarehouse.Resolve(ctx, table, files)
}

// mustEncodeHandle is encodeHandle for a test that has already decided the handle is valid.
func mustEncodeHandle(h handleV1) record.Blob {
	b, err := encodeHandle(h)
	if err != nil {
		panic(err)
	}
	return b
}

func (f *flaky) Commit(ctx context.Context, table, txn string, files []string) error {
	_ = f.stubWarehouse.Commit(ctx, table, txn, files)
	if !f.lied {
		f.lied = true
		return errIndeterminate
	}
	return nil
}

func TestHostileFlow(t *testing.T) {
	ctx := context.Background()
	wh := &flaky{}
	k := &sink{
		table: "t", stageURI: "s3://stage", minBytes: 4096, partSize: 1024,
		wh: wh, open: map[string]*upload{}, sealedHere: map[string]bool{},
	}
	if err := k.Open(ctx, &fakeRT{}, connector.Opening{
		Guarantee: connector.ExactlyOnce,
		Streams:   []connector.ConfiguredStream{{Stream: "events", Mode: connector.DestAppend}},
	}); err != nil {
		t.Fatal(err)
	}

	write := func(n int, id record.RecordID) {
		req := &connector.Request{
			Body:      make([]byte, n),
			Partition: "events",
			Count:     1,
			Records:   []record.Ref{{ID: id, Lane: "lane-1", Stream: "events"}},
		}
		res, err := k.Write(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if ok, want := res.Reconcile(req.Count); !ok {
			t.Fatalf("reconcile: got %d want %d", res.Written, want)
		}
	}

	// Accumulate below the minimum across several requests and a checkpoint.
	write(1000, 1)
	write(1000, 2)
	cs, err := k.PrepareCommit(ctx, connector.CommitPoint{ID: 1, Reason: connector.FlushCheckpoint})
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 1 {
		t.Fatalf("want 1 committable, got %d", len(cs))
	}
	outs, err := k.Commit(ctx, cs)
	if err != nil {
		t.Fatal(err)
	}
	if outs[0].Disposition != connector.DispositionRetryLater {
		t.Fatalf("an unsealed, undersized file must stay pending, got %s", outs[0].Disposition)
	}

	// Survive a restart mid-accumulation, in whichever order the core chooses to call
	// RestoreState, AbortStale and Commit (BREAKAGE 2).
	blobs, err := k.SnapshotState(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	k2 := &sink{
		table: "t", stageURI: "s3://stage", minBytes: 4096, partSize: 1024,
		wh: wh, open: map[string]*upload{}, sealedHere: map[string]bool{},
	}
	if err := k2.Open(ctx, &fakeRT{}, connector.Opening{Guarantee: connector.ExactlyOnce}); err != nil {
		t.Fatal(err)
	}
	if err := k2.RestoreState(ctx, blobs); err != nil {
		t.Fatal(err)
	}
	// The NORMATIVE recovery order: Open -> RestoreState -> ResolveStale -> Commit -> Write.
	// RestoreState first, because AbortStale's own job is to discard committables "this sink no
	// longer recognises" and recognition IS the restored writer state (BREAKAGE 2).
	if _, err := k2.ResolveStale(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := k2.Commit(ctx, cs); err != nil {
		t.Fatal(err)
	}
	if u := k2.open["events"]; u == nil || u.uploadID == "" || u.uploaded != 2000 {
		t.Fatalf("restored upload wrong: %+v", u)
	}

	// Cross the minimum, seal, and hit the commit that lands but reports indeterminate.
	k = k2
	write(3000, 3)
	cs2, err := k.PrepareCommit(ctx, connector.CommitPoint{ID: 2, Reason: connector.FlushCheckpoint})
	if err != nil {
		t.Fatal(err)
	}
	h, err := decodeHandle(cs2[0].Handle)
	if err != nil {
		t.Fatal(err)
	}
	if !h.Sealed {
		t.Fatalf("expected the file to be sealed at %d bytes", h.Bytes)
	}
	outs, err = k.Commit(ctx, cs2)
	if err != nil {
		t.Fatal(err)
	}
	if outs[0].Disposition != connector.DispositionAlreadyCommitted {
		t.Fatalf("a timed-out-but-landed commit must resolve to already_committed, got %s (%v)",
			outs[0].Disposition, outs[0].Fault)
	}
	if !wh.lied {
		t.Fatal("the indeterminate path was never exercised")
	}
	if len(k.open) != 0 {
		t.Fatalf("a published file must be released, still holding %d", len(k.open))
	}

	// Recovery re-presents the same committable: must not double-commit, and must say so.
	outs, err = k.Commit(ctx, cs2)
	if err != nil {
		t.Fatal(err)
	}
	if outs[0].Disposition != connector.DispositionAlreadyCommitted {
		t.Fatalf("a re-presented committable must be already_committed, got %s", outs[0].Disposition)
	}
	if err := k.Close(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRegistersAndConstructs(t *testing.T) {
	e, ok := registry.Default.Sink("stress_txn_warehouse")
	if !ok {
		t.Fatal("not registered")
	}
	if len(e.Descriptor.Warnings) != 0 {
		t.Fatalf("registration warnings: %v", e.Descriptor.Warnings)
	}
	cfg, diags := e.Spec.Validate(map[string]any{
		"table":     "analytics.public.events",
		"stage_uri": "s3://stage/events",
	})
	if diags.HasErrors() {
		t.Fatalf("config: %v", diags.Error())
	}
	k, err := e.New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	s := k.(*sink)
	if s.minBytes != 128<<20 || s.partSize != 8<<20 {
		t.Fatalf("sizes: min=%d part=%d", s.minBytes, s.partSize)
	}
	if s.maxAge.Minutes() != 15 {
		t.Fatalf("maxAge=%v", s.maxAge)
	}
}

// TestB1_ResolveStaleAnswersPerItem is BREAKAGE 1's regression test.
//
// B1 said: AbortStale returns ONE naked error for a whole batch, so a sink whose commit can
// time out AFTER SUCCEEDING cannot report per item whether a prepared transaction landed, is
// reclaimable, or is still in doubt — and the only remaining implementation was to attempt the
// commit from inside the abort path.
//
// This drives four committables through connector.StaleResolver in one call and asserts four
// different dispositions come back. That is the whole fix: one call, four truths.
func TestB1_ResolveStaleAnswersPerItem(t *testing.T) {
	ctx := context.Background()
	wh := &resolvable{}
	k := &sink{
		table: "t", stageURI: "s3://stage", minBytes: 1, partSize: 1024,
		wh: wh, open: map[string]*upload{}, sealedHere: map[string]bool{},
	}
	if err := k.Open(ctx, &fakeRT{}, connector.Opening{Guarantee: connector.ExactlyOnce}); err != nil {
		t.Fatal(err)
	}

	// (a) An unreadable handle names no artifact. Abortable, and now sayable alone.
	garbage := connector.Committable{Handle: record.Blob{Version: 99, Bytes: []byte("nope")}}

	// (b) A sealed file the warehouse already published: already_committed, not aborted.
	// Aborting it would have been a lie in the old shape and is now impossible to say.
	up, err := wh.StartUpload(ctx, "s3://stage/landed.parquet")
	if err != nil {
		t.Fatal(err)
	}
	uri, err := wh.CompleteUpload(ctx, up, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := wh.Commit(ctx, "t", "prior-txn", []string{uri}); err != nil {
		t.Fatal(err)
	}
	landedH := handleV1{UploadID: up, StagedURI: uri, Partition: "landed", Stream: "events", Sealed: true, Records: 7}
	landed := connector.Committable{Handle: mustEncodeHandle(landedH), Records: 7}

	// (c) A sealed file the warehouse does NOT have: publishable on this pass -> committed.
	up2, err := wh.StartUpload(ctx, "s3://stage/pending.parquet")
	if err != nil {
		t.Fatal(err)
	}
	uri2, err := wh.CompleteUpload(ctx, up2, nil)
	if err != nil {
		t.Fatal(err)
	}
	pendingH := handleV1{UploadID: up2, StagedURI: uri2, Partition: "pending", Stream: "events", Sealed: true, Records: 3}
	pending := connector.Committable{Handle: mustEncodeHandle(pendingH), Records: 3}

	outs, err := k.ResolveStale(ctx, []connector.Committable{garbage, landed, pending})
	if err != nil {
		t.Fatalf("ResolveStale returned a batch-level error, which is exactly what it exists to avoid: %v", err)
	}
	if len(outs) != 3 {
		t.Fatalf("ResolveStale returned %d outcomes for 3 committables; a per-item answer must be per item", len(outs))
	}

	got := map[connector.Disposition]int{}
	for _, o := range outs {
		got[o.Disposition]++
	}
	if got[connector.DispositionAborted] != 1 {
		t.Errorf("the unreadable handle should be aborted alone; dispositions were %v", got)
	}
	if got[connector.DispositionAlreadyCommitted] != 1 {
		t.Errorf("the already-published file should be already_committed, never aborted; dispositions were %v", got)
	}
	if got[connector.DispositionCommitted] != 1 {
		t.Errorf("the publishable file should be committed; dispositions were %v", got)
	}
	t.Logf("one ResolveStale call over 3 committables returned %d distinct dispositions: %v", len(got), got)

	// And the in-doubt case, which is the one the old signature could not express at all: the
	// warehouse cannot answer, so the committable stays PENDING rather than being aborted
	// (losing what landed) or committed (creating what did not).
	wh.failResolve = true
	up3, err := wh.StartUpload(ctx, "s3://stage/indoubt.parquet")
	if err != nil {
		t.Fatal(err)
	}
	uri3, err := wh.CompleteUpload(ctx, up3, nil)
	if err != nil {
		t.Fatal(err)
	}
	doubtH := handleV1{UploadID: up3, StagedURI: uri3, Partition: "indoubt", Stream: "events", Sealed: true, Records: 5}
	outs, err = k.ResolveStale(ctx, []connector.Committable{{Handle: mustEncodeHandle(doubtH), Records: 5}})
	if err != nil {
		t.Fatalf("an unreachable warehouse must not fail the batch: %v", err)
	}
	if len(outs) != 1 || outs[0].Disposition != connector.DispositionRetryLater {
		t.Fatalf("an in-doubt committable must be retry_later, got %+v", outs)
	}
	if outs[0].Fault == nil || outs[0].Fault.Class != fault.Indeterminate {
		t.Fatalf("an in-doubt committable's fault must classify as indeterminate, got %+v", outs[0].Fault)
	}
	t.Log("in doubt is now a sentence: retry_later + indeterminate, per item, keeping the artifact pending")

	// The capability is declared, so the core will prefer this over AbortStale, and the
	// resolved handle is what the engine nil-checks.
	e, ok := registry.Default.Sink("stress_txn_warehouse")
	if !ok {
		t.Fatal("sink is not registered")
	}
	if !e.Caps.ResolvesStale {
		t.Error("ResolvesStale is not declared, so the core will keep calling AbortStale and " +
			"collapse every one of the answers above into one error")
	}
	rs, err := registry.ResolveSink("stress_txn_warehouse", k, e.Caps)
	if err != nil {
		t.Fatalf("ResolveSink: %v", err)
	}
	if rs.Stale == nil {
		t.Fatal("ResolvesStale is declared but the resolved StaleResolver handle is nil")
	}
}
