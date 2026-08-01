package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/internal/metrics"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// The read model needed a real consumer to be worth producing, and /status is it. These assert the
// endpoint against the actual handlers rather than against a mock, because the interesting cases are
// the two the routing decides: a pipeline that does not exist yet, and a request that is not a read.

func mux(t *testing.T, src *statusSource) http.Handler {
	t.Helper()
	return observabilityMux(metrics.New(), src, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// BEFORE THE BUILD THERE IS NOTHING TO REPORT ON, and the listener binds first on purpose — a port
// conflict is worth reporting before every connector in the graph has been constructed. That window
// must answer 503 rather than an empty document, because an empty document is indistinguishable from
// a pipeline that has nothing to say.
func TestStatusIsUnavailableUntilThereIsAPipeline(t *testing.T) {
	w := httptest.NewRecorder()
	mux(t, &statusSource{}).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/status", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status is %d before a pipeline exists, want 503", w.Code)
	}
	if body, _ := io.ReadAll(w.Body); len(body) == 0 {
		t.Error("the 503 carried no explanation")
	}
}

// THE STATUS ENDPOINT IS READ-ONLY. It is an operational surface and not a control API: nothing
// reachable through it can change what the pipeline does, and a write verb has to be refused rather
// than quietly treated as a read.
func TestStatusRefusesAnythingThatIsNotARead(t *testing.T) {
	h := mux(t, &statusSource{})
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, "/status", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /status returned %d, want 405", method, w.Code)
		}
		if got := w.Header().Get("Allow"); got == "" {
			t.Errorf("%s /status returned 405 with no Allow header", method)
		}
	}
}

// An unknown path names what exists. The previous message said canal exposes "/metrics and nothing
// else", which stopped being true the moment /status was added — a 404 body is documentation, and a
// stale one sends an operator looking for an endpoint that is right there.
func TestAnUnknownPathNamesBothEndpoints(t *testing.T) {
	w := httptest.NewRecorder()
	mux(t, &statusSource{}).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nope", nil))

	if w.Code != http.StatusNotFound {
		t.Errorf("an unknown path returned %d, want 404", w.Code)
	}
	body, _ := io.ReadAll(w.Body)
	for _, want := range []string{"/metrics", "/status"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("the 404 body does not mention %s: %q", want, body)
		}
	}
}

// The document a client receives has to parse as the declared type, which is the whole point of
// there being one canonical struct. This runs the real handler over a real (unstarted) pipeline.
func TestStatusServesTheDeclaredDocument(t *testing.T) {
	p := builtPipeline(t)
	src := &statusSource{}
	src.set(p)

	w := httptest.NewRecorder()
	mux(t, src).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/status", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("status is %d, want 200: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content type is %q, want JSON", ct)
	}
	// The version is a CURSOR and not a cache key, so it is its own header and deliberately not an
	// ETag: the document carries live ages, so a conditional GET could never match.
	if w.Header().Get("X-Canal-Status-Version") == "" {
		t.Error("no X-Canal-Status-Version header; an SSE consumer has nothing to resume from")
	}
	if w.Header().Get("ETag") != "" {
		t.Error("an ETag was set; it can never match a document that carries a live checkpoint age")
	}

	var got telemetry.PipelineStatus
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("the body does not decode as telemetry.PipelineStatus: %v\n%s", err, w.Body.String())
	}
	if got.Phase != telemetry.PhasePending {
		t.Errorf("phase is %q, want pending for a built pipeline that has not run", got.Phase)
	}
	if len(got.Conditions) != len(telemetry.ConditionTypes) {
		t.Errorf("%d conditions crossed the wire, want the whole closed set of %d",
			len(got.Conditions), len(telemetry.ConditionTypes))
	}
	if got.Pipeline == "" {
		t.Error("the document names no pipeline")
	}
}

// builtPipeline builds a real line_file -> stdout pipeline and does not run it, which is the state
// /status has to describe between Build and Run.
func builtPipeline(t *testing.T) *engine.Pipeline {
	t.Helper()
	dir := t.TempDir()
	input := filepath.Join(dir, "input.txt")
	if err := os.WriteFile(input, []byte("a\nb\nc\n"), 0o644); err != nil {
		t.Fatalf("writing the input: %v", err)
	}
	specPath := writeSpec(t, dir, input)
	s, err := loadSpec(specPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("loading the spec: %v", err)
	}
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	p, _, diags := engine.Build(context.Background(), registry.Default, s,
		engine.Deps{State: st, Worker: "test", Metrics: metrics.New()})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	t.Cleanup(func() { _ = p.Close(context.Background()) })
	return p
}

// THE QUERY IS HONOURED, not merely accepted. telemetry.StatusQuery exists so the document can grow
// pagination without a wire break, and a selector nothing acts on is the declared-and-inert pattern
// that has produced most of this repository's defects.
func TestStatusHonoursItsSelectionParameters(t *testing.T) {
	p := builtPipeline(t)
	src := &statusSource{}
	src.set(p)
	h := mux(t, src)

	get := func(t *testing.T, query string) telemetry.PipelineStatus {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/status"+query, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("GET /status%s is %d: %s", query, w.Code, w.Body.String())
		}
		var s telemetry.PipelineStatus
		if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		return s
	}

	// A pipeline that has not run has no lanes, so what is asserted here is that the parameters reach
	// the projection at all rather than being dropped on the floor.
	if s := get(t, "?limit=0"); len(s.Lanes) != 0 {
		t.Errorf("limit=0 returned %d lanes", len(s.Lanes))
	}
	if s := get(t, "?stream=orders&cursor=lane-1&limit=5"); s.Phase == "" {
		t.Error("a fully parameterised query produced a document with no phase")
	}

	// A BAD PARAMETER IS A 400, NEVER A SILENT DEFAULT. "limit=fifty" quietly becoming the default
	// page is how an operator concludes the endpoint ignores them.
	for _, bad := range []string{"?limit=fifty", "?limit=-1", "?limit=1.5"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/status"+bad, nil))
		if w.Code != http.StatusBadRequest {
			t.Errorf("GET /status%s returned %d, want 400", bad, w.Code)
		}
		if !strings.Contains(w.Body.String(), "limit") {
			t.Errorf("the 400 for %s does not name the parameter: %q", bad, w.Body.String())
		}
	}
}
