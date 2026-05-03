package httpserver_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"canal.ingestion-edge-go/internal/buffer"
	"canal.ingestion-edge-go/internal/dedupe"
	"canal.ingestion-edge-go/internal/httpserver"
)

func newTestServer(t *testing.T, d httpserver.Deps) *httptest.Server {
	t.Helper()
	return httptest.NewServer(httpserver.NewHandler(d))
}

func validEvent(id string) map[string]any {
	return map[string]any{
		"id":         id,
		"type":       "test.event",
		"occurredAt": "2026-05-03T12:00:00.000Z",
		"payload":    map[string]any{"n": 1},
	}
}

func TestHealth(t *testing.T) {
	srv := newTestServer(t, httpserver.Deps{
		Buffer: buffer.NewP1Local(),
		Seen:   dedupe.New(),
	})
	defer srv.Close()
	res, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(res.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Fatalf("status field %v", body["status"])
	}
	if body["service"] != "ingestion-edge-go" {
		t.Fatalf("service %v", body["service"])
	}
	ver, _ := body["version"].(string)
	if len(ver) < 5 || ver[0] < '0' || ver[0] > '9' {
		t.Fatalf("version %q", ver)
	}
}

func TestStream501(t *testing.T) {
	srv := newTestServer(t, httpserver.Deps{
		Buffer: buffer.NewP1Local(),
		Seen:   dedupe.New(),
	})
	defer srv.Close()
	res, err := http.Get(srv.URL + "/v1/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status %d", res.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(res.Body).Decode(&body)
	if _, ok := body["error"].(string); !ok {
		t.Fatal("missing error")
	}
	if _, ok := body["message"].(string); !ok {
		t.Fatal("missing message")
	}
}

func TestPostEvents202(t *testing.T) {
	id := "evt-test-" + strings.ReplaceAll(t.Name(), "/", "-")
	srv := newTestServer(t, httpserver.Deps{
		Buffer: buffer.NewP1Local(),
		Seen:   dedupe.New(),
	})
	defer srv.Close()
	payload := map[string]any{
		"source": "integration-test",
		"events": []any{validEvent(id)},
	}
	b, _ := json.Marshal(payload)
	res, err := http.Post(srv.URL+"/v1/events", "application/json", strings.NewReader(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(res.Body)
		t.Fatalf("status %d %s", res.StatusCode, body)
	}
	var out struct {
		Accepted     int      `json:"accepted"`
		DuplicateIds []string `json:"duplicateIds"`
	}
	_ = json.NewDecoder(res.Body).Decode(&out)
	if out.Accepted != 1 || len(out.DuplicateIds) != 0 {
		t.Fatalf("%+v", out)
	}
}

func TestBufferDedupe(t *testing.T) {
	idA := "buf-a-test"
	idB := "buf-b-test"
	evb := buffer.NewP1Local()
	srv := newTestServer(t, httpserver.Deps{
		Buffer: evb,
		Seen:   dedupe.New(),
	})
	defer srv.Close()
	first := map[string]any{
		"source": "buf-test",
		"events": []any{validEvent(idA), validEvent(idB)},
	}
	b1, _ := json.Marshal(first)
	res1, _ := http.Post(srv.URL+"/v1/events", "application/json", strings.NewReader(string(b1)))
	if res1.StatusCode != http.StatusAccepted {
		t.Fatal(res1.StatusCode)
	}
	res1.Body.Close()
	rows := evb.ReadAfter(0, 10)
	if len(rows) != 2 {
		t.Fatalf("rows %d", len(rows))
	}
	if rows[0].Source != "buf-test" || rows[0].Event.ID != idA {
		t.Fatalf("row0 %+v", rows[0])
	}
	if rows[1].Event.ID != idB || rows[0].Sequence+1 != rows[1].Sequence {
		t.Fatalf("seq")
	}
	dup := map[string]any{"source": "buf-test", "events": []any{validEvent(idA)}}
	b2, _ := json.Marshal(dup)
	res2, _ := http.Post(srv.URL+"/v1/events", "application/json", strings.NewReader(string(b2)))
	if res2.StatusCode != http.StatusAccepted {
		t.Fatal(res2.StatusCode)
	}
	var out struct {
		DuplicateIds []string `json:"duplicateIds"`
	}
	_ = json.NewDecoder(res2.Body).Decode(&out)
	res2.Body.Close()
	if len(out.DuplicateIds) != 1 || out.DuplicateIds[0] != idA {
		t.Fatalf("%+v", out)
	}
	if len(evb.ReadAfter(0, 10)) != 2 {
		t.Fatal("buffer grew on duplicate")
	}
}

func TestReplayIdempotency(t *testing.T) {
	id := "dup-test-id"
	srv := newTestServer(t, httpserver.Deps{
		Buffer: buffer.NewP1Local(),
		Seen:   dedupe.New(),
	})
	defer srv.Close()
	payload := map[string]any{"source": "integration-test", "events": []any{validEvent(id)}}
	b, _ := json.Marshal(payload)
	r1, _ := http.Post(srv.URL+"/v1/events", "application/json", strings.NewReader(string(b)))
	if r1.StatusCode != http.StatusAccepted {
		t.Fatal(r1.StatusCode)
	}
	var j1 map[string]any
	_ = json.NewDecoder(r1.Body).Decode(&j1)
	r1.Body.Close()
	if j1["accepted"].(float64) != 1 {
		t.Fatal(j1)
	}
	r2, _ := http.Post(srv.URL+"/v1/events", "application/json", strings.NewReader(string(b)))
	if r2.StatusCode != http.StatusAccepted {
		t.Fatal(r2.StatusCode)
	}
	var j2 map[string]any
	_ = json.NewDecoder(r2.Body).Decode(&j2)
	r2.Body.Close()
	if int(j2["accepted"].(float64)) != 0 {
		t.Fatal(j2)
	}
	dups, _ := j2["duplicateIds"].([]any)
	if len(dups) != 1 || dups[0].(string) != id {
		t.Fatal(j2)
	}
}

// Control and adapter-instance routes are owned by ingestion-control-plane, not the Go edge.
func TestControlAndAdapterRoutesNotOnDataPlane(t *testing.T) {
	srv := newTestServer(t, httpserver.Deps{
		Buffer: buffer.NewP1Local(),
		Seen:   dedupe.New(),
	})
	defer srv.Close()
	cases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/v1/control/pipeline", ""},
		{http.MethodGet, "/v1/control/canal/segments", ""},
		{http.MethodGet, "/v1/adapter-instances", ""},
		{http.MethodPost, "/v1/adapter-instances", `{"catalogAdapterId":"adapter.placeholder.sink_connector"}`},
		{http.MethodGet, "/v1/adapter-instances/any-id", ""},
		{http.MethodPatch, "/v1/adapter-instances/any-id", `{}`},
		{http.MethodDelete, "/v1/adapter-instances/any-id", ""},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			var r *http.Request
			var err error
			if tc.body != "" {
				r, err = http.NewRequest(tc.method, srv.URL+tc.path, strings.NewReader(tc.body))
				if err != nil {
					t.Fatal(err)
				}
				r.Header.Set("Content-Type", "application/json")
			} else {
				r, err = http.NewRequest(tc.method, srv.URL+tc.path, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			res, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusNotFound {
				t.Fatalf("want 404, got %d", res.StatusCode)
			}
		})
	}
}

func TestPostEvents400(t *testing.T) {
	srv := newTestServer(t, httpserver.Deps{
		Buffer: buffer.NewP1Local(),
		Seen:   dedupe.New(),
	})
	defer srv.Close()
	cases := []struct {
		name string
		body string
	}{
		{"array", `[]`},
		{"missing source", `{"events":[{"id":"i","type":"t","occurredAt":"2026-05-03T12:00:00.000Z"}]}`},
		{"empty events", `{"source":"s","events":[]}`},
		{"bad date", `{"source":"s","events":[{"id":"i","type":"t","occurredAt":"not-a-date"}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := http.Post(srv.URL+"/v1/events", "application/json", strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusBadRequest {
				t.Fatalf("status %d", res.StatusCode)
			}
			var out map[string]any
			_ = json.NewDecoder(res.Body).Decode(&out)
			if out["error"] != "validation_error" {
				t.Fatal(out)
			}
			if out["message"].(string) == "" {
				t.Fatal(out)
			}
		})
	}
}
