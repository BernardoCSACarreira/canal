package httpserver_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"canal.ingestion-edge-go/internal/adapters"
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
		Buffer:   buffer.NewP1Local(),
		Seen:     dedupe.New(),
		Adapters: adapters.NewStore(),
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
		Buffer:   buffer.NewP1Local(),
		Seen:     dedupe.New(),
		Adapters: adapters.NewStore(),
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
		Buffer:   buffer.NewP1Local(),
		Seen:     dedupe.New(),
		Adapters: adapters.NewStore(),
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
		Buffer:   evb,
		Seen:     dedupe.New(),
		Adapters: adapters.NewStore(),
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
		Buffer:   buffer.NewP1Local(),
		Seen:     dedupe.New(),
		Adapters: adapters.NewStore(),
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

func TestControlPipeline(t *testing.T) {
	srv := newTestServer(t, httpserver.Deps{
		Buffer:   buffer.NewP1Local(),
		Seen:     dedupe.New(),
		Adapters: adapters.NewStore(),
	})
	defer srv.Close()
	res, err := http.Get(srv.URL + "/v1/control/pipeline")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body struct {
		ContractVersion string `json:"contractVersion"`
		Stages          []struct {
			Ordinal int    `json:"ordinal"`
			Key     string `json:"key"`
			Title   string `json:"title"`
		} `json:"stages"`
		AdapterInstances []struct {
			ID          string `json:"id"`
			StageKey    string `json:"stageKey"`
			DisplayName string `json:"displayName"`
			CatalogTier string `json:"catalogTier"`
		} `json:"adapterInstances"`
	}
	_ = json.NewDecoder(res.Body).Decode(&body)
	if body.ContractVersion != "0.1.0" {
		t.Fatal(body.ContractVersion)
	}
	if len(body.Stages) != 8 || body.Stages[0].Key != "source" || body.Stages[7].Key != "sink_connector" {
		t.Fatal(body.Stages)
	}
	tiers := map[string]bool{}
	for _, a := range body.AdapterInstances {
		tiers[a.CatalogTier] = true
	}
	if !tiers["tier-1"] || !tiers["tier-2"] || !tiers["tier-3"] {
		t.Fatal(tiers)
	}
}

func TestCanalSegments(t *testing.T) {
	srv := newTestServer(t, httpserver.Deps{
		Buffer:   buffer.NewP1Local(),
		Seen:     dedupe.New(),
		Adapters: adapters.NewStore(),
	})
	defer srv.Close()
	res, err := http.Get(srv.URL + "/v1/control/canal/segments")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body struct {
		Segments []struct {
			ID                  string `json:"id"`
			Kind                string `json:"kind"`
			FollowsStageOrdinal int    `json:"followsStageOrdinal"`
			ProviderProfile     string `json:"providerProfile"`
		} `json:"segments"`
	}
	_ = json.NewDecoder(res.Body).Decode(&body)
	if len(body.Segments) != 3 {
		t.Fatal(len(body.Segments))
	}
	for _, s := range body.Segments {
		if s.Kind != "buffer" || s.ProviderProfile != "p1-local" {
			t.Fatal(s)
		}
	}
}

func TestAdapterCRUD(t *testing.T) {
	store := adapters.NewStore()
	srv := newTestServer(t, httpserver.Deps{
		Buffer:   buffer.NewP1Local(),
		Seen:     dedupe.New(),
		Adapters: store,
	})
	defer srv.Close()
	bad, _ := http.Post(srv.URL+"/v1/adapter-instances", "application/json", strings.NewReader(`{"catalogAdapterId":"adapter.unknown"}`))
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatal(bad.StatusCode)
	}
	bad.Body.Close()
	create, _ := http.Post(srv.URL+"/v1/adapter-instances", "application/json",
		strings.NewReader(`{"catalogAdapterId":"adapter.placeholder.sink_connector","operatorLabel":" QA binding "}`))
	if create.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(create.Body)
		t.Fatalf("%d %s", create.StatusCode, body)
	}
	var rec struct {
		ID               string  `json:"id"`
		CatalogAdapterID string  `json:"catalogAdapterId"`
		CatalogTier      string  `json:"catalogTier"`
		StageKey         string  `json:"stageKey"`
		DisplayName      string  `json:"displayName"`
		OperatorLabel    *string `json:"operatorLabel"`
	}
	_ = json.NewDecoder(create.Body).Decode(&rec)
	create.Body.Close()
	if rec.CatalogAdapterID != "adapter.placeholder.sink_connector" || rec.CatalogTier != "tier-2" {
		t.Fatal(rec)
	}
	if rec.OperatorLabel == nil || *rec.OperatorLabel != "QA binding" {
		t.Fatal(rec.OperatorLabel)
	}
	list, _ := http.Get(srv.URL + "/v1/adapter-instances")
	var listBody struct {
		Items []struct {
			ID               string `json:"id"`
			CatalogAdapterID string `json:"catalogAdapterId"`
		} `json:"items"`
	}
	_ = json.NewDecoder(list.Body).Decode(&listBody)
	list.Body.Close()
	if len(listBody.Items) != 1 || listBody.Items[0].ID != rec.ID {
		t.Fatal(listBody)
	}
	patchPayload := `{"catalogAdapterId":"adapter.placeholder.community_sink","operatorLabel":null}`
	reqPatch, _ := http.NewRequest(http.MethodPatch, srv.URL+"/v1/adapter-instances/"+rec.ID, strings.NewReader(patchPayload))
	reqPatch.Header.Set("Content-Type", "application/json")
	patchRes, _ := http.DefaultClient.Do(reqPatch)
	if patchRes.StatusCode != http.StatusOK {
		t.Fatal(patchRes.StatusCode)
	}
	var patched struct {
		CatalogAdapterID string  `json:"catalogAdapterId"`
		CatalogTier      string  `json:"catalogTier"`
		OperatorLabel    *string `json:"operatorLabel"`
	}
	_ = json.NewDecoder(patchRes.Body).Decode(&patched)
	patchRes.Body.Close()
	if patched.CatalogAdapterID != "adapter.placeholder.community_sink" || patched.CatalogTier != "tier-3" || patched.OperatorLabel != nil {
		t.Fatal(patched)
	}
	reqDel, _ := http.NewRequest(http.MethodDelete, srv.URL+"/v1/adapter-instances/"+rec.ID, nil)
	delRes, _ := http.DefaultClient.Do(reqDel)
	if delRes.StatusCode != http.StatusNoContent {
		t.Fatal(delRes.StatusCode)
	}
	delRes.Body.Close()
	gone, _ := http.Get(srv.URL + "/v1/adapter-instances/" + rec.ID)
	if gone.StatusCode != http.StatusNotFound {
		t.Fatal(gone.StatusCode)
	}
	gone.Body.Close()
}

func TestPostEvents400(t *testing.T) {
	srv := newTestServer(t, httpserver.Deps{
		Buffer:   buffer.NewP1Local(),
		Seen:     dedupe.New(),
		Adapters: adapters.NewStore(),
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
