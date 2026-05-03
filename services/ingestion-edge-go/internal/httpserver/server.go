package httpserver

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"canal.ingestion-edge-go/internal/adapters"
	"canal.ingestion-edge-go/internal/buffer"
	"canal.ingestion-edge-go/internal/dedupe"
	"canal.ingestion-edge-go/internal/pipeline"
)

const version = "0.1.0"

type ingestBatchRequest struct {
	Source string               `json:"source"`
	Events []buffer.IngestEvent `json:"events"`
}

type Deps struct {
	Buffer   *buffer.P1LocalEventBuffer
	Seen     *dedupe.Seen
	Adapters *adapters.Store
}

func NewHandler(d Deps) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"service": "ingestion-edge-go",
			"version": version,
		})
	})
	mux.HandleFunc("GET /v1/stream", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotImplemented, map[string]string{
			"error":   "not_implemented",
			"message": "Streaming fan-out is not implemented in the Phase 1 scaffold; use POST /v1/events.",
		})
	})
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) {
		handleIngest(w, r, d)
	})
	mux.HandleFunc("GET /v1/control/pipeline", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, pipeline.PipelineSummary())
	})
	mux.HandleFunc("GET /v1/control/canal/segments", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, pipeline.CanalSegmentsRead())
	})
	mux.HandleFunc("GET /v1/adapter-instances", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": d.Adapters.List()})
	})
	mux.HandleFunc("GET /v1/adapter-instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		rec, ok := d.Adapters.Get(id)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "Adapter instance not found"})
			return
		}
		writeJSON(w, http.StatusOK, rec)
	})
	mux.HandleFunc("POST /v1/adapter-instances", func(w http.ResponseWriter, r *http.Request) {
		handleAdapterCreate(w, r, d.Adapters)
	})
	mux.HandleFunc("PATCH /v1/adapter-instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		handleAdapterPatch(w, r, d.Adapters, r.PathValue("id"))
	})
	mux.HandleFunc("DELETE /v1/adapter-instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		ok := d.Adapters.Delete(r.PathValue("id"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "Adapter instance not found"})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func handleIngest(w http.ResponseWriter, r *http.Request, d Deps) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<22))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "Could not read body"})
		return
	}
	var raw json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil || len(raw) == 0 || raw[0] != '{' {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "Body must be a JSON object"})
		return
	}
	var req ingestBatchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "Invalid JSON"})
		return
	}
	if msg := validateIngestBatch(&req); msg != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": msg})
		return
	}
	var duplicateIDs []string
	var accepted []buffer.IngestEvent
	for _, ev := range req.Events {
		if d.Seen.Check(ev.ID) {
			duplicateIDs = append(duplicateIDs, ev.ID)
			continue
		}
		accepted = append(accepted, ev)
	}
	if len(accepted) > 0 {
		d.Buffer.Append(req.Source, accepted)
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"accepted":     len(accepted),
		"duplicateIds": duplicateIDs,
	})
}

func validateIngestBatch(req *ingestBatchRequest) string {
	if len(req.Source) < 1 || len(req.Source) > 256 {
		return "`source` must be a non-empty string (max 256 chars)"
	}
	if len(req.Events) < 1 || len(req.Events) > 1000 {
		return "`events` must be an array with 1–1000 items"
	}
	for i, ev := range req.Events {
		if len(ev.ID) < 1 || len(ev.ID) > 128 {
			return "events[" + strconv.Itoa(i) + "].id invalid"
		}
		if len(ev.Type) < 1 || len(ev.Type) > 256 {
			return "events[" + strconv.Itoa(i) + "].type invalid"
		}
		if ev.OccurredAt == "" {
			return "events[" + strconv.Itoa(i) + "].occurredAt must be an ISO-8601 string"
		}
		if _, err := time.Parse(time.RFC3339, ev.OccurredAt); err != nil {
			if _, err2 := time.Parse(time.RFC3339Nano, ev.OccurredAt); err2 != nil {
				return "events[" + strconv.Itoa(i) + "].occurredAt is not a valid date-time"
			}
		}
	}
	return ""
}

func handleAdapterCreate(w http.ResponseWriter, r *http.Request, store *adapters.Store) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "Could not read body"})
		return
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil || len(raw) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "Body must be a JSON object"})
		return
	}
	catRaw, ok := raw["catalogAdapterId"]
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "`catalogAdapterId` must be a non-empty string"})
		return
	}
	var catalogID string
	if err := json.Unmarshal(catRaw, &catalogID); err != nil || catalogID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "`catalogAdapterId` must be a non-empty string"})
		return
	}
	var opLabel *string
	if lr, ok := raw["operatorLabel"]; ok {
		if string(lr) == "null" {
			opLabel = nil
		} else {
			var s string
			if err := json.Unmarshal(lr, &s); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "`operatorLabel` must be a string or null"})
				return
			}
			opLabel = &s
		}
	}
	rec, msg := store.Create(catalogID, opLabel)
	if msg != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": msg})
		return
	}
	writeJSON(w, http.StatusCreated, rec)
}

func handleAdapterPatch(w http.ResponseWriter, r *http.Request, store *adapters.Store, id string) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "Could not read body"})
		return
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "Body must be a JSON object"})
		return
	}
	var catalogPtr *string
	if v, ok := raw["catalogAdapterId"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil || s == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "`catalogAdapterId` must be a non-empty string when provided"})
			return
		}
		catalogPtr = &s
	}
	var labelPatch adapters.LabelPatch
	if v, ok := raw["operatorLabel"]; ok {
		labelPatch.Defined = true
		if string(v) == "null" {
			labelPatch.Value = nil
		} else {
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "`operatorLabel` must be a string or null"})
				return
			}
			labelPatch.Value = &s
		}
	}
	if catalogPtr == nil && !labelPatch.Defined {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": "Provide at least one of `catalogAdapterId`, `operatorLabel`"})
		return
	}
	rec, msg, found := store.Patch(id, catalogPtr, labelPatch)
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "Adapter instance not found"})
		return
	}
	if msg != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "validation_error", "message": msg})
		return
	}
	writeJSON(w, http.StatusOK, rec)
}
