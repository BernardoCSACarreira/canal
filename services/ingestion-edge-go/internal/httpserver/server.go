package httpserver

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"canal.ingestion-edge-go/internal/buffer"
	"canal.ingestion-edge-go/internal/dedupe"
)

const version = "0.1.0"

type ingestBatchRequest struct {
	Source string               `json:"source"`
	Events []buffer.IngestEvent `json:"events"`
}

// Deps holds data-plane-only dependencies (batch accept + dedupe + buffer).
// Control read models and adapter-instance CRUD live on ingestion-control-plane (Python).
type Deps struct {
	Buffer *buffer.P1LocalEventBuffer
	Seen   *dedupe.Seen
}

const splitStackControlHint = "Not served by ingestion-edge-go — use ingestion-control-plane for control and adapter-instance APIs in split-stack mode."

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
