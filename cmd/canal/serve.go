package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/internal/metrics"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// statusSource hands the listener a pipeline once there is one.
//
// The listener binds BEFORE the build — see serveObservability — so for a short window there is
// nothing to report on. That window is exactly when a build against an unreachable upstream is slow
// and somebody is watching, so it answers 503 rather than pretending the pipeline exists.
type statusSource struct {
	p atomic.Pointer[engine.Pipeline]
}

func (s *statusSource) set(p *engine.Pipeline) { s.p.Store(p) }

// serveObservability starts an HTTP listener exposing /metrics and /status, and returns a shutdown
// function.
//
// It is deliberately the smallest possible server: two routes, no middleware, no dependencies. Both
// are OPERATIONAL surfaces and not a control API — nothing here can change what the pipeline does,
// which is why it is safe to bind before the pipeline starts and to leave running while it drains.
//
// Binding happens SYNCHRONOUSLY so that a port already in use is an error the operator sees at
// start, next to the command they typed, rather than a log line they find later while wondering why
// nothing is being scraped.
func serveObservability(addr string, reg *metrics.Registry, src *statusSource, log *slog.Logger) (func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	srv := &http.Server{
		Handler: observabilityMux(reg, src, log),
		// A scrape that hangs must not hold a connection forever, and a slow client must not be able
		// to accumulate them.
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("the metrics listener stopped", "error", err)
		}
	}()
	log.Info("serving observability", "addr", ln.Addr().String(), "paths", "/metrics /status")

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}

// observabilityMux is the routing, separated from the listener so a test can exercise the real
// handlers without binding a port and guessing at one.
func observabilityMux(reg *metrics.Registry, src *statusSource, log *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := reg.WriteText(w); err != nil {
			// The status line is already sent by the first write, so this cannot become a 500. It is
			// logged and the body is left short, which a scraper reports as a parse failure — a
			// visible symptom rather than a silently truncated set of series.
			log.Error("writing the metrics body", "error", err)
		}
	})

	// /status serves telemetry.PipelineStatus, the read model. One document, everything inlined:
	// there is nothing to follow up with and no second request to make.
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "canal's status endpoint is read-only", http.StatusMethodNotAllowed)
			return
		}
		p := src.p.Load()
		if p == nil {
			http.Error(w, "the pipeline has not been built yet", http.StatusServiceUnavailable)
			return
		}
		q, qerr := parseStatusQuery(r.URL.Query())
		if qerr != nil {
			http.Error(w, qerr.Error(), http.StatusBadRequest)
			return
		}
		st := p.Status(q)
		body, err := json.MarshalIndent(st, "", "  ")
		if err != nil {
			// Before the status line, so this one CAN be a 500 — and it means a field of the read model
			// stopped being marshallable, which is a bug and not a pipeline condition.
			log.Error("marshalling the status document", "error", err)
			http.Error(w, "the status document could not be rendered", http.StatusInternalServerError)
			return
		}
		// NO ETag, DELIBERATELY, although Version is documented as one. The document carries live
		// ages — canal_checkpoint_age_seconds is the primary alert signal and it climbs every second —
		// so it genuinely differs on every read and a conditional GET could never match. An ETag that
		// never matches costs a round trip and buys nothing; a 304 that DID match would serve a stale
		// age for the one field an operator is watching. The version is exposed as its own header,
		// where it is honest about being a cursor rather than a cache key.
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("X-Canal-Status-Version", strconv.FormatUint(st.Version, 10))
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(body)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "canal exposes /metrics and /status and nothing else", http.StatusNotFound)
	})
	return mux
}

// parseStatusQuery reads the selection parameters off the URL.
//
// THE QUERY IS IMPLEMENTED, not merely declared. telemetry.StatusQuery exists so the document's
// shape can absorb pagination later without a wire break — and a selector nothing honours is the
// declared-and-inert pattern that has produced most of this repository's defects, so /status honours
// it now against the one worker it has.
//
//	?stream=orders   only that stream's lanes
//	?limit=50        page size; 0 asks for none, which is what a health banner wants
//	?cursor=<opaque> continue from a previous response's lanesCursor
//	?config=1        include the redacted config tree, which is otherwise absent
//
// A BAD PARAMETER IS A 400, never a silent default. "limit=fifty" quietly becoming the default page
// is how an operator concludes the endpoint ignores them.
func parseStatusQuery(v url.Values) (telemetry.StatusQuery, error) {
	q := telemetry.StatusQuery{
		Stream:     record.StreamName(v.Get("stream")),
		LaneCursor: v.Get("cursor"),
	}
	// A PARAMETER THAT MEANS ONE THING. Accepting only 1 and 0 rather than every spelling of truth is
	// the same refusal as limit's: "config=yes" quietly reading as false is how an operator concludes
	// the endpoint ignores them, and there is no reading of the word that this endpoint should guess at.
	if raw := v.Get("config"); raw != "" {
		switch raw {
		case "1":
			q.IncludeConfig = true
		case "0":
		default:
			return q, fmt.Errorf("config must be 1 or 0, got %q", raw)
		}
	}
	if raw := v.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return q, fmt.Errorf("limit must be a non-negative integer, got %q", raw)
		}
		q.LaneLimit = &n
	}
	return q, nil
}
