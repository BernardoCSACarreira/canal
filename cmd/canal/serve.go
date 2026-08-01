package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/metrics"
)

// serveMetrics starts an HTTP listener exposing /metrics and returns a shutdown function.
//
// It is deliberately the smallest possible server: one route, no middleware, no dependencies. The
// scrape endpoint is an OPERATIONAL surface and not a control API — nothing here can change what the
// pipeline does, which is why it is safe to bind before the pipeline starts and to leave running
// while it drains.
//
// Binding happens SYNCHRONOUSLY so that a port already in use is an error the operator sees at
// start, next to the command they typed, rather than a log line they find later while wondering why
// nothing is being scraped.
func serveMetrics(addr string, reg *metrics.Registry, log *slog.Logger) (func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

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
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "canal exposes /metrics and nothing else", http.StatusNotFound)
	})

	srv := &http.Server{
		Handler: mux,
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
	log.Info("serving metrics", "addr", ln.Addr().String(), "path", "/metrics")

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}
