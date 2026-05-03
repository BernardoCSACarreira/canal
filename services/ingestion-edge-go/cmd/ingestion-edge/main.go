package main

import (
	"log"
	"net/http"
	"os"

	"canal.ingestion-edge-go/internal/adapters"
	"canal.ingestion-edge-go/internal/buffer"
	"canal.ingestion-edge-go/internal/dedupe"
	"canal.ingestion-edge-go/internal/httpserver"
)

func main() {
	addr := ":8080"
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		addr = v
	} else if v := os.Getenv("PORT"); v != "" {
		addr = ":" + v
	}
	d := httpserver.Deps{
		Buffer:   buffer.NewP1Local(),
		Seen:     dedupe.New(),
		Adapters: adapters.NewStore(),
	}
	srv := &http.Server{
		Addr:    addr,
		Handler: httpserver.NewHandler(d),
	}
	log.Printf("ingestion-edge-go listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
