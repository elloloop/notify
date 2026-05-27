package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
)

// healthState is a tiny exported view of the service's liveness. /healthz
// returns 200 + a JSON body when ready, 503 when shutting down or not-ready.
// The atomic boolean keeps the handler lock-free on the hot path.
type healthState struct {
	ready atomic.Bool
}

func newHealthState() *healthState {
	return &healthState{}
}

func (h *healthState) markReady()    { h.ready.Store(true) }
func (h *healthState) markNotReady() { h.ready.Store(false) }

// ServeHTTP implements the /healthz handler. The body is intentionally JSON so
// container orchestrators that scrape it can read structured output.
func (h *healthState) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	body := map[string]string{"status": "ok"}
	if !h.ready.Load() {
		body["status"] = "not_ready"
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(body)
}

// newJSONLogger returns a slog.Logger writing JSON lines to stderr at the
// configured level. JSON is the production default — humans get jq, machines
// get structured ingestion. A nil writer falls back to os.Stderr.
func newJSONLogger(level slog.Level) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	return slog.New(handler)
}

// newMetricsMux returns a tiny mux that serves /healthz and a placeholder
// /metrics endpoint. We do NOT depend on prometheus/client_golang yet: the
// wave-2 ask is to expose the endpoint so deployment can scrape it without
// having to wait for a follow-up; the actual metric definitions belong to a
// later wave and slot in here without changing the mux shape.
func newMetricsMux(health *healthState) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/healthz", health)
	mux.Handle("/health", health) // alias for ops-tool muscle memory
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		// Placeholder until wave-3 wires the real Prometheus registry. A
		// scraper hitting this surface gets a parseable empty exposition
		// instead of a 404, which keeps the deployment plumbing honest.
		_, _ = w.Write([]byte("# notify metrics — placeholder\n"))
	})
	return mux
}
