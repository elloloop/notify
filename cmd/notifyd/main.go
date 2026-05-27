// Package main is the cmd/notifyd standalone container entry point.
//
// It is deliberately thin: parse environment configuration, build a
// *server.Server, run it. Everything that could grow logic — config parsing,
// handler wiring, lifecycle plumbing — lives in internal/server so it is
// testable in isolation.
//
// The one piece that does NOT live in internal/server is the construction of
// the Postgres / EntDB store drivers. Those are kept here so a library
// consumer that imports internal/server (or the server package via Build
// Constraints in the future) does NOT pull pgx + the EntDB SDK + the SDK's
// huge transitive closure into their binary. The cmd/notifyd binary always
// links them — every other binary is free to skip them.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	sdk "github.com/elloloop/tenant-shard-db/sdk/go/entdb/v2"

	"github.com/elloloop/notify"
	"github.com/elloloop/notify/internal/server"
	"github.com/elloloop/notify/store/entdb"
	"github.com/elloloop/notify/store/postgres"
)

// version is overridden at link time via -ldflags="-X main.version=...".
var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	if err := run(); err != nil {
		slog.Error("notifyd exited", "error", err.Error())
		os.Exit(1)
	}
}

// run is the deferable-friendly body. Returning an error from here is what
// keeps main() at 6 lines.
func run() error {
	cfg, err := server.LoadConfigFromEnv()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	deps, cleanup, err := buildDependencies(cfg)
	if err != nil {
		return fmt.Errorf("dependencies: %w", err)
	}
	defer cleanup()

	deps.Logger.Info("notifyd_starting",
		"version", version,
		"commit", commit,
		"store_driver", cfg.Store.Driver,
		"live_connections_enabled", cfg.LiveConnections.Enabled,
		"client_port", cfg.ClientPort,
		"internal_port", cfg.InternalPort,
		"metrics_port", cfg.MetricsPort,
	)

	s, err := server.NewWithDeps(cfg, deps)
	if err != nil {
		return fmt.Errorf("server: %w", err)
	}
	return s.Run(context.Background())
}

// buildDependencies returns a server.Dependencies populated for the configured
// store driver, plus a cleanup function the caller defers.
//
// The function returns a non-nil cleanup even on the error path so callers can
// always defer it — keeps run() shorter and removes a class of leak bugs.
func buildDependencies(cfg server.Config) (server.Dependencies, func(), error) {
	deps := server.Dependencies{
		Logger: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.SlogLevel()})),
	}
	cleanup := func() {}

	switch cfg.Store.Driver {
	case "memory":
		// memory.New() is wired inside server.New; nothing to do here.
		return deps, cleanup, nil

	case "postgres":
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		store, err := postgres.New(ctx, postgres.Config{
			DSN:         cfg.Store.PostgresDSN,
			AutoMigrate: cfg.Store.PostgresAutoMigrate,
		})
		if err != nil {
			return deps, cleanup, fmt.Errorf("postgres: %w", err)
		}
		deps.Store = store
		deps.StoreCloser = closeFuncStore(func() error { store.Close(); return nil })
		cleanup = func() { store.Close() }
		return deps, cleanup, nil

	case "entdb":
		client, err := sdk.NewClient(cfg.Store.EntDBAddress, sdk.WithSchema(entdb.SchemaMessages()...))
		if err != nil {
			return deps, cleanup, fmt.Errorf("entdb: NewClient: %w", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := client.Connect(ctx); err != nil {
			_ = client.Close()
			return deps, cleanup, fmt.Errorf("entdb: Connect: %w", err)
		}
		store := entdb.New(client, cfg.Store.TenantID)
		deps.Store = store
		deps.StoreCloser = closeFuncStore(func() error { return client.Close() })
		cleanup = func() { _ = client.Close() }
		return deps, cleanup, nil
	}

	return deps, cleanup, fmt.Errorf("unknown store driver %q", cfg.Store.Driver)
}

// closeFuncStore is a tiny adapter so we can return a function as a Closer.
type closeFuncStore func() error

func (f closeFuncStore) Close() error { return f() }

// _ ensures the notify import stays referenced after future refactors.
var _ = notify.ChannelInApp
