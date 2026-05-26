//go:build realentdb

// Package-level build tag `realentdb` gates this test behind a tag so
// `go test ./...` does not require a running EntDB. CI invokes it via
// `go test -tags=realentdb ./store/entdb/...` with NOTIFY_ENTDB_ADDRESS
// pointing at a service container. The matching pattern lives in
// identity's realentdb_conformance_test.go.

package entdb_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/elloloop/tenant-shard-db/sdk/go/entdb"

	"github.com/elloloop/notify"
	"github.com/elloloop/notify/store/conformance"
	entdbstore "github.com/elloloop/notify/store/entdb"
)

// TestConformance runs the driver-agnostic notify.Store conformance suite
// against a real tenant-shard-db instance. NOTIFY_ENTDB_ADDRESS gates the
// test — when unset, the test skips so local-dev `go test ./...` stays
// green without booting EntDB.
//
// Each subtest gets a fresh tenant id (process-unique base + atomic
// counter) so state never leaks between subtests. The shared sdk.DbClient
// is reused because Connect is expensive and per-tenant isolation is
// sufficient.
func TestConformance(t *testing.T) {
	addr := os.Getenv("NOTIFY_ENTDB_ADDRESS")
	if addr == "" {
		t.Skip("NOTIFY_ENTDB_ADDRESS unset — skipping entdb conformance")
	}

	client, err := sdk.NewClient(addr)
	if err != nil {
		t.Fatalf("sdk.NewClient: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("sdk client Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	base := fmt.Sprintf("notify-conf-%d", time.Now().UnixNano())
	var seq int64

	conformance.RunConformance(t, conformance.Driver{
		Name: "entdb",
		New: func(t *testing.T) notify.Store {
			t.Helper()
			n := atomic.AddInt64(&seq, 1)
			tenantID := fmt.Sprintf("%s-%d", base, n)
			ensureTenant(t, client, tenantID)
			return entdbstore.New(client, tenantID)
		},
	})
}

// ensureTenant registers a tenant in the global registry before any
// tenant-scoped write. tenant-shard-db v1.12+ enforces explicit tenant
// registration — an unregistered tenant id returns NOT_FOUND on any
// write.
func ensureTenant(t *testing.T, client *sdk.DbClient, tenantID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin := client.Admin()
	if _, err := admin.CreateTenant(ctx, "system:notify", tenantID, tenantID); err != nil {
		// Treat ALREADY_EXISTS as success — the helper is idempotent
		// to keep test boot resilient across reruns.
		if !isAlreadyExists(err) {
			t.Fatalf("admin.CreateTenant(%q): %v", tenantID, err)
		}
	}
}

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	var entErr *sdk.EntDBError
	if errors.As(err, &entErr) && entErr.Code == "ALREADY_EXISTS" {
		return true
	}
	var uce *sdk.UniqueConstraintError
	return errors.As(err, &uce)
}
