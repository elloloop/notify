package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/elloloop/notify"
	"github.com/elloloop/notify/store/conformance"
	"github.com/elloloop/notify/store/postgres"
)

// sharedPGOnce + sharedPGDSN make the testcontainers Postgres container a
// per-test-binary singleton. The conformance suite calls Driver.New for every
// subtest (27+), and spinning up a fresh container per call would balloon a
// 10s run into multiple minutes. The Store's TruncateAll resets state between
// subtests, so a shared container is correct and dramatically faster.
var (
	sharedPGOnce sync.Once
	sharedPGDSN  string
	sharedPGErr  error
)

func startSharedPostgres(t *testing.T) string {
	t.Helper()
	sharedPGOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		pg, err := tcpg.Run(
			ctx,
			"postgres:16.13-alpine3.23",
			tcpg.WithDatabase("notify"),
			tcpg.WithUsername("notify"),
			tcpg.WithPassword("notify"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(60*time.Second),
			),
		)
		if err != nil {
			sharedPGErr = err
			return
		}
		dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			sharedPGErr = err
			return
		}
		sharedPGDSN = dsn
		// The container lives for the entire test binary run. testcontainers
		// will clean it up via reaper at process exit; we do not Terminate it
		// here because subsequent t.Run calls still need it. A best-effort
		// terminate is registered for clean local runs.
		t.Cleanup(func() {
			_ = pg.Terminate(context.Background())
		})
	})
	if sharedPGErr != nil {
		t.Fatalf("start postgres: %v", sharedPGErr)
	}
	return sharedPGDSN
}

// newConformanceStore returns a Store backed by the shared container with a
// freshly-truncated schema. It is the per-subtest Driver.New body.
func newConformanceStore(t *testing.T, dsn string) notify.Store {
	t.Helper()
	ctx := context.Background()
	s, err := postgres.New(ctx, postgres.Config{
		DSN:         dsn,
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	if err := s.TruncateAll(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() {
		s.Close()
	})
	return s
}

// TestConformance runs the shared notify.Store contract suite against the
// Postgres driver. It is the differential check that the Postgres backend
// behaves identically to store/memory.
func TestConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker for testcontainers Postgres")
	}
	dsn := startSharedPostgres(t)

	conformance.RunConformance(t, conformance.Driver{
		Name: "postgres",
		New: func(t *testing.T) notify.Store {
			return newConformanceStore(t, dsn)
		},
	})
}
