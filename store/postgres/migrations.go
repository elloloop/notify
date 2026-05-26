package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migration is a single forward-only schema change. Migrations are applied
// in declaration order; once shipped, version numbers and SQL are immutable
// and new changes are appended at the end.
type migration struct {
	version int
	name    string
	sql     string
}

// migrations is the embedded schema bundle for the Postgres notify.Store.
// Keep migrations append-only: a new version means a new entry at the end,
// never edit a shipped one.
var migrations = []migration{
	{
		version: 1,
		name:    "init",
		sql: `
CREATE TABLE IF NOT EXISTS notify_notifications (
    id              uuid PRIMARY KEY,
    tenant_id       text   NOT NULL,
    user_id         text   NOT NULL,
    notification_id text   NOT NULL,
    subject_ref     text   NOT NULL DEFAULT '',
    subject_type    text   NOT NULL DEFAULT '',
    title           text   NOT NULL DEFAULT '',
    body            text   NOT NULL DEFAULT '',
    channel         text   NOT NULL DEFAULT '',
    status          text   NOT NULL,
    created_at_ms   bigint NOT NULL,
    delivered_at_ms bigint NOT NULL DEFAULT 0,
    ack_at_ms       bigint NOT NULL DEFAULT 0,
    read_at_ms      bigint NOT NULL DEFAULT 0,
    CONSTRAINT notify_notifications_tenant_user_nid_uq
        UNIQUE (tenant_id, user_id, notification_id)
);

CREATE INDEX IF NOT EXISTS notify_notifications_user_created
    ON notify_notifications (tenant_id, user_id, created_at_ms DESC, id DESC);

CREATE INDEX IF NOT EXISTS notify_notifications_user_unread
    ON notify_notifications (tenant_id, user_id)
    WHERE status <> 'read';

CREATE TABLE IF NOT EXISTS notify_devices (
    id             uuid PRIMARY KEY,
    tenant_id      text   NOT NULL,
    user_id        text   NOT NULL,
    device_type    text   NOT NULL,
    token          text   NOT NULL DEFAULT '',
    created_at_ms  bigint NOT NULL DEFAULT 0,
    last_active_ms bigint NOT NULL DEFAULT 0,
    CONSTRAINT notify_devices_tenant_user_type_uq
        UNIQUE (tenant_id, user_id, device_type)
);

CREATE INDEX IF NOT EXISTS notify_devices_user
    ON notify_devices (tenant_id, user_id, device_type);
`,
	},
}

// migrationAdvisoryLockKey is a stable arbitrary int64 used with
// pg_advisory_lock so concurrent processes that boot at the same time
// serialise their migration runs instead of racing.
const migrationAdvisoryLockKey int64 = 0x6E6F74_69667900 // "notify\0" in hex-ish

// applyMigrations brings the database up to the latest schema version.
// It is idempotent and safe to call concurrently from many processes —
// callers serialise via a session-level advisory lock and only run the
// statements whose version has not yet been recorded.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("postgres: acquire migration conn: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationAdvisoryLockKey); err != nil {
		return fmt.Errorf("postgres: acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrationAdvisoryLockKey)
	}()

	if _, err := conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS notify_schema_migrations (
    version    integer PRIMARY KEY,
    name       text    NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)`); err != nil {
		return fmt.Errorf("postgres: ensure schema_migrations: %w", err)
	}

	rows, err := conn.Query(ctx, `SELECT version FROM notify_schema_migrations`)
	if err != nil {
		return fmt.Errorf("postgres: read applied migrations: %w", err)
	}
	applied := make(map[int]struct{})
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return fmt.Errorf("postgres: scan applied migration: %w", err)
		}
		applied[v] = struct{}{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: iterate applied migrations: %w", err)
	}

	for _, m := range migrations {
		if _, ok := applied[m.version]; ok {
			continue
		}
		if _, err := conn.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("postgres: apply migration %d (%s): %w", m.version, m.name, err)
		}
		if _, err := conn.Exec(ctx,
			`INSERT INTO notify_schema_migrations (version, name) VALUES ($1, $2)`,
			m.version, m.name,
		); err != nil {
			return fmt.Errorf("postgres: record migration %d: %w", m.version, err)
		}
	}
	return nil
}
