// Package postgres is the Postgres-backed implementation of notify.Store.
//
// It is the dependable durable backend the platform falls back to when the
// tenant-shard-db driver is unavailable, and it is verified against the
// shared conformance suite in store/conformance — so its observable
// behaviour is identical to store/memory.
//
// Schema highlights:
//
//   - notify_notifications has UNIQUE (tenant_id, user_id, notification_id);
//     CreateNotification uses ON CONFLICT DO NOTHING for idempotency.
//   - notify_devices has UNIQUE (tenant_id, user_id, device_type); UpsertDevice
//     uses ON CONFLICT DO UPDATE to rotate the token in place.
//   - notify_notifications (tenant_id, user_id, created_at_ms DESC, id DESC)
//     index backs the cursor-walked pagination order.
//
// All composite keys use real Postgres column tuples — no concatenation —
// so separator-byte collisions and case-sensitivity work the same way they
// do in store/memory.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/elloloop/notify"
)

// Store is a notify.Store backed by Postgres.
type Store struct {
	pool *pgxpool.Pool
	cfg  Config
}

var _ notify.Store = (*Store)(nil)

// New opens a connection pool, optionally applies pending migrations and
// returns a *Store ready to use. The caller owns the pool's lifecycle and
// must call Close when finished.
func New(ctx context.Context, cfg Config) (*Store, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse DSN: %w", err)
	}
	poolCfg.MaxConns = cfg.MaxConns
	// MaxConnLifetime + MaxConnIdleTime mirror identity's settings, which
	// play nicely with upstream pgbouncer / RDS proxy idle timeouts.
	poolCfg.MaxConnLifetime = time.Hour
	poolCfg.MaxConnIdleTime = 30 * time.Minute
	if cfg.ConnTimeout > 0 {
		poolCfg.ConnConfig.ConnectTimeout = cfg.ConnTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: open pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	if cfg.AutoMigrate {
		if err := applyMigrations(ctx, pool); err != nil {
			pool.Close()
			return nil, err
		}
	}

	return &Store{pool: pool, cfg: cfg}, nil
}

// Close releases all pool resources. Safe to call on a nil receiver.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// TruncateAll clears every row from notify_notifications and notify_devices.
// Tests use it as a per-subtest reset so the conformance suite gets a clean
// state without spinning up a fresh container per subtest.
func (s *Store) TruncateAll(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE notify_notifications, notify_devices RESTART IDENTITY`)
	if err != nil {
		return fmt.Errorf("postgres: truncate: %w", err)
	}
	return nil
}

// CreateNotification inserts one per-user row, idempotent on
// (tenant_id, user_id, notification_id). ON CONFLICT DO NOTHING absorbs the
// duplicate; the follow-up SELECT recovers the existing id so the caller
// always learns it.
func (s *Store) CreateNotification(ctx context.Context, n *notify.Notification) (bool, error) {
	if n == nil {
		return false, errors.New("postgres: nil notification")
	}
	if n.ID == "" {
		n.ID = uuid.NewString()
	}

	const insertSQL = `
INSERT INTO notify_notifications (
    id, tenant_id, user_id, notification_id,
    subject_ref, subject_type, title, body,
    channel, status,
    created_at_ms, delivered_at_ms, ack_at_ms, read_at_ms
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7, $8,
    $9, $10,
    $11, $12, $13, $14
)
ON CONFLICT (tenant_id, user_id, notification_id) DO NOTHING
RETURNING id`

	var insertedID string
	err := s.pool.QueryRow(ctx, insertSQL,
		n.ID, n.TenantID, n.UserID, n.NotificationID,
		n.SubjectRef, n.SubjectType, n.Title, n.Body,
		string(n.Channel), string(n.Status),
		n.CreatedAtMS, n.DeliveredAtMS, n.AckAtMS, n.ReadAtMS,
	).Scan(&insertedID)
	switch {
	case err == nil:
		n.ID = insertedID
		return true, nil
	case errors.Is(err, pgx.ErrNoRows):
		// Row already existed for this idempotency key — recover its id.
		var existingID string
		if err := s.pool.QueryRow(ctx, `
SELECT id FROM notify_notifications
WHERE tenant_id = $1 AND user_id = $2 AND notification_id = $3`,
			n.TenantID, n.UserID, n.NotificationID,
		).Scan(&existingID); err != nil {
			return false, fmt.Errorf("postgres: select existing notification: %w", err)
		}
		n.ID = existingID
		return false, nil
	case isUniqueViolation(err):
		// Should be absorbed by ON CONFLICT, but a racing primary-key
		// collision on a caller-supplied ID would reach here.
		return false, fmt.Errorf("postgres: insert notification: %w", notify.ErrConflict)
	default:
		return false, fmt.Errorf("postgres: insert notification: %w", err)
	}
}

// GetNotification returns the row identified by id, scoped to (tenant, user).
func (s *Store) GetNotification(ctx context.Context, tenantID, userID, id string) (*notify.Notification, error) {
	const sel = `
SELECT id, tenant_id, user_id, notification_id,
       subject_ref, subject_type, title, body,
       channel, status,
       created_at_ms, delivered_at_ms, ack_at_ms, read_at_ms
FROM notify_notifications
WHERE tenant_id = $1 AND user_id = $2 AND id = $3`

	row := s.pool.QueryRow(ctx, sel, tenantID, userID, id)
	n, err := scanNotification(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, notify.ErrNotFound
		}
		// An invalid uuid for the id argument trips pgx's uuid encoder; in
		// that case there is definitively no row with that id, so we map
		// to ErrNotFound rather than surface a driver-internal error.
		if isInvalidUUID(err) {
			return nil, notify.ErrNotFound
		}
		return nil, fmt.Errorf("postgres: select notification: %w", err)
	}
	return n, nil
}

// UpdateStatus sets the new status and stamps the matching timestamp column
// in a single UPDATE. The CASE expressions leave the other timestamp
// columns untouched.
func (s *Store) UpdateStatus(ctx context.Context, tenantID, id string, status notify.DeliveryStatus, atMS int64) error {
	const upd = `
UPDATE notify_notifications
SET status         = $3,
    delivered_at_ms = CASE WHEN $3 = 'delivered' THEN $4 ELSE delivered_at_ms END,
    ack_at_ms       = CASE WHEN $3 = 'acked'     THEN $4 ELSE ack_at_ms       END,
    read_at_ms      = CASE WHEN $3 = 'read'      THEN $4 ELSE read_at_ms      END
WHERE tenant_id = $1 AND id = $2`

	tag, err := s.pool.Exec(ctx, upd, tenantID, id, string(status), atMS)
	if err != nil {
		if isInvalidUUID(err) {
			return notify.ErrNotFound
		}
		return fmt.Errorf("postgres: update status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return notify.ErrNotFound
	}
	return nil
}

// QueryUserNotifications returns one page of a user's notifications,
// newest-first, plus the total unread count across the whole user.
//
// Ordering is (created_at_ms DESC, id DESC) so a tiebreak on a duplicated
// timestamp is deterministic. The cursor encodes only created_at_ms (matches
// memory) and the next page uses created_at_ms < cursor (strict).
func (s *Store) QueryUserNotifications(ctx context.Context, q notify.Query) ([]*notify.Notification, string, int, error) {
	limit := notify.ClampLimit(q.Limit)

	// We fetch limit+1 rows so we can detect "there is at least one more page"
	// without a second SELECT count(*). If we get limit+1 rows, the cursor is
	// non-empty and points at the limit-th row.
	args := []any{q.TenantID, q.UserID}
	where := "tenant_id = $1 AND user_id = $2"

	if q.CursorMS != nil {
		args = append(args, *q.CursorMS)
		where += fmt.Sprintf(" AND created_at_ms < $%d", len(args))
	}
	if q.UnreadOnly {
		where += " AND status <> 'read'"
	}
	args = append(args, limit+1)
	limitPlaceholder := len(args)

	sel := fmt.Sprintf(`
SELECT id, tenant_id, user_id, notification_id,
       subject_ref, subject_type, title, body,
       channel, status,
       created_at_ms, delivered_at_ms, ack_at_ms, read_at_ms
FROM notify_notifications
WHERE %s
ORDER BY created_at_ms DESC, id DESC
LIMIT $%d`, where, limitPlaceholder)

	rows, err := s.pool.Query(ctx, sel, args...)
	if err != nil {
		return nil, "", 0, fmt.Errorf("postgres: query notifications: %w", err)
	}
	defer rows.Close()

	items := make([]*notify.Notification, 0, limit)
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, "", 0, fmt.Errorf("postgres: scan notification: %w", err)
		}
		items = append(items, n)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, fmt.Errorf("postgres: iterate notifications: %w", err)
	}

	nextCursor := ""
	if len(items) > limit {
		items = items[:limit]
		nextCursor = strconv.FormatInt(items[len(items)-1].CreatedAtMS, 10)
	}

	// Unread count is independent of the page filter — count every
	// not-yet-read row this user has.
	var unread int
	if err := s.pool.QueryRow(ctx, `
SELECT count(*) FROM notify_notifications
WHERE tenant_id = $1 AND user_id = $2 AND status <> 'read'`,
		q.TenantID, q.UserID,
	).Scan(&unread); err != nil {
		return nil, "", 0, fmt.Errorf("postgres: count unread: %w", err)
	}

	return items, nextCursor, unread, nil
}

// UpsertDevice inserts or updates a push registration keyed on
// (tenant_id, user_id, device_type). The conflict resolution rotates the
// token and last_active_ms in place; created_at_ms stays at its first-write
// value.
func (s *Store) UpsertDevice(ctx context.Context, d *notify.Device) (*notify.Device, error) {
	if d == nil {
		return nil, errors.New("postgres: nil device")
	}
	if d.ID == "" {
		d.ID = uuid.NewString()
	}

	const ups = `
INSERT INTO notify_devices (
    id, tenant_id, user_id, device_type, token, created_at_ms, last_active_ms
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
ON CONFLICT (tenant_id, user_id, device_type) DO UPDATE
SET token          = EXCLUDED.token,
    last_active_ms = EXCLUDED.last_active_ms
RETURNING id, tenant_id, user_id, device_type, token, created_at_ms, last_active_ms`

	row := s.pool.QueryRow(ctx, ups,
		d.ID, d.TenantID, d.UserID, d.DeviceType, d.Token, d.CreatedAtMS, d.LastActiveMS,
	)
	out, err := scanDevice(row)
	if err != nil {
		return nil, fmt.Errorf("postgres: upsert device: %w", err)
	}
	return out, nil
}

// ListDevices returns every registered device for a user, ordered by device
// type for deterministic output (matches store/memory).
func (s *Store) ListDevices(ctx context.Context, tenantID, userID string) ([]*notify.Device, error) {
	const sel = `
SELECT id, tenant_id, user_id, device_type, token, created_at_ms, last_active_ms
FROM notify_devices
WHERE tenant_id = $1 AND user_id = $2
ORDER BY device_type ASC`

	rows, err := s.pool.Query(ctx, sel, tenantID, userID)
	if err != nil {
		return nil, fmt.Errorf("postgres: list devices: %w", err)
	}
	defer rows.Close()

	var out []*notify.Device
	for rows.Next() {
		d, err := scanDevice(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan device: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: iterate devices: %w", err)
	}
	return out, nil
}

// rowScanner is the common surface implemented by both pgx.Row and pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanNotification(r rowScanner) (*notify.Notification, error) {
	var (
		n       notify.Notification
		channel string
		status  string
	)
	if err := r.Scan(
		&n.ID, &n.TenantID, &n.UserID, &n.NotificationID,
		&n.SubjectRef, &n.SubjectType, &n.Title, &n.Body,
		&channel, &status,
		&n.CreatedAtMS, &n.DeliveredAtMS, &n.AckAtMS, &n.ReadAtMS,
	); err != nil {
		return nil, err
	}
	n.Channel = notify.ChannelKind(channel)
	n.Status = notify.DeliveryStatus(status)
	return &n, nil
}

func scanDevice(r rowScanner) (*notify.Device, error) {
	var d notify.Device
	if err := r.Scan(
		&d.ID, &d.TenantID, &d.UserID, &d.DeviceType, &d.Token, &d.CreatedAtMS, &d.LastActiveMS,
	); err != nil {
		return nil, err
	}
	return &d, nil
}

// isInvalidUUID reports whether err is the pgx-side rejection of a string
// that fails uuid syntax. The Get/Update paths translate this to ErrNotFound
// rather than leaking a driver-internal error — there is, definitionally,
// no notification with that id.
func isInvalidUUID(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// pgx wraps with messages like:
	//   "cannot parse ... as uuid"
	//   "invalid input syntax for type uuid"
	for _, needle := range []string{
		"invalid input syntax for type uuid",
		"cannot parse",
		"invalid UUID",
		"invalid byte sequence",
	} {
		if containsFold(msg, needle) {
			return true
		}
	}
	return false
}

func containsFold(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i+len(substr) <= len(s); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			a := s[i+j]
			b := substr[j]
			if a >= 'A' && a <= 'Z' {
				a += 'a' - 'A'
			}
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
