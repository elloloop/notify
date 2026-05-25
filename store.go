package notify

import (
	"context"
	"errors"
)

// Store is the durable persistence contract for the platform. Implementations
// live in sub-packages: store/memory (tests + dev), store/entdb
// (tenant-shard-db) and store/postgres. Every implementation is verified
// against the shared suite in store/conformance so the backends stay
// behaviourally identical.
//
// The live-connection registry and at-least-once retry state are in-memory
// (package realtime) and deliberately NOT part of this interface — the Store
// only holds durable data: notification history, device registrations and
// (later) per-user channel preferences.
type Store interface {
	// CreateNotification inserts one per-user notification row. It is
	// idempotent on (TenantID, UserID, NotificationID): a repeat call leaves
	// the stored row unchanged, sets n.ID to the existing row's id, and returns
	// created=false.
	CreateNotification(ctx context.Context, n *Notification) (created bool, err error)

	// GetNotification returns the row with the given store id, scoped to the
	// user. It returns ErrNotFound if no such row exists.
	GetNotification(ctx context.Context, tenantID, userID, id string) (*Notification, error)

	// UpdateStatus sets the delivery status and stamps the timestamp field that
	// matches the new status (delivered→DeliveredAtMS, acked→AckAtMS,
	// read→ReadAtMS). It returns ErrNotFound for an unknown id.
	UpdateStatus(ctx context.Context, tenantID, id string, status DeliveryStatus, atMS int64) error

	// QueryUserNotifications returns one page of a user's notifications,
	// newest-first. nextCursor is the opaque cursor for the following page
	// (empty on the last page); unreadCount is the user's total not-yet-read
	// count, independent of the page window and filters.
	QueryUserNotifications(ctx context.Context, q Query) (items []*Notification, nextCursor string, unreadCount int, err error)

	// UpsertDevice inserts or updates a push registration keyed on
	// (TenantID, UserID, DeviceType), returning the stored device.
	UpsertDevice(ctx context.Context, d *Device) (*Device, error)

	// ListDevices returns every registered device for a user, ordered by device
	// type for deterministic output.
	ListDevices(ctx context.Context, tenantID, userID string) ([]*Device, error)
}

// Query parameterises QueryUserNotifications.
type Query struct {
	TenantID string
	UserID   string
	// CursorMS, when non-nil, returns only rows strictly older than this
	// created-at (epoch ms) — the value encoded by the previous page's
	// nextCursor.
	CursorMS *int64
	// Limit caps the page size. Values <= 0 use DefaultPageLimit; values above
	// MaxPageLimit are clamped to it.
	Limit int
	// UnreadOnly restricts the page to notifications not yet read.
	UnreadOnly bool
}

// Page-size bounds shared by every Store implementation.
const (
	DefaultPageLimit = 20
	MaxPageLimit     = 100
)

// Sentinel errors returned across all Store implementations.
var (
	// ErrNotFound is returned when a lookup resolves to no row.
	ErrNotFound = errors.New("notify: not found")
	// ErrConflict is returned on a uniqueness violation not absorbed by
	// idempotency (e.g. a racing insert the caller must retry).
	ErrConflict = errors.New("notify: conflict")
)

// ClampLimit applies DefaultPageLimit / MaxPageLimit to a requested limit. Store
// implementations call it so paging bounds stay identical across drivers.
func ClampLimit(limit int) int {
	switch {
	case limit <= 0:
		return DefaultPageLimit
	case limit > MaxPageLimit:
		return MaxPageLimit
	default:
		return limit
	}
}
