// Package realtime is the in-memory engine that maintains live client
// connections for the in-app channel: a per-user connection registry with
// non-blocking fan-out, plus an at-least-once retry tracker. It is generic over
// the event payload T, so it carries no dependency on the notification proto or
// any transport — the server adapts it to Connect/SSE by instantiating
// Registry[*pb.StreamEvent].
//
// Enabling/disabling this subsystem is a deployment choice (see
// notify.LiveConnectionsConfig): when disabled the service never constructs a
// Registry and holds no connections.
package realtime

import (
	"time"

	"github.com/google/uuid"
)

// DefaultEventBuffer is the per-connection queue depth. A stalled client whose
// buffer fills has further events dropped (see Registry.Push) rather than
// blocking the sender or growing memory without bound.
const DefaultEventBuffer = 64

// Conn is one active client connection tracked in a Registry. EventCh is
// buffered and never closed (closing would race concurrent non-blocking
// senders); once the connection is unregistered and its handler returns the
// channel is unreachable and reclaimed by the GC.
type Conn[T any] struct {
	ID         string
	UserID     string
	TenantID   string
	DeviceType string
	EventCh    chan T
	CreatedAt  time.Time
}

// NewConn builds a Conn with a fresh id and a buffered event channel of the
// given size (<=0 uses DefaultEventBuffer).
func NewConn[T any](userID, tenantID, deviceType string, buffer int) *Conn[T] {
	if buffer <= 0 {
		buffer = DefaultEventBuffer
	}
	return &Conn[T]{
		ID:         uuid.NewString(),
		UserID:     userID,
		TenantID:   tenantID,
		DeviceType: deviceType,
		EventCh:    make(chan T, buffer),
		CreatedAt:  time.Now(),
	}
}
