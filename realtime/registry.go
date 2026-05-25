package realtime

import (
	"log/slog"
	"sync"
)

// Registry is the in-memory map userID → live connections used to push events
// to a user's active in-app streams. All operations are safe for concurrent
// use: reads take an RLock; Register/Unregister take a Lock; Push snapshots the
// slice under RLock and sends on each channel outside the lock.
type Registry[T any] struct {
	mu     sync.RWMutex
	byUser map[string][]*Conn[T]
	logger *slog.Logger
}

// NewRegistry returns an empty registry. A nil logger uses slog.Default().
func NewRegistry[T any](logger *slog.Logger) *Registry[T] {
	if logger == nil {
		logger = slog.Default()
	}
	return &Registry[T]{byUser: make(map[string][]*Conn[T]), logger: logger}
}

// Register adds a connection to its user's list.
func (r *Registry[T]) Register(c *Conn[T]) {
	r.mu.Lock()
	r.byUser[c.UserID] = append(r.byUser[c.UserID], c)
	r.mu.Unlock()
}

// Unregister removes the connection with the given id, if present.
func (r *Registry[T]) Unregister(connID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for userID, conns := range r.byUser {
		for i, c := range conns {
			if c.ID == connID {
				conns[i] = conns[len(conns)-1]
				conns[len(conns)-1] = nil
				r.byUser[userID] = conns[:len(conns)-1]
				if len(r.byUser[userID]) == 0 {
					delete(r.byUser, userID)
				}
				return
			}
		}
	}
}

// Connections returns a snapshot copy of a user's connection slice. The copy is
// safe to iterate without the lock; the *Conn pointers still alias the shared
// connection objects.
func (r *Registry[T]) Connections(userID string) []*Conn[T] {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conns := r.byUser[userID]
	if len(conns) == 0 {
		return nil
	}
	out := make([]*Conn[T], len(conns))
	copy(out, conns)
	return out
}

// Get returns the connection with the given id and whether it exists.
func (r *Registry[T]) Get(connID string) (*Conn[T], bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, conns := range r.byUser {
		for _, c := range conns {
			if c.ID == connID {
				return c, true
			}
		}
	}
	return nil, false
}

// Count returns the number of live connections for a user.
func (r *Registry[T]) Count(userID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byUser[userID])
}

// Push does a non-blocking send of ev to every active connection for the user,
// returning the number that accepted it. A connection whose buffer is full is
// skipped (logged) but stays registered.
func (r *Registry[T]) Push(userID string, ev T) int {
	pushed := 0
	for _, c := range r.Connections(userID) {
		if r.trySend(c, ev) {
			pushed++
		}
	}
	return pushed
}

// PushTo does a non-blocking send to one connection by id. It returns false if
// the connection is unknown or its buffer is full.
func (r *Registry[T]) PushTo(connID string, ev T) bool {
	c, ok := r.Get(connID)
	if !ok {
		return false
	}
	return r.trySend(c, ev)
}

func (r *Registry[T]) trySend(c *Conn[T], ev T) bool {
	select {
	case c.EventCh <- ev:
		return true
	default:
		r.logger.Warn("event_queue_full", "connection_id", c.ID, "user_id", c.UserID)
		return false
	}
}
