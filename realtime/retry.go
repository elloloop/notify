package realtime

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// RetryCallback is invoked on each retry attempt; it should be idempotent per
// (key, connID). A non-nil error is logged and the loop continues to the next
// attempt.
type RetryCallback func(ctx context.Context, key, connID string) error

// RetryTracker delivers at-least-once by scheduling per-(key, connID) retries
// of an event send until the client acks or the attempt budget is exhausted.
//
//   - Track starts a goroutine per connection that retries on a fixed interval.
//   - Ack cancels the retry for a specific connection.
//   - CancelAll cancels every in-flight retry (shutdown).
//   - Once max attempts are reached the entry is evicted.
type RetryTracker struct {
	max      int
	interval time.Duration
	logger   *slog.Logger

	mu    sync.Mutex
	byKey map[string]map[string]*retryState
}

type retryState struct{ cancel context.CancelFunc }

// NewRetryTracker constructs a tracker. max<=0 disables retries (Track becomes a
// no-op). A nil logger uses slog.Default().
func NewRetryTracker(max int, interval time.Duration, logger *slog.Logger) *RetryTracker {
	if logger == nil {
		logger = slog.Default()
	}
	return &RetryTracker{
		max:      max,
		interval: interval,
		logger:   logger,
		byKey:    make(map[string]map[string]*retryState),
	}
}

// Track schedules retries of cb for each connID under key. Duplicate (key,
// connID) pairs are ignored. Canceling parent cancels every spawned retry.
func (t *RetryTracker) Track(parent context.Context, key string, connIDs []string, cb RetryCallback) {
	if t.max <= 0 || len(connIDs) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	states, ok := t.byKey[key]
	if !ok {
		states = make(map[string]*retryState)
		t.byKey[key] = states
	}
	for _, connID := range connIDs {
		if _, dup := states[connID]; dup {
			continue
		}
		ctx, cancel := context.WithCancel(parent)
		states[connID] = &retryState{cancel: cancel}
		go t.loop(ctx, key, connID, cb)
	}
}

// Ack cancels the retry for (key, connID) and removes the entry. Unknown keys
// or connections are ignored.
func (t *RetryTracker) Ack(key, connID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	states, ok := t.byKey[key]
	if !ok {
		return
	}
	st, ok := states[connID]
	if !ok {
		return
	}
	st.cancel()
	delete(states, connID)
	if len(states) == 0 {
		delete(t.byKey, key)
	}
}

// CancelAll cancels every in-flight retry. Safe to call multiple times.
func (t *RetryTracker) CancelAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, states := range t.byKey {
		for _, st := range states {
			st.cancel()
		}
	}
	t.byKey = make(map[string]map[string]*retryState)
}

func (t *RetryTracker) loop(ctx context.Context, key, connID string, cb RetryCallback) {
	timer := time.NewTimer(t.interval)
	defer timer.Stop()

	for attempt := 1; attempt <= t.max; attempt++ {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if err := cb(ctx, key, connID); err != nil {
			t.logger.Warn("retry_failed",
				"key", key, "connection_id", connID, "attempt", attempt, "error", err.Error())
		}
		if attempt < t.max {
			timer.Reset(t.interval)
		}
	}

	t.mu.Lock()
	if states, ok := t.byKey[key]; ok {
		delete(states, connID)
		if len(states) == 0 {
			delete(t.byKey, key)
		}
	}
	t.mu.Unlock()
}
