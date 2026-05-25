package notify

import (
	"context"
	"time"
)

// Clock returns the current time; injectable for tests.
type Clock func() time.Time

// Notifier orchestrates persistence and per-channel delivery. It depends only
// on the Store and Provider interfaces, so the same logic runs whether it is
// embedded as a library or driven by the standalone container.
type Notifier struct {
	store     Store
	providers *ProviderRegistry
	now       Clock
}

// NotifierOption customises a Notifier.
type NotifierOption func(*Notifier)

// WithClock overrides the time source (tests).
func WithClock(c Clock) NotifierOption {
	return func(n *Notifier) { n.now = c }
}

// NewNotifier constructs a Notifier over the given store and provider registry.
// A nil registry is treated as empty (every channel store-only).
func NewNotifier(store Store, providers *ProviderRegistry, opts ...NotifierOption) *Notifier {
	n := &Notifier{store: store, providers: providers, now: time.Now}
	for _, opt := range opts {
		opt(n)
	}
	if n.providers == nil {
		n.providers = NewProviderRegistry()
	}
	return n
}

// NotifyRequest asks the platform to deliver one logical notification to a set
// of recipients over a set of channels.
type NotifyRequest struct {
	TenantID string
	// NotificationID is the caller idempotency key, shared by every recipient
	// row produced from this request.
	NotificationID string

	// UserIDs are the recipients; one stored Notification is produced per
	// (user, channel).
	UserIDs []string
	// Channels to attempt. Empty means every channel that has a provider.
	Channels []ChannelKind

	SubjectRef  string
	SubjectType string
	Title       string
	Body        string

	// Addresses optionally maps userID → channel → destination for channels
	// that need an explicit address the Store does not hold (email, phone). A
	// missing address means that channel is stored-only for that user. In-app
	// needs no address — the destination is the user id.
	Addresses map[string]map[ChannelKind]string
}

// NotifyResult summarises the fan-out.
type NotifyResult struct {
	// Delivered counts rows a provider accepted.
	Delivered int
	// Pending counts rows stored without an active/eligible provider.
	Pending int
	// Failed counts rows whose provider returned an error.
	Failed int
}

// Notify persists and dispatches the request. For each (user, channel) it
// stores an idempotent Notification row, then — if a provider is configured for
// the channel and a destination is available — attempts delivery and records
// the resulting status. Storage failures abort the call; per-channel delivery
// failures are counted and do not stop the fan-out.
func (n *Notifier) Notify(ctx context.Context, req NotifyRequest) (NotifyResult, error) {
	channels := req.Channels
	if len(channels) == 0 {
		channels = n.providers.Enabled()
	}

	var res NotifyResult
	for _, userID := range req.UserIDs {
		for _, kind := range channels {
			row := &Notification{
				NotificationID: req.NotificationID,
				TenantID:       req.TenantID,
				UserID:         userID,
				SubjectRef:     req.SubjectRef,
				SubjectType:    req.SubjectType,
				Title:          req.Title,
				Body:           req.Body,
				Channel:        kind,
				Status:         StatusPending,
				CreatedAtMS:    n.now().UnixMilli(),
			}
			if _, err := n.store.CreateNotification(ctx, row); err != nil {
				return res, err
			}

			provider, ok := n.providers.Get(kind)
			to := addressFor(req, userID, kind)
			if !ok || to == "" {
				res.Pending++
				continue
			}

			receipt, err := provider.Send(ctx, Message{
				Notification: row,
				To:           to,
				Title:        req.Title,
				Body:         req.Body,
			})
			if err != nil {
				res.Failed++
				_ = n.store.UpdateStatus(ctx, req.TenantID, row.ID, StatusFailed, n.now().UnixMilli())
				continue
			}
			status := receipt.Status
			if status == "" {
				status = StatusDelivered
			}
			if err := n.store.UpdateStatus(ctx, req.TenantID, row.ID, status, n.now().UnixMilli()); err != nil {
				return res, err
			}
			res.Delivered++
		}
	}
	return res, nil
}

// addressFor resolves the destination for one (user, channel). In-app always
// resolves to the user id; every other channel needs an explicit address.
func addressFor(req NotifyRequest, userID string, kind ChannelKind) string {
	if byChannel, ok := req.Addresses[userID]; ok {
		if to, ok := byChannel[kind]; ok {
			return to
		}
	}
	if kind == ChannelInApp {
		return userID
	}
	return ""
}
