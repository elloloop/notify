package notify

import "context"

// Message is a single rendered notification handed to a Provider for delivery
// to one recipient address over one channel.
type Message struct {
	// Notification is the persisted per-user row this delivery corresponds to
	// (nil for fire-and-forget sends that are not stored).
	Notification *Notification

	// To is the channel-specific destination: an email address, an E.164 phone
	// number, a device token, or — for in-app — the recipient user id.
	To string

	Title string
	Body  string

	// Data carries channel-agnostic structured key/values (deep links, ids).
	Data map[string]string
}

// Receipt is a Provider's acknowledgement of a send attempt.
type Receipt struct {
	// ProviderMessageID is the upstream id (Twilio SID, FCM message name, …),
	// when the provider returns one.
	ProviderMessageID string
	// Status is the delivery status to persist. An empty Status is treated as
	// StatusDelivered.
	Status DeliveryStatus
}

// Provider delivers messages over exactly one ChannelKind. Each channel has
// pluggable providers (email: emailservice|ses|acs|smtp; sms/whatsapp: twilio;
// mobile push: fcm|apns; …) selected by configuration. A Provider that returns
// an error causes the notification to be recorded as StatusFailed.
type Provider interface {
	// Kind reports which channel this provider serves.
	Kind() ChannelKind
	// Name is the concrete backend's identifier, e.g. "twilio" or "fcm".
	Name() string
	// Send delivers one message and returns its receipt.
	Send(ctx context.Context, msg Message) (Receipt, error)
}

// ProviderRegistry maps each ChannelKind to the active Provider chosen by
// configuration. An unconfigured channel is simply absent — sends to it are
// stored (for catch-up) but not dispatched.
type ProviderRegistry struct {
	byKind map[ChannelKind]Provider
}

// NewProviderRegistry returns an empty registry.
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{byKind: make(map[ChannelKind]Provider)}
}

// Register sets the active provider for its channel, replacing any previous one.
func (r *ProviderRegistry) Register(p Provider) {
	r.byKind[p.Kind()] = p
}

// Get returns the provider for a channel and whether one is configured.
func (r *ProviderRegistry) Get(kind ChannelKind) (Provider, bool) {
	p, ok := r.byKind[kind]
	return p, ok
}

// Enabled reports the channels that currently have a provider.
func (r *ProviderRegistry) Enabled() []ChannelKind {
	kinds := make([]ChannelKind, 0, len(r.byKind))
	for k := range r.byKind {
		kinds = append(kinds, k)
	}
	return kinds
}
