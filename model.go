// Package notify is the core of the elloloop multi-channel notification
// platform: the domain types, the Store and Provider contracts, and the
// Notifier that orchestrates persistence + per-channel delivery.
//
// It is dependency-light on purpose — only interfaces and plain structs live
// here, so it can be imported in-process as a library. Concrete backends
// (store/postgres, store/entdb, channels/twilio, …) live in sub-packages and
// are injected at construction time. The standalone container in cmd/notifyd is
// a thin wrapper that wires the same pieces from configuration.
package notify

// ChannelKind identifies a delivery transport. A notification is delivered over
// one or more channels; each channel is backed by a configurable Provider.
type ChannelKind string

const (
	// ChannelInApp is the built-in real-time (SSE) channel served by package
	// realtime. It is the only channel that maintains a live connection.
	ChannelInApp ChannelKind = "in_app"
	// ChannelEmail delivers via an email Provider (emailservice, SES, ACS, …).
	ChannelEmail ChannelKind = "email"
	// ChannelWebPush delivers via the Web Push API (VAPID).
	ChannelWebPush ChannelKind = "web_push"
	// ChannelMobilePush delivers to mobile devices (FCM, APNS).
	ChannelMobilePush ChannelKind = "mobile_push"
	// ChannelSMS delivers a text message (Twilio, SNS, ACS).
	ChannelSMS ChannelKind = "sms"
	// ChannelWhatsApp delivers a WhatsApp message (Twilio, Meta Cloud API).
	ChannelWhatsApp ChannelKind = "whatsapp"
)

// DeliveryStatus is the per-recipient lifecycle of a notification.
type DeliveryStatus string

const (
	// StatusPending is the initial state: stored but not yet delivered.
	StatusPending DeliveryStatus = "pending"
	// StatusDelivered means a provider accepted the message.
	StatusDelivered DeliveryStatus = "delivered"
	// StatusAcked means the client confirmed receipt.
	StatusAcked DeliveryStatus = "acked"
	// StatusRead means the user opened/read it.
	StatusRead DeliveryStatus = "read"
	// StatusFailed means a provider rejected the send.
	StatusFailed DeliveryStatus = "failed"
)

// Notification is one recipient's copy of a notification. Delivery and read
// state are per-user: a single logical notification fanned out to N recipients
// becomes N Notification rows, each with an independent lifecycle.
type Notification struct {
	// ID is assigned by the Store on create.
	ID string
	// NotificationID is the caller-provided idempotency key. The triple
	// (TenantID, UserID, NotificationID) is unique.
	NotificationID string

	TenantID string
	UserID   string

	// SubjectRef is an opaque, caller-defined reference to whatever the
	// notification is about (a task, a message, an invoice…). The platform
	// never interprets it — it is stored and echoed back verbatim. SubjectType
	// is an optional discriminator the caller may set alongside it.
	SubjectRef  string
	SubjectType string

	Title string
	Body  string

	Channel ChannelKind
	Status  DeliveryStatus

	CreatedAtMS   int64
	DeliveredAtMS int64
	AckAtMS       int64
	ReadAtMS      int64
}

// Device is a registered push endpoint for one user on one device type. The
// triple (TenantID, UserID, DeviceType) is unique; re-registering rotates the
// token in place rather than creating a new row.
type Device struct {
	ID       string
	TenantID string
	UserID   string

	// DeviceType is "browser", "android" or "ios".
	DeviceType string
	// Token is the provider-specific address: an FCM/APNS token, or a JSON Web
	// Push subscription.
	Token string

	CreatedAtMS  int64
	LastActiveMS int64
}
