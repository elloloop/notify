package notify

import "time"

// Config is the full configuration for the standalone container. In library
// mode callers construct providers and a Store directly and may ignore most of
// this. Each channel turns on only when its provider block is populated; an
// empty block leaves the channel disabled.
type Config struct {
	LiveConnections LiveConnectionsConfig
	Store           StoreConfig
	Auth            AuthConfig

	Email      EmailConfig
	SMS        TwilioConfig
	WhatsApp   TwilioConfig
	WebPush    WebPushConfig
	MobilePush MobilePushConfig
}

// LiveConnectionsConfig governs the in-app real-time (SSE) subsystem. When
// Enabled is false the service holds no client connections and in-app
// notifications are store-only (clients catch up via the history API).
type LiveConnectionsConfig struct {
	Enabled           bool
	HeartbeatInterval time.Duration
	RetryMaxAttempts  int
	RetryInterval     time.Duration
	AllowedOrigins    []string
}

// StoreConfig selects the durable backend.
type StoreConfig struct {
	// Driver is "entdb", "postgres" or "memory".
	Driver string

	// EntDB.
	EntDBAddress string
	TenantID     string

	// Postgres.
	PostgresDSN         string
	PostgresAutoMigrate bool
}

// AuthConfig configures how client JWTs are validated. The platform validates
// tokens directly against identity (or a shared secret) — it does not call back
// into any consuming application.
type AuthConfig struct {
	// IdentityAddress, when set, validates tokens via the identity service.
	IdentityAddress string
	// JWTSecret, when set, verifies HS256 tokens locally.
	JWTSecret string
}

// EmailConfig selects and configures the email provider.
type EmailConfig struct {
	// Provider is "emailservice", "ses", "acs", "smtp" or "sendgrid".
	Provider string
	From     string

	// EmailServiceAddress is the address of an elloloop/glassa EmailService
	// when Provider == "emailservice".
	EmailServiceAddress string

	// SMTP.
	SMTPHost     string
	SMTPPort     int
	SMTPUsername string
	SMTPPassword string

	// API-key providers (SES / SendGrid / ACS).
	APIKey   string
	Endpoint string
	Region   string
}

// TwilioConfig configures a Twilio-backed channel (SMS or WhatsApp). The same
// shape covers alternative providers via Provider.
type TwilioConfig struct {
	// Provider is "twilio" (default), "meta" (WhatsApp), "sns" or "acs" (SMS).
	Provider            string
	AccountSID          string
	AuthToken           string
	MessagingServiceSID string
	// From is the E.164 sender, or "whatsapp:+…" for WhatsApp.
	From string
}

// WebPushConfig holds VAPID keys for the Web Push channel.
type WebPushConfig struct {
	Provider     string // "vapid"
	VAPIDPublic  string
	VAPIDPrivate string
	ContactEmail string
}

// MobilePushConfig configures the mobile push channel.
type MobilePushConfig struct {
	Provider string // "fcm" | "apns" | "azure" | "aws"

	// FCM.
	FCMCredentialsJSON string
	FCMProjectID       string

	// APNS.
	APNSKeyP8   string
	APNSKeyID   string
	APNSTeamID  string
	APNSTopic   string
	APNSSandbox bool
}
