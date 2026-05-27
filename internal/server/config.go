// Package server is the standalone-container wiring for elloloop/notify: the
// thing cmd/notifyd boots. It is intentionally a sub-package (not main) so the
// wiring is testable: handlers, middleware, config parsing, lifecycle, and the
// in-app provider all live here behind exported types that a test can construct
// with fakes.
//
// The package depends on the root notify package (for the contracts), the
// generated proto stubs (for the wire types), the in-memory realtime engine,
// and the store / channel sub-packages — exactly the surface a production
// deployment wires up. It never imports any consumer.
package server

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/elloloop/notify"
)

// Defaults for the standalone container. Picking them out as constants keeps
// the env-var parsing tests deterministic and gives operators one obvious
// place to look up "what does the service do when I leave X unset".
const (
	DefaultClientPort         = 8080
	DefaultInternalPort       = 8081
	DefaultMetricsPort        = 9090
	DefaultShutdownTimeout    = 30 * time.Second
	DefaultHeartbeatInterval  = 30 * time.Second
	DefaultRetryMaxAttempts   = 3
	DefaultRetryInterval      = 5 * time.Second
	DefaultStoreDriver        = "memory"
	DefaultJWTLeeway          = 30 * time.Second
	DefaultLogLevel           = "info"
	DefaultEventBufferPerConn = 64
)

// Config is the fully-resolved configuration for one server process. It is
// derived from the environment by LoadConfigFromEnv but Server.New accepts a
// Config directly so tests can stamp every field without touching os.Setenv.
type Config struct {
	// Ports.
	ClientPort   int // public Connect/HTTP/2 surface (NotificationClientService).
	InternalPort int // private gRPC surface (NotificationInternalService).
	MetricsPort  int // /metrics + /healthz on a separate listener.

	// Logging.
	LogLevel string

	// Lifecycle.
	ShutdownTimeout time.Duration

	// Store / providers / live-connections / auth — re-uses the same shapes
	// the root notify package already declares so the library and the
	// container speak the same dialect.
	Store           notify.StoreConfig
	Email           notify.EmailConfig
	SMS             notify.TwilioConfig
	WhatsApp        notify.TwilioConfig
	WebPush         notify.WebPushConfig
	MobilePush      notify.MobilePushConfig
	LiveConnections notify.LiveConnectionsConfig
	Auth            AuthConfig
}

// AuthConfig is the server-side superset of notify.AuthConfig. It carries the
// JWT validation parameters AND the internal-service shared secret + the
// dev-mode escape hatch — both of which only the container cares about.
type AuthConfig struct {
	// JWTSecret is the HS256 verification key. Required when DevMode=false.
	JWTSecret string
	// JWTIssuer optionally pins the expected `iss` claim. Empty = not checked.
	JWTIssuer string
	// JWTAudience optionally pins the expected `aud` claim. Empty = not checked.
	JWTAudience string
	// JWTLeeway is the allowed clock skew when validating exp / nbf / iat.
	JWTLeeway time.Duration

	// InternalToken is the static shared secret expected on the
	// X-Notify-Internal-Token header for NotificationInternalService calls.
	// Required when DevMode=false.
	InternalToken string

	// DevMode relaxes the boot-time validation rules: an unset JWT secret
	// accepts the trivial `Authorization: Bearer dev:<userid>:<tenant>` form,
	// and an unset InternalToken skips the internal-token check. Intended for
	// local development ONLY.
	DevMode bool
}

// LoadConfigFromEnv assembles a Config from process environment variables.
// Every variable has a documented default in NOTIFY_*-prefixed form; an
// invalid value (e.g. negative port, unknown store driver) fails fast at
// boot so production never silently runs misconfigured.
func LoadConfigFromEnv() (Config, error) {
	return loadConfig(os.Getenv)
}

// loadConfig is the testable seam — pass a func(string) string so unit tests
// can hand it a fixed table instead of touching the real process env.
func loadConfig(get func(string) string) (Config, error) {
	cfg := Config{
		ClientPort:      DefaultClientPort,
		InternalPort:    DefaultInternalPort,
		MetricsPort:     DefaultMetricsPort,
		LogLevel:        DefaultLogLevel,
		ShutdownTimeout: DefaultShutdownTimeout,
	}

	var firstErr error
	setErr := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}

	if v := get("NOTIFY_CLIENT_PORT"); v != "" {
		p, err := parsePort("NOTIFY_CLIENT_PORT", v)
		if err != nil {
			setErr(err)
		} else {
			cfg.ClientPort = p
		}
	}
	if v := get("NOTIFY_INTERNAL_PORT"); v != "" {
		p, err := parsePort("NOTIFY_INTERNAL_PORT", v)
		if err != nil {
			setErr(err)
		} else {
			cfg.InternalPort = p
		}
	}
	if v := get("NOTIFY_METRICS_PORT"); v != "" {
		p, err := parsePort("NOTIFY_METRICS_PORT", v)
		if err != nil {
			setErr(err)
		} else {
			cfg.MetricsPort = p
		}
	}

	if v := get("NOTIFY_LOG_LEVEL"); v != "" {
		cfg.LogLevel = strings.ToLower(v)
	}
	if _, err := parseLogLevel(cfg.LogLevel); err != nil {
		setErr(err)
	}

	if v := get("NOTIFY_SHUTDOWN_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			setErr(fmt.Errorf("NOTIFY_SHUTDOWN_TIMEOUT: %w", err))
		} else if d <= 0 {
			setErr(fmt.Errorf("NOTIFY_SHUTDOWN_TIMEOUT: must be > 0"))
		} else {
			cfg.ShutdownTimeout = d
		}
	}

	// Store driver block.
	cfg.Store.Driver = strings.ToLower(strOrDefault(get("NOTIFY_STORE_DRIVER"), DefaultStoreDriver))
	switch cfg.Store.Driver {
	case "memory":
	case "postgres":
		cfg.Store.PostgresDSN = get("NOTIFY_POSTGRES_DSN")
		if cfg.Store.PostgresDSN == "" {
			setErr(errors.New("NOTIFY_POSTGRES_DSN is required when NOTIFY_STORE_DRIVER=postgres"))
		}
		cfg.Store.PostgresAutoMigrate = parseBoolDefault(get("NOTIFY_POSTGRES_AUTOMIGRATE"), true)
	case "entdb":
		cfg.Store.EntDBAddress = get("NOTIFY_ENTDB_ADDRESS")
		cfg.Store.TenantID = get("NOTIFY_ENTDB_TENANT_ID")
		if cfg.Store.EntDBAddress == "" {
			setErr(errors.New("NOTIFY_ENTDB_ADDRESS is required when NOTIFY_STORE_DRIVER=entdb"))
		}
		if cfg.Store.TenantID == "" {
			setErr(errors.New("NOTIFY_ENTDB_TENANT_ID is required when NOTIFY_STORE_DRIVER=entdb"))
		}
	default:
		setErr(fmt.Errorf("NOTIFY_STORE_DRIVER: unknown driver %q (want memory|postgres|entdb)", cfg.Store.Driver))
	}

	// Auth.
	cfg.Auth.DevMode = parseBoolDefault(get("NOTIFY_AUTH_DEV_MODE"), false)
	cfg.Auth.JWTSecret = get("NOTIFY_AUTH_JWT_SECRET")
	cfg.Auth.JWTIssuer = get("NOTIFY_AUTH_JWT_ISSUER")
	cfg.Auth.JWTAudience = get("NOTIFY_AUTH_JWT_AUDIENCE")
	cfg.Auth.InternalToken = get("NOTIFY_INTERNAL_TOKEN")
	cfg.Auth.JWTLeeway = DefaultJWTLeeway
	if v := get("NOTIFY_AUTH_JWT_LEEWAY"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			setErr(fmt.Errorf("NOTIFY_AUTH_JWT_LEEWAY: %w", err))
		} else if d < 0 {
			setErr(fmt.Errorf("NOTIFY_AUTH_JWT_LEEWAY: must be >= 0"))
		} else {
			cfg.Auth.JWTLeeway = d
		}
	}
	if !cfg.Auth.DevMode {
		if cfg.Auth.JWTSecret == "" {
			setErr(errors.New("NOTIFY_AUTH_JWT_SECRET is required unless NOTIFY_AUTH_DEV_MODE=true"))
		}
		if cfg.Auth.InternalToken == "" {
			setErr(errors.New("NOTIFY_INTERNAL_TOKEN is required unless NOTIFY_AUTH_DEV_MODE=true"))
		}
	}
	// Mirror onto notify.AuthConfig (kept around for API surface symmetry).
	// Library callers using notify.Config in-process see the same JWT secret.
	cfg.LiveConnections.AllowedOrigins = splitList(get("NOTIFY_ALLOWED_ORIGINS"))

	// Live-connections (optional subsystem).
	cfg.LiveConnections.Enabled = parseBoolDefault(get("NOTIFY_LIVE_CONNECTIONS_ENABLED"), true)
	cfg.LiveConnections.HeartbeatInterval = DefaultHeartbeatInterval
	if v := get("NOTIFY_LIVE_HEARTBEAT_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			setErr(fmt.Errorf("NOTIFY_LIVE_HEARTBEAT_INTERVAL: %w", err))
		} else if d <= 0 {
			setErr(fmt.Errorf("NOTIFY_LIVE_HEARTBEAT_INTERVAL: must be > 0"))
		} else {
			cfg.LiveConnections.HeartbeatInterval = d
		}
	}
	cfg.LiveConnections.RetryMaxAttempts = DefaultRetryMaxAttempts
	if v := get("NOTIFY_LIVE_RETRY_MAX_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			setErr(fmt.Errorf("NOTIFY_LIVE_RETRY_MAX_ATTEMPTS: %w", err))
		} else if n < 0 {
			setErr(fmt.Errorf("NOTIFY_LIVE_RETRY_MAX_ATTEMPTS: must be >= 0"))
		} else {
			cfg.LiveConnections.RetryMaxAttempts = n
		}
	}
	cfg.LiveConnections.RetryInterval = DefaultRetryInterval
	if v := get("NOTIFY_LIVE_RETRY_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			setErr(fmt.Errorf("NOTIFY_LIVE_RETRY_INTERVAL: %w", err))
		} else if d <= 0 {
			setErr(fmt.Errorf("NOTIFY_LIVE_RETRY_INTERVAL: must be > 0"))
		} else {
			cfg.LiveConnections.RetryInterval = d
		}
	}

	// Email channel — provider is gated by NOTIFY_EMAIL_PROVIDER. "none"
	// (the default) leaves the channel disabled; "emailservice" wires the
	// elloloop EmailService Connect client; future providers slot in here.
	cfg.Email.Provider = strings.ToLower(strOrDefault(get("NOTIFY_EMAIL_PROVIDER"), "none"))
	cfg.Email.From = get("NOTIFY_EMAIL_FROM")
	cfg.Email.EmailServiceAddress = get("NOTIFY_EMAIL_SERVICE_ADDRESS")
	cfg.Email.SMTPHost = get("NOTIFY_EMAIL_SMTP_HOST")
	cfg.Email.SMTPUsername = get("NOTIFY_EMAIL_SMTP_USERNAME")
	cfg.Email.SMTPPassword = get("NOTIFY_EMAIL_SMTP_PASSWORD")
	cfg.Email.APIKey = get("NOTIFY_EMAIL_API_KEY")
	cfg.Email.Region = get("NOTIFY_EMAIL_REGION")
	cfg.Email.Endpoint = get("NOTIFY_EMAIL_ENDPOINT")
	if v := get("NOTIFY_EMAIL_SMTP_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p < 1 || p > 65535 {
			setErr(fmt.Errorf("NOTIFY_EMAIL_SMTP_PORT: must be in 1..65535"))
		} else {
			cfg.Email.SMTPPort = p
		}
	}
	if cfg.Email.Provider == "emailservice" && cfg.Email.EmailServiceAddress == "" {
		setErr(errors.New("NOTIFY_EMAIL_SERVICE_ADDRESS is required when NOTIFY_EMAIL_PROVIDER=emailservice"))
	}
	if cfg.Email.Provider != "none" && cfg.Email.From == "" {
		setErr(errors.New("NOTIFY_EMAIL_FROM is required when an email provider is configured"))
	}

	// Twilio (SMS + WhatsApp share one config shape). Loaded from a shared
	// block of env vars; SMS_PROVIDER / WHATSAPP_PROVIDER opts a channel in.
	twilioBlock := func(prefix string) notify.TwilioConfig {
		return notify.TwilioConfig{
			Provider:            strings.ToLower(get(prefix + "PROVIDER")),
			AccountSID:          get(prefix + "ACCOUNT_SID"),
			AuthToken:           get(prefix + "AUTH_TOKEN"),
			MessagingServiceSID: get(prefix + "MESSAGING_SERVICE_SID"),
			From:                get(prefix + "FROM"),
		}
	}
	cfg.SMS = twilioBlock("NOTIFY_SMS_")
	cfg.WhatsApp = twilioBlock("NOTIFY_WHATSAPP_")
	if cfg.SMS.Provider == "twilio" {
		if cfg.SMS.AccountSID == "" || cfg.SMS.AuthToken == "" {
			setErr(errors.New("NOTIFY_SMS_ACCOUNT_SID and NOTIFY_SMS_AUTH_TOKEN are required when NOTIFY_SMS_PROVIDER=twilio"))
		}
	}
	if cfg.WhatsApp.Provider == "twilio" {
		if cfg.WhatsApp.AccountSID == "" || cfg.WhatsApp.AuthToken == "" {
			setErr(errors.New("NOTIFY_WHATSAPP_ACCOUNT_SID and NOTIFY_WHATSAPP_AUTH_TOKEN are required when NOTIFY_WHATSAPP_PROVIDER=twilio"))
		}
	}

	// Web push (VAPID).
	cfg.WebPush.Provider = strings.ToLower(get("NOTIFY_WEBPUSH_PROVIDER"))
	cfg.WebPush.VAPIDPublic = get("NOTIFY_WEBPUSH_VAPID_PUBLIC")
	cfg.WebPush.VAPIDPrivate = get("NOTIFY_WEBPUSH_VAPID_PRIVATE")
	cfg.WebPush.ContactEmail = get("NOTIFY_WEBPUSH_CONTACT_EMAIL")
	if cfg.WebPush.Provider == "vapid" {
		if cfg.WebPush.VAPIDPublic == "" || cfg.WebPush.VAPIDPrivate == "" {
			setErr(errors.New("NOTIFY_WEBPUSH_VAPID_PUBLIC and NOTIFY_WEBPUSH_VAPID_PRIVATE are required when NOTIFY_WEBPUSH_PROVIDER=vapid"))
		}
	}

	// Mobile push.
	cfg.MobilePush.Provider = strings.ToLower(get("NOTIFY_MOBILEPUSH_PROVIDER"))
	cfg.MobilePush.FCMCredentialsJSON = get("NOTIFY_FCM_CREDENTIALS_JSON")
	cfg.MobilePush.FCMProjectID = get("NOTIFY_FCM_PROJECT_ID")
	cfg.MobilePush.APNSKeyP8 = get("NOTIFY_APNS_KEY_P8")
	cfg.MobilePush.APNSKeyID = get("NOTIFY_APNS_KEY_ID")
	cfg.MobilePush.APNSTeamID = get("NOTIFY_APNS_TEAM_ID")
	cfg.MobilePush.APNSTopic = get("NOTIFY_APNS_TOPIC")
	cfg.MobilePush.APNSSandbox = parseBoolDefault(get("NOTIFY_APNS_SANDBOX"), false)
	if cfg.MobilePush.Provider == "fcm" {
		if cfg.MobilePush.FCMCredentialsJSON == "" || cfg.MobilePush.FCMProjectID == "" {
			setErr(errors.New("NOTIFY_FCM_CREDENTIALS_JSON and NOTIFY_FCM_PROJECT_ID are required when NOTIFY_MOBILEPUSH_PROVIDER=fcm"))
		}
	}

	if firstErr != nil {
		return Config{}, firstErr
	}
	return cfg, nil
}

// LogLevel returns the resolved slog level for the config; the level string
// has already been validated by loadConfig so this never returns an error.
func (c Config) SlogLevel() slog.Level {
	lvl, _ := parseLogLevel(c.LogLevel)
	return lvl
}

func parsePort(name, v string) (int, error) {
	p, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	if p < 1 || p > 65535 {
		return 0, fmt.Errorf("%s: must be in 1..65535, got %d", name, p)
	}
	return p, nil
}

func parseLogLevel(v string) (slog.Level, error) {
	switch strings.ToLower(v) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("NOTIFY_LOG_LEVEL: unknown level %q (want debug|info|warn|error)", v)
	}
}

func parseBoolDefault(v string, def bool) bool {
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func strOrDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func splitList(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
