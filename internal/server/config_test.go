package server

import (
	"strings"
	"testing"
	"time"
)

// mapGetter is the test surface for loadConfig — a closure that emulates
// os.Getenv so tests do not touch the real process environment.
func mapGetter(env map[string]string) func(string) string {
	return func(k string) string { return env[k] }
}

func baseEnv() map[string]string {
	// The minimum set of vars that produces a valid Config (non-dev, memory
	// driver, JWT secret + internal token both set). Individual tests
	// override exactly the keys they care about.
	return map[string]string{
		"NOTIFY_AUTH_JWT_SECRET": "super-secret-key",
		"NOTIFY_INTERNAL_TOKEN":  "internal-shared-token",
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := loadConfig(mapGetter(baseEnv()))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.ClientPort != DefaultClientPort {
		t.Errorf("ClientPort = %d, want %d", cfg.ClientPort, DefaultClientPort)
	}
	if cfg.InternalPort != DefaultInternalPort {
		t.Errorf("InternalPort = %d, want %d", cfg.InternalPort, DefaultInternalPort)
	}
	if cfg.MetricsPort != DefaultMetricsPort {
		t.Errorf("MetricsPort = %d, want %d", cfg.MetricsPort, DefaultMetricsPort)
	}
	if cfg.Store.Driver != "memory" {
		t.Errorf("Store.Driver = %q, want %q", cfg.Store.Driver, "memory")
	}
	if cfg.LogLevel != DefaultLogLevel {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, DefaultLogLevel)
	}
	if !cfg.LiveConnections.Enabled {
		t.Errorf("LiveConnections.Enabled = false, want true")
	}
	if cfg.LiveConnections.HeartbeatInterval != DefaultHeartbeatInterval {
		t.Errorf("HeartbeatInterval = %v, want %v", cfg.LiveConnections.HeartbeatInterval, DefaultHeartbeatInterval)
	}
	if cfg.Auth.JWTLeeway != DefaultJWTLeeway {
		t.Errorf("JWTLeeway = %v, want %v", cfg.Auth.JWTLeeway, DefaultJWTLeeway)
	}
	if cfg.ShutdownTimeout != DefaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %v, want %v", cfg.ShutdownTimeout, DefaultShutdownTimeout)
	}
}

func TestLoadConfig_PortValidation(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{"client port non-numeric", "NOTIFY_CLIENT_PORT", "abc", "NOTIFY_CLIENT_PORT"},
		{"client port too low", "NOTIFY_CLIENT_PORT", "0", "NOTIFY_CLIENT_PORT"},
		{"client port too high", "NOTIFY_CLIENT_PORT", "70000", "NOTIFY_CLIENT_PORT"},
		{"internal port non-numeric", "NOTIFY_INTERNAL_PORT", "x", "NOTIFY_INTERNAL_PORT"},
		{"metrics port too high", "NOTIFY_METRICS_PORT", "99999", "NOTIFY_METRICS_PORT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := baseEnv()
			env[tc.key] = tc.value
			_, err := loadConfig(mapGetter(env))
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadConfig_ValidPorts(t *testing.T) {
	env := baseEnv()
	env["NOTIFY_CLIENT_PORT"] = "9100"
	env["NOTIFY_INTERNAL_PORT"] = "9101"
	env["NOTIFY_METRICS_PORT"] = "9102"
	cfg, err := loadConfig(mapGetter(env))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.ClientPort != 9100 || cfg.InternalPort != 9101 || cfg.MetricsPort != 9102 {
		t.Fatalf("ports = (%d,%d,%d), want (9100,9101,9102)", cfg.ClientPort, cfg.InternalPort, cfg.MetricsPort)
	}
}

func TestLoadConfig_LogLevel(t *testing.T) {
	cases := []struct {
		input   string
		wantErr bool
	}{
		{"debug", false},
		{"INFO", false},
		{"warn", false},
		{"warning", false},
		{"error", false},
		{"", false},
		{"trace", true},
		{"bananas", true},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			env := baseEnv()
			if tc.input != "" {
				env["NOTIFY_LOG_LEVEL"] = tc.input
			}
			_, err := loadConfig(mapGetter(env))
			if tc.wantErr != (err != nil) {
				t.Fatalf("log level %q: err=%v wantErr=%v", tc.input, err, tc.wantErr)
			}
		})
	}
}

func TestLoadConfig_ShutdownTimeout(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_SHUTDOWN_TIMEOUT"] = "45s"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.ShutdownTimeout != 45*time.Second {
			t.Fatalf("ShutdownTimeout = %v, want 45s", cfg.ShutdownTimeout)
		}
	})
	t.Run("invalid duration", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_SHUTDOWN_TIMEOUT"] = "not-a-duration"
		if _, err := loadConfig(mapGetter(env)); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("must be positive", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_SHUTDOWN_TIMEOUT"] = "0s"
		if _, err := loadConfig(mapGetter(env)); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestLoadConfig_StoreDriver(t *testing.T) {
	t.Run("memory default", func(t *testing.T) {
		cfg, err := loadConfig(mapGetter(baseEnv()))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.Store.Driver != "memory" {
			t.Fatalf("driver = %q, want memory", cfg.Store.Driver)
		}
	})

	t.Run("postgres requires DSN", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_STORE_DRIVER"] = "postgres"
		_, err := loadConfig(mapGetter(env))
		if err == nil || !strings.Contains(err.Error(), "NOTIFY_POSTGRES_DSN") {
			t.Fatalf("err = %v, want NOTIFY_POSTGRES_DSN missing", err)
		}
	})

	t.Run("postgres with DSN parses automigrate default true", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_STORE_DRIVER"] = "postgres"
		env["NOTIFY_POSTGRES_DSN"] = "postgres://localhost/x"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if !cfg.Store.PostgresAutoMigrate {
			t.Fatal("PostgresAutoMigrate = false, want true by default")
		}
	})

	t.Run("postgres automigrate explicit false", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_STORE_DRIVER"] = "postgres"
		env["NOTIFY_POSTGRES_DSN"] = "postgres://localhost/x"
		env["NOTIFY_POSTGRES_AUTOMIGRATE"] = "false"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.Store.PostgresAutoMigrate {
			t.Fatal("PostgresAutoMigrate = true, want false")
		}
	})

	t.Run("entdb requires address + tenant", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_STORE_DRIVER"] = "entdb"
		_, err := loadConfig(mapGetter(env))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("entdb fully configured", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_STORE_DRIVER"] = "entdb"
		env["NOTIFY_ENTDB_ADDRESS"] = "localhost:50051"
		env["NOTIFY_ENTDB_TENANT_ID"] = "t1"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.Store.EntDBAddress != "localhost:50051" || cfg.Store.TenantID != "t1" {
			t.Fatalf("entdb cfg wrong: %+v", cfg.Store)
		}
	})

	t.Run("unknown driver", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_STORE_DRIVER"] = "magic"
		_, err := loadConfig(mapGetter(env))
		if err == nil || !strings.Contains(err.Error(), "unknown driver") {
			t.Fatalf("err = %v, want unknown driver", err)
		}
	})
}

func TestLoadConfig_AuthRequiredFields(t *testing.T) {
	t.Run("non-dev rejects missing jwt secret", func(t *testing.T) {
		env := baseEnv()
		delete(env, "NOTIFY_AUTH_JWT_SECRET")
		_, err := loadConfig(mapGetter(env))
		if err == nil || !strings.Contains(err.Error(), "NOTIFY_AUTH_JWT_SECRET") {
			t.Fatalf("err = %v, want JWT secret required", err)
		}
	})

	t.Run("non-dev rejects missing internal token", func(t *testing.T) {
		env := baseEnv()
		delete(env, "NOTIFY_INTERNAL_TOKEN")
		_, err := loadConfig(mapGetter(env))
		if err == nil || !strings.Contains(err.Error(), "NOTIFY_INTERNAL_TOKEN") {
			t.Fatalf("err = %v, want NOTIFY_INTERNAL_TOKEN required", err)
		}
	})

	t.Run("dev mode lets both be empty", func(t *testing.T) {
		env := map[string]string{"NOTIFY_AUTH_DEV_MODE": "true"}
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if !cfg.Auth.DevMode {
			t.Fatal("DevMode = false, want true")
		}
		if cfg.Auth.JWTSecret != "" || cfg.Auth.InternalToken != "" {
			t.Fatal("expected empty secrets in dev mode")
		}
	})

	t.Run("jwt leeway negative", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_AUTH_JWT_LEEWAY"] = "-5s"
		_, err := loadConfig(mapGetter(env))
		if err == nil {
			t.Fatal("expected error for negative leeway")
		}
	})

	t.Run("jwt leeway parsed", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_AUTH_JWT_LEEWAY"] = "90s"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.Auth.JWTLeeway != 90*time.Second {
			t.Fatalf("leeway = %v, want 90s", cfg.Auth.JWTLeeway)
		}
	})
}

func TestLoadConfig_LiveConnections(t *testing.T) {
	t.Run("disabled via env", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_LIVE_CONNECTIONS_ENABLED"] = "false"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.LiveConnections.Enabled {
			t.Fatal("Enabled = true, want false")
		}
	})

	t.Run("heartbeat parses", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_LIVE_HEARTBEAT_INTERVAL"] = "10s"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.LiveConnections.HeartbeatInterval != 10*time.Second {
			t.Fatalf("heartbeat = %v, want 10s", cfg.LiveConnections.HeartbeatInterval)
		}
	})

	t.Run("heartbeat zero rejected", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_LIVE_HEARTBEAT_INTERVAL"] = "0s"
		_, err := loadConfig(mapGetter(env))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("retry max negative rejected", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_LIVE_RETRY_MAX_ATTEMPTS"] = "-1"
		_, err := loadConfig(mapGetter(env))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("retry max zero ok", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_LIVE_RETRY_MAX_ATTEMPTS"] = "0"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.LiveConnections.RetryMaxAttempts != 0 {
			t.Fatalf("retry max = %d", cfg.LiveConnections.RetryMaxAttempts)
		}
	})

	t.Run("retry interval garbage rejected", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_LIVE_RETRY_INTERVAL"] = "no"
		_, err := loadConfig(mapGetter(env))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestLoadConfig_AllowedOrigins(t *testing.T) {
	env := baseEnv()
	env["NOTIFY_ALLOWED_ORIGINS"] = "https://app.example.com, https://www.example.com ,"
	cfg, err := loadConfig(mapGetter(env))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if len(cfg.LiveConnections.AllowedOrigins) != 2 {
		t.Fatalf("AllowedOrigins = %v, want 2 entries", cfg.LiveConnections.AllowedOrigins)
	}
	if cfg.LiveConnections.AllowedOrigins[0] != "https://app.example.com" {
		t.Fatalf("first origin = %q", cfg.LiveConnections.AllowedOrigins[0])
	}
}

func TestLoadConfig_EmailChannel(t *testing.T) {
	t.Run("none default disables channel", func(t *testing.T) {
		cfg, err := loadConfig(mapGetter(baseEnv()))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.Email.Provider != "none" {
			t.Fatalf("provider = %q, want none", cfg.Email.Provider)
		}
	})

	t.Run("emailservice requires address", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_EMAIL_PROVIDER"] = "emailservice"
		env["NOTIFY_EMAIL_FROM"] = "from@example.com"
		_, err := loadConfig(mapGetter(env))
		if err == nil || !strings.Contains(err.Error(), "NOTIFY_EMAIL_SERVICE_ADDRESS") {
			t.Fatalf("err = %v, want NOTIFY_EMAIL_SERVICE_ADDRESS required", err)
		}
	})

	t.Run("emailservice requires from", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_EMAIL_PROVIDER"] = "emailservice"
		env["NOTIFY_EMAIL_SERVICE_ADDRESS"] = "email:8080"
		_, err := loadConfig(mapGetter(env))
		if err == nil || !strings.Contains(err.Error(), "NOTIFY_EMAIL_FROM") {
			t.Fatalf("err = %v, want NOTIFY_EMAIL_FROM required", err)
		}
	})

	t.Run("emailservice configured", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_EMAIL_PROVIDER"] = "emailservice"
		env["NOTIFY_EMAIL_SERVICE_ADDRESS"] = "email:8080"
		env["NOTIFY_EMAIL_FROM"] = "no-reply@example.com"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.Email.EmailServiceAddress != "email:8080" {
			t.Fatalf("EmailServiceAddress = %q", cfg.Email.EmailServiceAddress)
		}
	})

	t.Run("smtp port invalid rejected", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_EMAIL_SMTP_PORT"] = "99999"
		_, err := loadConfig(mapGetter(env))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestLoadConfig_TwilioChannels(t *testing.T) {
	t.Run("sms twilio requires SID + token", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_SMS_PROVIDER"] = "twilio"
		_, err := loadConfig(mapGetter(env))
		if err == nil || !strings.Contains(err.Error(), "NOTIFY_SMS_ACCOUNT_SID") {
			t.Fatalf("err = %v, want NOTIFY_SMS_ACCOUNT_SID required", err)
		}
	})

	t.Run("sms twilio configured", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_SMS_PROVIDER"] = "twilio"
		env["NOTIFY_SMS_ACCOUNT_SID"] = "ACxxx"
		env["NOTIFY_SMS_AUTH_TOKEN"] = "tok"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.SMS.AccountSID != "ACxxx" {
			t.Fatalf("SMS.AccountSID = %q", cfg.SMS.AccountSID)
		}
	})

	t.Run("whatsapp twilio missing token rejected", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_WHATSAPP_PROVIDER"] = "twilio"
		env["NOTIFY_WHATSAPP_ACCOUNT_SID"] = "ACxxx"
		_, err := loadConfig(mapGetter(env))
		if err == nil {
			t.Fatal("expected error for whatsapp missing auth token")
		}
	})
}

func TestLoadConfig_WebPushAndMobile(t *testing.T) {
	t.Run("vapid requires both keys", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_WEBPUSH_PROVIDER"] = "vapid"
		env["NOTIFY_WEBPUSH_VAPID_PUBLIC"] = "pub"
		_, err := loadConfig(mapGetter(env))
		if err == nil {
			t.Fatal("expected error for missing VAPID private")
		}
	})

	t.Run("vapid fully configured", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_WEBPUSH_PROVIDER"] = "vapid"
		env["NOTIFY_WEBPUSH_VAPID_PUBLIC"] = "pub"
		env["NOTIFY_WEBPUSH_VAPID_PRIVATE"] = "priv"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.WebPush.VAPIDPublic != "pub" {
			t.Fatalf("VAPIDPublic = %q", cfg.WebPush.VAPIDPublic)
		}
	})

	t.Run("fcm requires creds + project", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_MOBILEPUSH_PROVIDER"] = "fcm"
		_, err := loadConfig(mapGetter(env))
		if err == nil {
			t.Fatal("expected error for missing FCM creds")
		}
	})

	t.Run("fcm configured", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_MOBILEPUSH_PROVIDER"] = "fcm"
		env["NOTIFY_FCM_CREDENTIALS_JSON"] = `{"type":"service_account"}`
		env["NOTIFY_FCM_PROJECT_ID"] = "p"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.MobilePush.FCMProjectID != "p" {
			t.Fatalf("FCMProjectID = %q", cfg.MobilePush.FCMProjectID)
		}
	})

	t.Run("apns sandbox toggle", func(t *testing.T) {
		env := baseEnv()
		env["NOTIFY_APNS_SANDBOX"] = "true"
		cfg, err := loadConfig(mapGetter(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if !cfg.MobilePush.APNSSandbox {
			t.Fatal("APNSSandbox = false, want true")
		}
	})
}

func TestSlogLevel(t *testing.T) {
	cases := []struct {
		level string
		want  string
	}{
		{"debug", "DEBUG"},
		{"info", "INFO"},
		{"warn", "WARN"},
		{"error", "ERROR"},
		{"", "INFO"},
	}
	for _, tc := range cases {
		t.Run(tc.level, func(t *testing.T) {
			cfg := Config{LogLevel: tc.level}
			got := cfg.SlogLevel().String()
			if got != tc.want {
				t.Fatalf("SlogLevel() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseBoolDefault(t *testing.T) {
	cases := []struct {
		v    string
		def  bool
		want bool
	}{
		{"", true, true},
		{"", false, false},
		{"true", false, true},
		{"FALSE", true, false},
		{"1", false, true},
		{"0", true, false},
		{"junk", true, true}, // default on parse error
		{"junk", false, false},
	}
	for _, tc := range cases {
		if got := parseBoolDefault(tc.v, tc.def); got != tc.want {
			t.Errorf("parseBoolDefault(%q, %v) = %v, want %v", tc.v, tc.def, got, tc.want)
		}
	}
}

func TestSplitList(t *testing.T) {
	cases := []struct {
		v    string
		want []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{" a , b , ", []string{"a", "b"}},
		{",,,", nil},
	}
	for _, tc := range cases {
		got := splitList(tc.v)
		if len(got) != len(tc.want) {
			t.Errorf("splitList(%q) length = %d, want %d", tc.v, len(got), len(tc.want))
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("splitList(%q)[%d] = %q, want %q", tc.v, i, got[i], tc.want[i])
			}
		}
	}
}
