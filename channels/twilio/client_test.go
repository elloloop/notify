package twilio

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/elloloop/notify"
)

func TestNewClient_Validation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		cfg     notify.TwilioConfig
		wantErr string
	}{
		{
			name:    "missing AccountSID",
			cfg:     notify.TwilioConfig{AuthToken: "tok", From: "+15551112222"},
			wantErr: "AccountSID is required",
		},
		{
			name:    "missing AuthToken",
			cfg:     notify.TwilioConfig{AccountSID: "AC123", From: "+15551112222"},
			wantErr: "AuthToken is required",
		},
		{
			name:    "missing both From and MessagingServiceSID",
			cfg:     notify.TwilioConfig{AccountSID: "AC123", AuthToken: "tok"},
			wantErr: "From or MessagingServiceSID is required",
		},
		{
			name:    "blank AccountSID (whitespace only)",
			cfg:     notify.TwilioConfig{AccountSID: "   ", AuthToken: "tok", From: "+15551112222"},
			wantErr: "AccountSID is required",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewClient(tc.cfg)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestNewClient_AcceptsFromOnly(t *testing.T) {
	t.Parallel()
	c, err := NewClient(notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "+15551112222",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	key, val := c.pickFrom()
	if key != "From" || val != "+15551112222" {
		t.Fatalf("pickFrom = (%q, %q), want (From, +15551112222)", key, val)
	}
}

func TestNewClient_PrefersMessagingService(t *testing.T) {
	t.Parallel()
	c, err := NewClient(notify.TwilioConfig{
		AccountSID:          "AC123",
		AuthToken:           "tok",
		From:                "+15551112222",
		MessagingServiceSID: "MGabc",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	key, val := c.pickFrom()
	if key != "MessagingServiceSid" || val != "MGabc" {
		t.Fatalf("pickFrom = (%q, %q), want (MessagingServiceSid, MGabc)", key, val)
	}
}

func TestBasicAuth_EncodesAccountSIDAndToken(t *testing.T) {
	t.Parallel()
	got := basicAuth("AC123", "secret")
	want := base64.StdEncoding.EncodeToString([]byte("AC123:secret"))
	if got != want {
		t.Fatalf("basicAuth = %q, want %q", got, want)
	}
}

func TestWithBaseURL_TrimsTrailingSlashAndCopies(t *testing.T) {
	t.Parallel()
	orig, err := NewClient(notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "+15551112222",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cp := orig.WithBaseURL("https://example.test/")
	if cp.baseURL != "https://example.test" {
		t.Fatalf("baseURL = %q, want %q", cp.baseURL, "https://example.test")
	}
	if orig.baseURL == cp.baseURL {
		t.Fatalf("WithBaseURL mutated original (both have %q)", orig.baseURL)
	}
}

func TestDefaultBaseURL(t *testing.T) {
	t.Parallel()
	c, err := NewClient(notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "+15551112222",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.baseURL != defaultBaseURL {
		t.Fatalf("baseURL = %q, want %q", c.baseURL, defaultBaseURL)
	}
	if defaultBaseURL != "https://api.twilio.com" {
		t.Fatalf("defaultBaseURL drift: %q", defaultBaseURL)
	}
}
