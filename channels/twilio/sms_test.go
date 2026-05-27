package twilio

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/elloloop/notify"
)

// newTestClient is the shared helper: spin up an httptest.Server with the
// supplied handler, build a Client wired to it. Returns both so tests can close
// the server when they're done.
func newTestClient(t *testing.T, cfg notify.TwilioConfig, h http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	c, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c.WithBaseURL(srv.URL), srv
}

func TestSMS_Send_HappyPath(t *testing.T) {
	t.Parallel()

	var capturedPath, capturedAuth, capturedCT, capturedBody string
	handler := func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedAuth = r.Header.Get("Authorization")
		capturedCT = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		capturedBody = string(body)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sid":"SM_test_sid_123","status":"queued"}`))
	}

	c, _ := newTestClient(t, notify.TwilioConfig{
		AccountSID: "ACdeadbeef",
		AuthToken:  "supersecret",
		From:       "+15551112222",
	}, handler)

	p := NewSMS(c)

	if p.Kind() != notify.ChannelSMS {
		t.Fatalf("Kind = %v, want %v", p.Kind(), notify.ChannelSMS)
	}
	if p.Name() != "twilio" {
		t.Fatalf("Name = %v, want twilio", p.Name())
	}

	receipt, err := p.Send(context.Background(), notify.Message{
		To:    "+12025551234",
		Title: "ignored when body present",
		Body:  "hello world",
	})
	if err != nil {
		t.Fatalf("Send: unexpected error: %v", err)
	}
	if receipt.ProviderMessageID != "SM_test_sid_123" {
		t.Fatalf("ProviderMessageID = %q, want SM_test_sid_123", receipt.ProviderMessageID)
	}
	if receipt.Status != notify.StatusDelivered {
		t.Fatalf("Status = %v, want %v", receipt.Status, notify.StatusDelivered)
	}

	wantPath := "/2010-04-01/Accounts/ACdeadbeef/Messages.json"
	if capturedPath != wantPath {
		t.Fatalf("path = %q, want %q", capturedPath, wantPath)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("ACdeadbeef:supersecret"))
	if capturedAuth != wantAuth {
		t.Fatalf("Authorization = %q, want %q", capturedAuth, wantAuth)
	}
	if capturedCT != "application/x-www-form-urlencoded" {
		t.Fatalf("Content-Type = %q, want application/x-www-form-urlencoded", capturedCT)
	}

	// Form body assertions: form values are URL-encoded.
	for _, frag := range []string{
		"To=%2B12025551234",
		"From=%2B15551112222",
		"Body=hello+world",
	} {
		if !strings.Contains(capturedBody, frag) {
			t.Fatalf("body %q missing fragment %q", capturedBody, frag)
		}
	}
}

func TestSMS_Send_TitleFallbackWhenBodyEmpty(t *testing.T) {
	t.Parallel()

	var capturedBody string
	c, _ := newTestClient(t, notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "+15551112222",
	}, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		capturedBody = string(b)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sid":"SMx","status":"queued"}`))
	})

	p := NewSMS(c)
	_, err := p.Send(context.Background(), notify.Message{
		To:    "+12025551234",
		Title: "fallback-title",
		Body:  "",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !strings.Contains(capturedBody, "Body=fallback-title") {
		t.Fatalf("body %q does not carry title fallback", capturedBody)
	}
}

func TestSMS_Send_PrefersMessagingServiceSID(t *testing.T) {
	t.Parallel()

	var capturedBody string
	c, _ := newTestClient(t, notify.TwilioConfig{
		AccountSID:          "AC123",
		AuthToken:           "tok",
		From:                "+15551112222",
		MessagingServiceSID: "MGabc",
	}, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		capturedBody = string(b)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sid":"SMx","status":"queued"}`))
	})

	p := NewSMS(c)
	_, err := p.Send(context.Background(), notify.Message{
		To:   "+12025551234",
		Body: "hi",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !strings.Contains(capturedBody, "MessagingServiceSid=MGabc") {
		t.Fatalf("body %q missing MessagingServiceSid", capturedBody)
	}
	if strings.Contains(capturedBody, "From=") {
		t.Fatalf("body %q must NOT carry From when MessagingServiceSid set", capturedBody)
	}
}

func TestSMS_Send_TwilioErrorWrapsCodeAndMessage(t *testing.T) {
	t.Parallel()

	c, _ := newTestClient(t, notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "+15551112222",
	}, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":21408,"message":"Permission to send an SMS has not been enabled for the region indicated by the 'To' number.","more_info":"https://www.twilio.com/docs/errors/21408","status":400}`))
	})

	p := NewSMS(c)
	receipt, err := p.Send(context.Background(), notify.Message{
		To:   "+12025551234",
		Body: "hi",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if receipt.Status != notify.StatusFailed {
		t.Fatalf("Status = %v, want %v", receipt.Status, notify.StatusFailed)
	}
	if receipt.ProviderMessageID != "" {
		t.Fatalf("ProviderMessageID should be empty on failure, got %q", receipt.ProviderMessageID)
	}
	if !strings.Contains(err.Error(), "21408") {
		t.Fatalf("error %q does not carry Twilio code", err.Error())
	}
	if !strings.Contains(err.Error(), "Permission to send an SMS") {
		t.Fatalf("error %q does not carry Twilio message", err.Error())
	}

	// Twilio error should be unwrappable to *twilioError.
	var terr *twilioError
	if !errors.As(err, &terr) {
		t.Fatalf("error %v is not unwrappable to *twilioError", err)
	}
	if terr.Code != 21408 {
		t.Fatalf("twilioError.Code = %d, want 21408", terr.Code)
	}
}

func TestSMS_Send_Non2xxWithoutJSONBody(t *testing.T) {
	t.Parallel()

	c, _ := newTestClient(t, notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "+15551112222",
	}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html>oops</html>"))
	})

	p := NewSMS(c)
	receipt, err := p.Send(context.Background(), notify.Message{
		To:   "+12025551234",
		Body: "hi",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if receipt.Status != notify.StatusFailed {
		t.Fatalf("Status = %v, want %v", receipt.Status, notify.StatusFailed)
	}
	if !strings.Contains(err.Error(), "502") {
		t.Fatalf("error %q does not carry status code", err.Error())
	}
}

func TestSMS_Send_RejectsMalformedRecipient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		to   string
	}{
		{"empty", ""},
		{"missing plus", "12025551234"},
		{"contains letters", "+1202abc1234"},
		{"too short", "+1234"},
		{"too long", "+1234567890123456"},
	}

	// Handler must NOT be invoked — assert via t.Fatal if it is.
	c, _ := newTestClient(t, notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "+15551112222",
	}, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("handler must not be hit for malformed To")
		w.WriteHeader(http.StatusOK)
	})

	p := NewSMS(c)
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			receipt, err := p.Send(context.Background(), notify.Message{To: tc.to, Body: "x"})
			if err == nil {
				t.Fatalf("expected validation error for To=%q", tc.to)
			}
			if receipt.Status != notify.StatusFailed {
				t.Fatalf("Status = %v, want %v", receipt.Status, notify.StatusFailed)
			}
		})
	}
}

func TestSMS_Send_NetworkErrorPropagates(t *testing.T) {
	t.Parallel()

	// Spin up and immediately close a server — connecting to it should fail.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()

	c, err := NewClient(notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "+15551112222",
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c = c.WithBaseURL(srv.URL)

	p := NewSMS(c)
	receipt, err := p.Send(context.Background(), notify.Message{
		To:   "+12025551234",
		Body: "hi",
	})
	if err == nil {
		t.Fatalf("expected network error, got nil")
	}
	if receipt.Status != notify.StatusFailed {
		t.Fatalf("Status = %v, want %v", receipt.Status, notify.StatusFailed)
	}
}
