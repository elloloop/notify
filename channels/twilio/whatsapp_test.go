package twilio

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/elloloop/notify"
)

func TestWhatsApp_Send_PrependsPrefixOnFromAndTo(t *testing.T) {
	t.Parallel()

	var capturedBody string
	c, _ := newTestClient(t, notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "+14155550000",
	}, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		capturedBody = string(b)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sid":"SM_wa_sid","status":"queued"}`))
	})

	p := NewWhatsApp(c)
	if p.Kind() != notify.ChannelWhatsApp {
		t.Fatalf("Kind = %v, want %v", p.Kind(), notify.ChannelWhatsApp)
	}
	if p.Name() != "twilio" {
		t.Fatalf("Name = %v, want twilio", p.Name())
	}

	receipt, err := p.Send(context.Background(), notify.Message{
		To:   "+12025551234",
		Body: "hola",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if receipt.ProviderMessageID != "SM_wa_sid" {
		t.Fatalf("ProviderMessageID = %q, want SM_wa_sid", receipt.ProviderMessageID)
	}
	if receipt.Status != notify.StatusDelivered {
		t.Fatalf("Status = %v, want %v", receipt.Status, notify.StatusDelivered)
	}

	// Parse the form body and assert prefix presence; assertion via URL decoding
	// is more honest than substring matching ("+" → "%2B" + "whatsapp%3A").
	parsed, perr := url.ParseQuery(capturedBody)
	if perr != nil {
		t.Fatalf("ParseQuery: %v", perr)
	}
	gotFrom := parsed.Get("From")
	gotTo := parsed.Get("To")
	if gotFrom != "whatsapp:+14155550000" {
		t.Fatalf("From = %q, want whatsapp:+14155550000", gotFrom)
	}
	if gotTo != "whatsapp:+12025551234" {
		t.Fatalf("To = %q, want whatsapp:+12025551234", gotTo)
	}
	if !strings.HasPrefix(gotFrom, "whatsapp:") {
		t.Fatalf("From %q missing whatsapp: prefix", gotFrom)
	}
	if !strings.HasPrefix(gotTo, "whatsapp:") {
		t.Fatalf("To %q missing whatsapp: prefix", gotTo)
	}
}

func TestWhatsApp_Send_PreservesExistingPrefix(t *testing.T) {
	t.Parallel()

	var capturedBody string
	c, _ := newTestClient(t, notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "whatsapp:+14155550000",
	}, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		capturedBody = string(b)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sid":"SMx","status":"queued"}`))
	})

	p := NewWhatsApp(c)
	_, err := p.Send(context.Background(), notify.Message{
		To:   "whatsapp:+12025551234",
		Body: "hola",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	parsed, _ := url.ParseQuery(capturedBody)
	if parsed.Get("From") != "whatsapp:+14155550000" {
		t.Fatalf("From = %q, want whatsapp:+14155550000 (no double prefix)", parsed.Get("From"))
	}
	if parsed.Get("To") != "whatsapp:+12025551234" {
		t.Fatalf("To = %q, want whatsapp:+12025551234 (no double prefix)", parsed.Get("To"))
	}
}

func TestWhatsApp_Send_RejectsMalformedRecipient(t *testing.T) {
	t.Parallel()

	c, _ := newTestClient(t, notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "+14155550000",
	}, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("handler must not be hit for malformed To")
		w.WriteHeader(http.StatusOK)
	})

	p := NewWhatsApp(c)
	receipt, err := p.Send(context.Background(), notify.Message{
		To:   "whatsapp:not-a-number",
		Body: "hi",
	})
	if err == nil {
		t.Fatalf("expected validation error, got nil")
	}
	if receipt.Status != notify.StatusFailed {
		t.Fatalf("Status = %v, want %v", receipt.Status, notify.StatusFailed)
	}
}

func TestWhatsApp_Send_MessagingServiceNotPrefixed(t *testing.T) {
	t.Parallel()

	var capturedBody string
	c, _ := newTestClient(t, notify.TwilioConfig{
		AccountSID:          "AC123",
		AuthToken:           "tok",
		MessagingServiceSID: "MGabc",
	}, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		capturedBody = string(b)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sid":"SMx","status":"queued"}`))
	})

	p := NewWhatsApp(c)
	_, err := p.Send(context.Background(), notify.Message{
		To:   "+12025551234",
		Body: "hola",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	parsed, _ := url.ParseQuery(capturedBody)
	if parsed.Get("MessagingServiceSid") != "MGabc" {
		t.Fatalf("MessagingServiceSid = %q, want MGabc", parsed.Get("MessagingServiceSid"))
	}
	if parsed.Get("To") != "whatsapp:+12025551234" {
		t.Fatalf("To = %q, want whatsapp prefix", parsed.Get("To"))
	}
	// From must NOT be set when MessagingServiceSid is in use.
	if parsed.Get("From") != "" {
		t.Fatalf("From = %q, want empty when MessagingServiceSid set", parsed.Get("From"))
	}
}

func TestWhatsApp_Send_TwilioErrorReturnsFailed(t *testing.T) {
	t.Parallel()

	c, _ := newTestClient(t, notify.TwilioConfig{
		AccountSID: "AC123",
		AuthToken:  "tok",
		From:       "+14155550000",
	}, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":63007,"message":"Twilio could not find a Channel with the specified From address","status":400}`))
	})

	p := NewWhatsApp(c)
	receipt, err := p.Send(context.Background(), notify.Message{
		To:   "+12025551234",
		Body: "hola",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if receipt.Status != notify.StatusFailed {
		t.Fatalf("Status = %v, want %v", receipt.Status, notify.StatusFailed)
	}
	if !strings.Contains(err.Error(), "63007") {
		t.Fatalf("error %q does not carry Twilio code", err.Error())
	}
}
