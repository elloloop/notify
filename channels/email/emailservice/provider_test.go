package emailservice

import (
	"context"
	"errors"
	"testing"

	"github.com/elloloop/notify"
)

// fakeSender is a tiny in-file double — no external mocking library, per the
// CLAUDE.md "real tests" bar. Each field controls one knob of the contract,
// and gotIn captures the SendEmailInput so mapping assertions are direct.
type fakeSender struct {
	out    SendEmailOutput
	err    error
	gotIn  SendEmailInput
	called int
}

func (f *fakeSender) SendEmail(_ context.Context, in SendEmailInput) (SendEmailOutput, error) {
	f.called++
	f.gotIn = in
	return f.out, f.err
}

func TestNew_Validation(t *testing.T) {
	t.Run("nil sender is rejected", func(t *testing.T) {
		if _, err := New("emailservice", nil, "noreply@example.com"); err == nil {
			t.Fatal("expected error for nil sender, got nil")
		}
	})

	t.Run("empty from is rejected", func(t *testing.T) {
		if _, err := New("emailservice", &fakeSender{}, ""); err == nil {
			t.Fatal("expected error for empty from, got nil")
		}
	})

	t.Run("empty name defaults to emailservice", func(t *testing.T) {
		p, err := New("", &fakeSender{}, "noreply@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := p.Name(); got != "emailservice" {
			t.Fatalf("Name() = %q, want %q", got, "emailservice")
		}
	})

	t.Run("explicit name is preserved", func(t *testing.T) {
		p, err := New("ses", &fakeSender{}, "noreply@example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := p.Name(); got != "ses" {
			t.Fatalf("Name() = %q, want %q", got, "ses")
		}
	})
}

func TestProvider_Kind(t *testing.T) {
	p, err := New("emailservice", &fakeSender{}, "noreply@example.com")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if k := p.Kind(); k != notify.ChannelEmail {
		t.Fatalf("Kind() = %q, want %q", k, notify.ChannelEmail)
	}
}

func TestSend_HappyPath(t *testing.T) {
	fs := &fakeSender{out: SendEmailOutput{MessageID: "msg-123"}}
	p, err := New("emailservice", fs, "noreply@example.com")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	rcpt, err := p.Send(context.Background(), notify.Message{
		To:    "user@example.com",
		Title: "Hello",
		Body:  "Welcome aboard.",
	})
	if err != nil {
		t.Fatalf("Send: unexpected error: %v", err)
	}
	if rcpt.ProviderMessageID != "msg-123" {
		t.Fatalf("ProviderMessageID = %q, want %q", rcpt.ProviderMessageID, "msg-123")
	}
	if rcpt.Status != notify.StatusDelivered {
		t.Fatalf("Status = %q, want %q", rcpt.Status, notify.StatusDelivered)
	}
	if fs.called != 1 {
		t.Fatalf("Sender called %d times, want 1", fs.called)
	}

	// Mapping assertions — the only place this contract is enforced.
	if fs.gotIn.From != "noreply@example.com" {
		t.Errorf("From = %q, want %q", fs.gotIn.From, "noreply@example.com")
	}
	if fs.gotIn.To != "user@example.com" {
		t.Errorf("To = %q, want %q", fs.gotIn.To, "user@example.com")
	}
	if fs.gotIn.Subject != "Hello" {
		t.Errorf("Subject = %q, want %q", fs.gotIn.Subject, "Hello")
	}
	if fs.gotIn.BodyText != "Welcome aboard." {
		t.Errorf("BodyText = %q, want %q", fs.gotIn.BodyText, "Welcome aboard.")
	}
	if fs.gotIn.BodyHTML != "" {
		t.Errorf("BodyHTML = %q, want empty", fs.gotIn.BodyHTML)
	}
}

func TestSend_EmptyTo(t *testing.T) {
	fs := &fakeSender{}
	p, err := New("emailservice", fs, "noreply@example.com")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	rcpt, err := p.Send(context.Background(), notify.Message{
		To:    "",
		Title: "Hello",
		Body:  "Welcome.",
	})
	if err == nil {
		t.Fatal("expected error for empty To, got nil")
	}
	if rcpt.Status != notify.StatusFailed {
		t.Fatalf("Status = %q, want %q", rcpt.Status, notify.StatusFailed)
	}
	if rcpt.ProviderMessageID != "" {
		t.Errorf("ProviderMessageID = %q, want empty", rcpt.ProviderMessageID)
	}
	if fs.called != 0 {
		t.Fatalf("Sender called %d times on empty To, want 0", fs.called)
	}
}

func TestSend_SenderError(t *testing.T) {
	upstream := errors.New("smtp 421 try later")
	fs := &fakeSender{err: upstream}
	p, err := New("emailservice", fs, "noreply@example.com")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	rcpt, err := p.Send(context.Background(), notify.Message{
		To:    "user@example.com",
		Title: "Hello",
		Body:  "Welcome.",
	})
	if err == nil {
		t.Fatal("expected error from Sender, got nil")
	}
	if !errors.Is(err, upstream) {
		t.Fatalf("error chain does not wrap upstream: %v", err)
	}
	if rcpt.Status != notify.StatusFailed {
		t.Fatalf("Status = %q, want %q", rcpt.Status, notify.StatusFailed)
	}
	if rcpt.ProviderMessageID != "" {
		t.Errorf("ProviderMessageID = %q, want empty on failure", rcpt.ProviderMessageID)
	}
	if fs.called != 1 {
		t.Fatalf("Sender called %d times, want 1", fs.called)
	}
}

// TestProvider_ImplementsInterface is a compile-time assertion that Provider
// actually satisfies notify.Provider. If a future refactor breaks the
// signature, this fails at build time rather than at the consumer's wiring
// site.
func TestProvider_ImplementsInterface(t *testing.T) {
	var _ notify.Provider = (*Provider)(nil)
}
