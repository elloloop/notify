// Package emailservice implements the notify Email channel by delegating to a
// configurable email-sending backend. It defines a narrow Sender interface so
// consumers can wire whatever SendEmail RPC their stack exposes — the elloloop
// EmailService, AWS SES, an in-house SMTP relay — without this package taking a
// dependency on any specific email proto.
//
// The provider is intentionally tiny: it is a translator between
// notify.Message (channel-agnostic) and SendEmailInput (the seam), plus a
// receipt mapping. All transport, retries, TLS, and provider auth belong to
// the Sender implementation supplied by the caller.
package emailservice

import (
	"context"
	"errors"
	"fmt"

	"github.com/elloloop/notify"
)

// defaultName is the Provider.Name() value when the caller passes empty.
// Concrete backends (e.g. SES) should pass their own label so observability
// surfaces ("notify_send_total{provider=...}") line up with the actual
// transport in use.
const defaultName = "emailservice"

// Sender is the narrow contract this provider depends on. Implementations may
// be a thin shim over a generated EmailService Connect client, an AWS SES SDK
// call, or a raw SMTP submission — the provider doesn't care. Keeping the
// interface here (rather than importing it from a consumer) preserves the
// "no backwards dependency on consumers" rule from CLAUDE.md.
type Sender interface {
	SendEmail(ctx context.Context, in SendEmailInput) (SendEmailOutput, error)
}

// SendEmailInput is the data the provider hands to the Sender. At least one of
// BodyText / BodyHTML must be non-empty; validation lives in the Sender
// because the rendering decision (text-only vs multipart) is backend-specific.
type SendEmailInput struct {
	From     string
	To       string
	Subject  string
	BodyText string
	BodyHTML string
}

// SendEmailOutput is the Sender's acknowledgement. MessageID is the upstream
// id (SES Message-ID, EmailService SendEmailResponse.message_id, …) which we
// echo into notify.Receipt.ProviderMessageID for traceability.
type SendEmailOutput struct {
	MessageID string
}

// Provider implements notify.Provider for ChannelEmail. It is stateless apart
// from its dependencies; safe for concurrent use as long as the underlying
// Sender is.
type Provider struct {
	name   string
	sender Sender
	from   string
}

// New constructs a Provider.
//
//	name   — concrete-backend label returned by Provider.Name() (e.g.
//	         "emailservice", "ses"). Defaults to "emailservice" when empty so
//	         the common case is one argument shorter.
//	sender — the delegate that actually puts bytes on the wire. Required.
//	from   — default From address stamped on every outbound message. Required
//	         because most providers reject sends without an envelope sender,
//	         and notify.Message has no per-send From field.
func New(name string, sender Sender, from string) (*Provider, error) {
	if sender == nil {
		return nil, errors.New("emailservice: sender is required")
	}
	if from == "" {
		return nil, errors.New("emailservice: from address is required")
	}
	if name == "" {
		name = defaultName
	}
	return &Provider{name: name, sender: sender, from: from}, nil
}

// Kind reports the channel this provider serves.
func (p *Provider) Kind() notify.ChannelKind { return notify.ChannelEmail }

// Name is the configured backend label.
func (p *Provider) Name() string { return p.name }

// Send maps a notify.Message onto SendEmailInput and dispatches via the
// Sender. The mapping is deliberately simple:
//
//	From     = provider.from (notify.Message has no From)
//	To       = msg.To
//	Subject  = msg.Title
//	BodyText = msg.Body
//	BodyHTML = "" (HTML rendering is a future concern; callers that need
//	          it can wrap their Sender to template msg.Data into HTML).
//
// On empty msg.To we fail fast — every email transport rejects an empty
// recipient, and returning early gives the orchestrator a clean StatusFailed
// without a wasted RPC. On Sender error we wrap with %w so callers can
// errors.Is / errors.As against the underlying transport error.
func (p *Provider) Send(ctx context.Context, msg notify.Message) (notify.Receipt, error) {
	if msg.To == "" {
		return notify.Receipt{Status: notify.StatusFailed}, errors.New("emailservice: message To is empty")
	}

	out, err := p.sender.SendEmail(ctx, SendEmailInput{
		From:     p.from,
		To:       msg.To,
		Subject:  msg.Title,
		BodyText: msg.Body,
	})
	if err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("emailservice: send: %w", err)
	}

	return notify.Receipt{
		ProviderMessageID: out.MessageID,
		Status:            notify.StatusDelivered,
	}, nil
}
