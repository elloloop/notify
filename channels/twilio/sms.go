package twilio

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/elloloop/notify"
)

// SMSProvider implements notify.Provider for the SMS channel.
type SMSProvider struct {
	client *Client
}

// NewSMS returns an SMS provider backed by c. The same Client instance can be
// shared with NewWhatsApp; both providers are stateless beyond the client.
func NewSMS(c *Client) *SMSProvider {
	return &SMSProvider{client: c}
}

// Kind reports notify.ChannelSMS.
func (p *SMSProvider) Kind() notify.ChannelKind { return notify.ChannelSMS }

// Name reports the static identifier "twilio". Multiple Twilio-backed channels
// share the same name; the channel kind disambiguates them in logs.
func (p *SMSProvider) Name() string { return "twilio" }

// Send delivers one SMS via Twilio's Messages API.
//
// Validation: To must be non-empty and look like an E.164 number ("+" followed
// by digits). Anything else short-circuits to StatusFailed without an upstream
// call — Twilio would reject it anyway, and we'd rather not spend a request on
// a malformed input.
//
// Body precedence: msg.Body if non-empty, else msg.Title. SMS does not carry a
// separate subject line, so collapsing them keeps callers from having to know
// the wire shape.
func (p *SMSProvider) Send(ctx context.Context, msg notify.Message) (notify.Receipt, error) {
	to := strings.TrimSpace(msg.To)
	if err := validateE164(to); err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("twilio sms: %w", err)
	}

	form := url.Values{}
	form.Set("To", to)
	fromKey, fromVal := p.client.pickFrom()
	form.Set(fromKey, fromVal)
	form.Set("Body", pickBody(msg))

	resp, err := p.client.sendMessage(ctx, form)
	if err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("twilio sms: send: %w", err)
	}
	return notify.Receipt{
		ProviderMessageID: resp.SID,
		Status:            notify.StatusDelivered,
	}, nil
}

// validateE164 is a cheap structural check — not a full phone-number validator.
// It rejects the obvious garbage (empty, no leading +, non-digit body) before
// we burn a Twilio request on it. Twilio remains the authority on validity.
func validateE164(s string) error {
	if s == "" {
		return errors.New("To: empty recipient address")
	}
	if !strings.HasPrefix(s, "+") {
		return fmt.Errorf("To: %q must be E.164 (must start with '+')", s)
	}
	digits := s[1:]
	if len(digits) < 6 || len(digits) > 15 {
		return fmt.Errorf("To: %q has invalid length", s)
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return fmt.Errorf("To: %q contains non-digit characters", s)
		}
	}
	return nil
}

// pickBody returns the SMS body. Twilio requires a non-empty Body unless a
// messaging service template is in use; we don't model templates here, so a
// caller that supplies neither Body nor Title will produce an empty string and
// Twilio will reject it with code 21602 — that's the correct semantic and is
// surfaced to the caller via the standard error path.
func pickBody(msg notify.Message) string {
	if strings.TrimSpace(msg.Body) != "" {
		return msg.Body
	}
	return msg.Title
}
