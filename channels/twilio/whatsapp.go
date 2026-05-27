package twilio

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/elloloop/notify"
)

// whatsappPrefix is the channel marker Twilio's API expects on both the From
// and To addresses for WhatsApp sends.
const whatsappPrefix = "whatsapp:"

// WhatsAppProvider implements notify.Provider for the WhatsApp channel.
type WhatsAppProvider struct {
	client *Client
}

// NewWhatsApp returns a WhatsApp provider backed by c. The Client is shared
// with NewSMS — the channel-specific rules live on the provider, not the
// client.
func NewWhatsApp(c *Client) *WhatsAppProvider {
	return &WhatsAppProvider{client: c}
}

// Kind reports notify.ChannelWhatsApp.
func (p *WhatsAppProvider) Kind() notify.ChannelKind { return notify.ChannelWhatsApp }

// Name reports "twilio" — same as the SMS provider; the kind disambiguates.
func (p *WhatsAppProvider) Name() string { return "twilio" }

// Send delivers one WhatsApp message via Twilio's Messages API. The wire shape
// is identical to SMS except both the sender and recipient addresses are
// prefixed with "whatsapp:" (e.g. "whatsapp:+14155550000"). Twilio uses that
// prefix to route the request through the WhatsApp Business pipeline instead
// of the SMS one.
//
// If a MessagingServiceSID is configured we use it as-is — messaging services
// do not take the whatsapp: prefix. Only the static From number does.
func (p *WhatsAppProvider) Send(ctx context.Context, msg notify.Message) (notify.Receipt, error) {
	to := strings.TrimSpace(msg.To)
	if err := validateE164(stripWhatsAppPrefix(to)); err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("twilio whatsapp: %w", err)
	}

	form := url.Values{}
	form.Set("To", ensureWhatsAppPrefix(to))

	fromKey, fromVal := p.client.pickFrom()
	if fromKey == "From" {
		fromVal = ensureWhatsAppPrefix(fromVal)
	}
	form.Set(fromKey, fromVal)

	form.Set("Body", pickBody(msg))

	resp, err := p.client.sendMessage(ctx, form)
	if err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("twilio whatsapp: send: %w", err)
	}
	return notify.Receipt{
		ProviderMessageID: resp.SID,
		Status:            notify.StatusDelivered,
	}, nil
}

// ensureWhatsAppPrefix adds the whatsapp: marker if it's not already there.
// Idempotent: a caller that has already prefixed the address (config-side
// convention or a legacy code path) will not get a double prefix.
func ensureWhatsAppPrefix(addr string) string {
	if strings.HasPrefix(addr, whatsappPrefix) {
		return addr
	}
	return whatsappPrefix + addr
}

// stripWhatsAppPrefix is the inverse, used only to feed validateE164 a bare
// phone number.
func stripWhatsAppPrefix(addr string) string {
	return strings.TrimPrefix(addr, whatsappPrefix)
}
