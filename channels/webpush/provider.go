// Package webpush is a notify.Provider that delivers via the W3C Web Push API
// (RFC 8030) using VAPID (RFC 8292) for application-server identification and
// the aes128gcm content encoding (RFC 8291) for payload encryption.
//
// The browser-side `PushSubscription` JSON blob — `{endpoint, keys.p256dh,
// keys.auth}` — is the per-device address. Consumers register that JSON with
// notify (it lands in `Device.Token`) and the orchestrator passes it back to
// us as `Message.To`. We decode it, encrypt + POST the payload, and translate
// the push service's response into a notify.Receipt.
//
// The push service itself does not return a message id, so successful sends
// produce a receipt with an empty ProviderMessageID. A 410 Gone from the push
// service — the signal that the user has uninstalled the app / unsubscribed —
// surfaces as ErrSubscriptionGone so callers can decide whether to purge the
// device row; this package never auto-purges.
package webpush

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	wp "github.com/SherClockHolmes/webpush-go"

	"github.com/elloloop/notify"
)

// ErrSubscriptionGone is returned in the error of Send's Receipt when the push
// service responds with 410 Gone, indicating the subscription is no longer
// valid (the user unsubscribed or uninstalled the browser/app). Callers may
// match with errors.Is to decide whether to purge the device row.
var ErrSubscriptionGone = errors.New("webpush: subscription gone")

// defaultTTL is the value sent in the TTL header (seconds) when the config
// omits one. 60s mirrors what most browser-side examples ship: deliver-now or
// drop. The Web Push spec requires TTL be present on every request.
const defaultTTL = 60

// Provider is the VAPID Web Push provider. Construct it with New and inject it
// into a notify.ProviderRegistry under ChannelKind == ChannelWebPush.
type Provider struct {
	cfg     notify.WebPushConfig
	ttl     int
	urgency wp.Urgency
	// client is the HTTP client the underlying webpush-go library uses to POST
	// to the push service. Tests inject an httptest-backed client; production
	// leaves it nil and the library falls back to http.DefaultClient.
	client wp.HTTPClient
}

// New constructs a VAPID Web Push provider from a WebPushConfig. The contact
// email is required by RFC 8292 — push services may reject anonymous senders.
// We accept either an RFC 5321 mailto recipient (with or without the prefix)
// or an https URL identifying the application server operator.
//
// Optional fields (TTL, Urgency) fall back to the defaults documented above.
func New(cfg notify.WebPushConfig, opts ...Option) (*Provider, error) {
	if cfg.VAPIDPublic == "" {
		return nil, errors.New("webpush: VAPIDPublic is required")
	}
	if cfg.VAPIDPrivate == "" {
		return nil, errors.New("webpush: VAPIDPrivate is required")
	}
	if err := validateSubscriber(cfg.ContactEmail); err != nil {
		return nil, err
	}

	p := &Provider{
		cfg:     cfg,
		ttl:     defaultTTL,
		urgency: wp.UrgencyNormal,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// Option tweaks a Provider during construction. The functional-options pattern
// keeps New's signature stable as we add knobs (currently TTL, urgency, and a
// custom HTTP client for tests).
type Option func(*Provider)

// WithTTL sets the value sent in the push service's TTL header (seconds). The
// push service is permitted to drop the message if the user's device cannot
// receive it within this window.
func WithTTL(seconds int) Option {
	return func(p *Provider) {
		if seconds > 0 {
			p.ttl = seconds
		}
	}
}

// WithUrgency sets the Urgency header for the push request. Only the four
// values defined by RFC 8030 §5.3 are accepted by the library — anything else
// is silently dropped from the request, so we let the library validate.
func WithUrgency(u string) Option {
	return func(p *Provider) {
		if u != "" {
			p.urgency = wp.Urgency(u)
		}
	}
}

// WithHTTPClient injects a custom HTTP client. Intended for tests that point
// the provider at an httptest.Server; production callers should leave this
// unset and let the library use http.DefaultClient.
func WithHTTPClient(c wp.HTTPClient) Option {
	return func(p *Provider) {
		p.client = c
	}
}

// Kind reports the channel this provider serves.
func (p *Provider) Kind() notify.ChannelKind { return notify.ChannelWebPush }

// Name reports the concrete backend identifier persisted alongside delivery
// records for auditing / debugging.
func (p *Provider) Name() string { return "vapid" }

// pushPayload is the JSON shape we ship to the service worker. Keeping it
// flat (`title`, `body`, `data`) matches the de-facto contract used by every
// `self.addEventListener('push', …)` example in the wild — service workers
// can blindly forward `payload.data` to `notification.data` without parsing.
type pushPayload struct {
	Title string            `json:"title"`
	Body  string            `json:"body"`
	Data  map[string]string `json:"data,omitempty"`
}

// Send encrypts and POSTs the message to the subscription's push endpoint.
//
// Error handling is layered: subscription-parse failures and missing keys are
// caller-visible errors (the device row is unusable and should be purged or
// refreshed). 410 Gone is reported as ErrSubscriptionGone — same outcome,
// distinct sentinel — so callers can purge specifically on that signal. Other
// 4xx/5xx are reported with the body text included for triage.
//
// All failure paths return Receipt{Status: StatusFailed} alongside the error
// so the orchestrator records the row as failed even if it ignores the error.
func (p *Provider) Send(ctx context.Context, msg notify.Message) (notify.Receipt, error) {
	sub, err := decodeSubscription(msg.To)
	if err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, err
	}

	body, err := json.Marshal(pushPayload{
		Title: msg.Title,
		Body:  msg.Body,
		Data:  msg.Data,
	})
	if err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("webpush: marshal payload: %w", err)
	}

	opts := &wp.Options{
		Subscriber:      p.cfg.ContactEmail,
		VAPIDPublicKey:  p.cfg.VAPIDPublic,
		VAPIDPrivateKey: p.cfg.VAPIDPrivate,
		TTL:             p.ttl,
		Urgency:         p.urgency,
		HTTPClient:      p.client,
	}

	resp, err := wp.SendNotificationWithContext(ctx, body, sub, opts)
	if err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("webpush: send: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusAccepted:
		// Push services typically return 201 Created for a queued message; 200
		// and 202 are also spec-legal. Drain the body so the connection can be
		// reused — the push protocol doesn't carry a message id.
		_, _ = io.Copy(io.Discard, resp.Body)
		return notify.Receipt{Status: notify.StatusDelivered}, nil

	case resp.StatusCode == http.StatusGone:
		return notify.Receipt{Status: notify.StatusFailed}, ErrSubscriptionGone

	default:
		// Surface the response body for triage. We cap at a few KB because
		// some push services return verbose HTML on misconfiguration.
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf(
			"webpush: push service returned %d: %s",
			resp.StatusCode, strings.TrimSpace(string(bodyBytes)),
		)
	}
}

// decodeSubscription parses the JSON shape the browser's PushSubscription.toJSON()
// produces. Both keys (auth, p256dh) are required for content encryption — a
// subscription without them cannot accept payloads. We reject early rather
// than letting the webpush library return a generic base64 error.
func decodeSubscription(raw string) (*wp.Subscription, error) {
	if raw == "" {
		return nil, errors.New("webpush: subscription is empty")
	}
	var s wp.Subscription
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return nil, fmt.Errorf("webpush: decode subscription: %w", err)
	}
	if s.Endpoint == "" {
		return nil, errors.New("webpush: subscription missing endpoint")
	}
	if s.Keys.Auth == "" || s.Keys.P256dh == "" {
		return nil, errors.New("webpush: subscription missing keys (auth and p256dh required)")
	}
	return &s, nil
}

// validateSubscriber enforces RFC 8292's requirement that the `sub` claim in
// the VAPID JWT be either a `mailto:` URI or an `https:` URL. The underlying
// library auto-prefixes `mailto:` for non-https strings, but we still reject
// obviously malformed inputs up front so misconfiguration fails loud.
func validateSubscriber(s string) error {
	if s == "" {
		return errors.New("webpush: ContactEmail is required")
	}
	if strings.HasPrefix(s, "mailto:") {
		return nil
	}
	if strings.HasPrefix(s, "https://") {
		if _, err := url.Parse(s); err != nil {
			return fmt.Errorf("webpush: ContactEmail: invalid https URL: %w", err)
		}
		return nil
	}
	// Bare address — accept iff it parses as `user@host`. The library will
	// prepend `mailto:` for us.
	if i := strings.IndexByte(s, '@'); i > 0 && i < len(s)-1 {
		return nil
	}
	return fmt.Errorf("webpush: ContactEmail must be a mailto: address or https URL, got %q", s)
}
