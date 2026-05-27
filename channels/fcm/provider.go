// Package fcm implements the mobile-push channel against Firebase Cloud
// Messaging's HTTP v1 API. Implementation choice: hand-rolled HTTP v1 send
// (not the Firebase Admin Go SDK) — the v1 send is one POST and JWT auth is a
// single call into golang.org/x/oauth2/google, so we keep ~150 lines of plain
// Go and own all of the mocking surface via http.Client.Transport.
package fcm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	notify "github.com/elloloop/notify"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// defaultBaseURL is the public FCM HTTP v1 endpoint root. The path
// /v1/projects/{id}/messages:send is appended at send time.
const defaultBaseURL = "https://fcm.googleapis.com"

// fcmScope is the OAuth2 scope required to call the FCM HTTP v1 send endpoint.
const fcmScope = "https://www.googleapis.com/auth/firebase.messaging"

// ErrUnregisteredToken is returned (wrapped) when FCM reports the device token
// is no longer valid (UNREGISTERED or NOT_FOUND / registration-token-not-
// registered). Callers may unwrap with errors.Is to decide whether to purge the
// token from their device store. The provider itself never auto-purges — that
// is the orchestrator/store's job.
var ErrUnregisteredToken = errors.New("fcm: registration token is no longer valid")

// Provider is the FCM HTTP v1 mobile-push notify.Provider.
type Provider struct {
	projectID string
	baseURL   string

	httpClient *http.Client
	tokens     oauth2.TokenSource
}

// Option customises a Provider after construction. All options are optional;
// production callers typically pass none.
type Option func(*Provider)

// WithHTTPClient overrides the HTTP client used to POST to the FCM endpoint.
// Tests pass httptest.NewServer's client; production code leaves the default.
// The client is used only for the FCM send call, never for OAuth token fetch
// (token-fetch is governed by WithTokenSource).
func WithHTTPClient(c *http.Client) Option {
	return func(p *Provider) {
		if c != nil {
			p.httpClient = c
		}
	}
}

// WithTokenSource overrides the OAuth2 token source used to mint Bearer tokens
// for the FCM send call. Tests pass a static-token source so the test never
// hits oauth2.googleapis.com.
func WithTokenSource(ts oauth2.TokenSource) Option {
	return func(p *Provider) {
		if ts != nil {
			p.tokens = ts
		}
	}
}

// New constructs an FCM provider from notify.MobilePushConfig.
//
// Required config:
//   - FCMCredentialsJSON: raw bytes of the GCP service-account JSON. Callers
//     read this from a file or secret manager and pass the contents in.
//   - FCMProjectID: the GCP project id used in the v1 URL.
//
// Optional via opts: a custom http.Client or a custom oauth2.TokenSource. The
// constructor parses the credentials JSON eagerly so misconfiguration fails
// fast at wire-up time, not on first send.
func New(cfg notify.MobilePushConfig, opts ...Option) (*Provider, error) {
	if cfg.FCMProjectID == "" {
		return nil, errors.New("fcm: FCMProjectID is required")
	}
	if cfg.FCMCredentialsJSON == "" {
		return nil, errors.New("fcm: FCMCredentialsJSON is required")
	}

	p := &Provider{
		projectID: cfg.FCMProjectID,
		baseURL:   defaultBaseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	// Parse the service-account JSON eagerly so a malformed credential blob
	// fails at construction, not on first send.
	jwtCfg, err := google.JWTConfigFromJSON([]byte(cfg.FCMCredentialsJSON), fcmScope)
	if err != nil {
		return nil, fmt.Errorf("fcm: parse credentials: %w", err)
	}
	// Default token source is the real one. Tests override via WithTokenSource
	// before any Send call, which sidesteps the real OAuth2 endpoint.
	p.tokens = jwtCfg.TokenSource(context.Background())

	for _, opt := range opts {
		opt(p)
	}

	return p, nil
}

// Kind reports the channel this provider serves.
func (p *Provider) Kind() notify.ChannelKind { return notify.ChannelMobilePush }

// Name identifies the concrete backend.
func (p *Provider) Name() string { return "fcm" }

// SetBaseURL overrides the FCM endpoint root. Exposed for tests that spin up
// an httptest.Server; production callers leave it untouched. Trailing slashes
// are stripped.
func (p *Provider) SetBaseURL(u string) {
	p.baseURL = strings.TrimRight(u, "/")
}

// fcmMessage is the request body for POST /v1/projects/{id}/messages:send.
// Only the fields the provider populates today are modelled; FCM tolerates
// unknown fields on requests but we keep the surface minimal.
type fcmMessage struct {
	Message fcmMessageInner `json:"message"`
}

type fcmMessageInner struct {
	Token        string            `json:"token"`
	Notification *fcmNotification  `json:"notification,omitempty"`
	Data         map[string]string `json:"data,omitempty"`
}

type fcmNotification struct {
	Title string `json:"title,omitempty"`
	Body  string `json:"body,omitempty"`
}

// fcmSendResponse is the success body returned by the v1 send endpoint.
type fcmSendResponse struct {
	Name string `json:"name"`
}

// fcmErrorEnvelope is the standard Google API error shape. We only consume the
// fields we route on: HTTP status (via the response), the error message (for
// wrapping), and the FCM-specific ErrorCode in details.
type fcmErrorEnvelope struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
		Details []struct {
			Type      string `json:"@type"`
			ErrorCode string `json:"errorCode"`
		} `json:"details"`
	} `json:"error"`
}

// Send delivers one message to FCM. See package comment for the request shape
// and status mapping.
func (p *Provider) Send(ctx context.Context, msg notify.Message) (notify.Receipt, error) {
	if msg.To == "" {
		return notify.Receipt{Status: notify.StatusFailed}, errors.New("fcm: empty device token")
	}

	body := fcmMessage{
		Message: fcmMessageInner{
			Token: msg.To,
			Data:  msg.Data,
		},
	}
	if msg.Title != "" || msg.Body != "" {
		body.Message.Notification = &fcmNotification{Title: msg.Title, Body: msg.Body}
	}

	buf, err := json.Marshal(body)
	if err != nil {
		// json.Marshal of plain string maps never fails in practice; we still
		// surface it cleanly rather than panic.
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("fcm: marshal payload: %w", err)
	}

	endpoint, err := url.JoinPath(p.baseURL, "/v1/projects/", p.projectID, "/messages:send")
	if err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("fcm: build endpoint: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(buf))
	if err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("fcm: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	tok, err := p.tokens.Token()
	if err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("fcm: mint access token: %w", err)
	}
	tok.SetAuthHeader(req)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("fcm: post: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("fcm: read response: %w", err)
	}

	if resp.StatusCode == http.StatusOK {
		var ok fcmSendResponse
		if err := json.Unmarshal(respBody, &ok); err != nil {
			return notify.Receipt{Status: notify.StatusFailed}, fmt.Errorf("fcm: decode success: %w", err)
		}
		return notify.Receipt{ProviderMessageID: ok.Name, Status: notify.StatusDelivered}, nil
	}

	// Non-2xx: try to decode the standard Google API error envelope and route
	// on it. If decoding fails we still fail the send and wrap the raw body.
	var apiErr fcmErrorEnvelope
	_ = json.Unmarshal(respBody, &apiErr)

	if isUnregistered(resp.StatusCode, apiErr) {
		// Wrap ErrUnregisteredToken so callers can errors.Is() and choose to
		// purge the token. We deliberately do NOT delete state here.
		return notify.Receipt{Status: notify.StatusFailed},
			fmt.Errorf("%w: %s", ErrUnregisteredToken, summarise(apiErr, respBody))
	}

	return notify.Receipt{Status: notify.StatusFailed},
		fmt.Errorf("fcm: send failed (http %d): %s", resp.StatusCode, summarise(apiErr, respBody))
}

// isUnregistered reports whether the FCM error indicates the token is gone.
// The v1 API signals this two ways:
//   - HTTP 404 with status NOT_FOUND
//   - HTTP 404 with details[].errorCode == "UNREGISTERED"
//
// Older transitional responses also surface UNREGISTERED under a 400. We
// accept either by looking at details first and falling back to the HTTP
// status + status string.
func isUnregistered(httpStatus int, env fcmErrorEnvelope) bool {
	for _, d := range env.Error.Details {
		if d.ErrorCode == "UNREGISTERED" {
			return true
		}
	}
	if httpStatus == http.StatusNotFound {
		return true
	}
	if env.Error.Status == "NOT_FOUND" {
		return true
	}
	return false
}

// summarise picks the most useful short string out of an FCM error envelope,
// falling back to the raw body when the envelope is empty (e.g. an HTML 502
// from a frontend proxy).
func summarise(env fcmErrorEnvelope, raw []byte) string {
	if env.Error.Message != "" {
		return env.Error.Message
	}
	if env.Error.Status != "" {
		return env.Error.Status
	}
	if len(raw) == 0 {
		return "(empty body)"
	}
	if len(raw) > 512 {
		return string(raw[:512]) + "…"
	}
	return string(raw)
}
