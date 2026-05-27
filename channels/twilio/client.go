// Package twilio implements notify.Provider for the SMS and WhatsApp channels
// backed by Twilio's Messages API (https://www.twilio.com/docs/messaging/api).
//
// Implementation choice: hand-rolled net/http instead of github.com/twilio/twilio-go.
// The Messages API is a single HTTP POST with form-encoded body and HTTP Basic
// auth — roughly 50 lines of Go. The official SDK is a heavy dependency tree
// and its only "mocking hook" (SetEdge/SetRegion) is awkward to thread through
// tests; a custom Client lets the test inject any base URL via httptest.Server
// with no extra plumbing.
package twilio

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/elloloop/notify"
)

// defaultBaseURL is Twilio's production REST API host. Tests override it via
// notify.TwilioConfig.BaseURL is not present on the shared TwilioConfig (kept
// stable for cross-provider compatibility), so the override is exposed at the
// Client struct level instead — see New / NewWithBaseURL.
const defaultBaseURL = "https://api.twilio.com"

// apiVersion is fixed: Twilio's Messages resource lives under /2010-04-01.
const apiVersion = "2010-04-01"

// Client is the shared low-level HTTP client used by both the SMS and WhatsApp
// providers. It holds nothing channel-specific: the same instance can serve
// both channels concurrently.
type Client struct {
	accountSID          string
	authToken           string
	messagingServiceSID string
	from                string
	baseURL             string
	httpClient          *http.Client
}

// NewClient builds a Client from a notify.TwilioConfig. It validates that the
// account SID, auth token, and at least one of (From, MessagingServiceSID) are
// set. A missing required field is a configuration bug, not a runtime error —
// the container/library must fail fast at construction.
func NewClient(cfg notify.TwilioConfig) (*Client, error) {
	if strings.TrimSpace(cfg.AccountSID) == "" {
		return nil, errors.New("twilio: AccountSID is required")
	}
	if strings.TrimSpace(cfg.AuthToken) == "" {
		return nil, errors.New("twilio: AuthToken is required")
	}
	if strings.TrimSpace(cfg.From) == "" && strings.TrimSpace(cfg.MessagingServiceSID) == "" {
		return nil, errors.New("twilio: one of From or MessagingServiceSID is required")
	}
	return &Client{
		accountSID:          cfg.AccountSID,
		authToken:           cfg.AuthToken,
		messagingServiceSID: strings.TrimSpace(cfg.MessagingServiceSID),
		from:                strings.TrimSpace(cfg.From),
		baseURL:             defaultBaseURL,
		httpClient:          &http.Client{Timeout: 15 * time.Second},
	}, nil
}

// WithBaseURL returns a shallow copy of c with the base URL replaced. Tests use
// this to redirect requests at an httptest.Server; production callers leave it
// alone so the default api.twilio.com is hit.
func (c *Client) WithBaseURL(baseURL string) *Client {
	cp := *c
	cp.baseURL = strings.TrimRight(baseURL, "/")
	return &cp
}

// WithHTTPClient returns a shallow copy of c with the underlying http.Client
// replaced. Useful for plugging custom transports (proxies, instrumentation)
// without rebuilding the whole Client.
func (c *Client) WithHTTPClient(h *http.Client) *Client {
	cp := *c
	cp.httpClient = h
	return &cp
}

// messageResponse mirrors the subset of Twilio's Message resource fields the
// providers need. The full resource has ~20 fields; we only persist the SID.
type messageResponse struct {
	SID    string `json:"sid"`
	Status string `json:"status"`
}

// twilioError mirrors Twilio's standard error envelope:
//
//	{ "code": 21408, "message": "Permission to send an SMS has not been enabled...",
//	  "more_info": "https://...", "status": 400 }
//
// Only Code + Message are surfaced to the caller; the rest is incidental.
type twilioError struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	MoreInfo string `json:"more_info"`
	Status   int    `json:"status"`
}

func (e *twilioError) Error() string {
	return fmt.Sprintf("twilio: code=%d message=%q", e.Code, e.Message)
}

// sendMessage POSTs to /2010-04-01/Accounts/{SID}/Messages.json with the
// supplied form values plus Basic auth, and returns the decoded response. The
// caller (sms.go / whatsapp.go) is responsible for populating From/To/Body
// according to channel-specific rules; sendMessage itself is channel-agnostic.
func (c *Client) sendMessage(ctx context.Context, form url.Values) (messageResponse, error) {
	endpoint := fmt.Sprintf("%s/%s/Accounts/%s/Messages.json", c.baseURL, apiVersion, c.accountSID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return messageResponse{}, fmt.Errorf("twilio: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Basic "+basicAuth(c.accountSID, c.authToken))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return messageResponse{}, fmt.Errorf("twilio: do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return messageResponse{}, fmt.Errorf("twilio: read response: %w", err)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var out messageResponse
		if err := json.Unmarshal(body, &out); err != nil {
			return messageResponse{}, fmt.Errorf("twilio: decode response: %w", err)
		}
		return out, nil
	}

	// Non-2xx: try to decode Twilio's structured error first. If decoding fails
	// (bad gateway, HTML error page, …) fall back to a generic status-line error
	// so the caller still gets something actionable.
	var terr twilioError
	if jsonErr := json.Unmarshal(body, &terr); jsonErr == nil && terr.Code != 0 {
		return messageResponse{}, &terr
	}
	snippet := strings.TrimSpace(string(body))
	if len(snippet) > 200 {
		snippet = snippet[:200] + "..."
	}
	return messageResponse{}, fmt.Errorf("twilio: http %d: %s", resp.StatusCode, snippet)
}

// basicAuth returns the base64-encoded "user:pass" used in the Authorization
// header. Twilio uses the AccountSID as the username and the AuthToken as the
// password; nothing else.
func basicAuth(user, pass string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
}

// pickFrom resolves which sender identifier to use for a given send. If
// MessagingServiceSID is configured it wins (Twilio routes via the messaging
// service pool); otherwise the static From number is used. This mirrors
// Twilio's own precedence rules.
func (c *Client) pickFrom() (formKey, formVal string) {
	if c.messagingServiceSID != "" {
		return "MessagingServiceSid", c.messagingServiceSID
	}
	return "From", c.from
}
