package webpush_test

import (
	"context"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	wp "github.com/SherClockHolmes/webpush-go"

	"github.com/elloloop/notify"
	notifywp "github.com/elloloop/notify/channels/webpush"
)

// rewriteTransport rewrites the host/scheme of every outbound request to point
// at a test server, preserving the original path. This lets us put a realistic
// `https://fcm.googleapis.com/wp/...` URL in the subscription (so the webpush
// library's VAPID `aud` calculation works against a real-looking endpoint) and
// still serve the request locally.
type rewriteTransport struct {
	target *url.URL
	inner  http.RoundTripper
}

func (r *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = r.target.Scheme
	req.URL.Host = r.target.Host
	req.Host = r.target.Host
	return r.inner.RoundTrip(req)
}

// testKeys carries everything a test needs to drive the provider: a VAPID
// keypair (server-side), a base64'd P-256 subscription point and matching auth
// secret (client-side). All are freshly generated per test to keep cases
// independent.
type testKeys struct {
	vapidPub  string
	vapidPriv string

	subP256dh string
	subAuth   string
}

func newTestKeys(t *testing.T) testKeys {
	t.Helper()

	vpriv, vpub, err := wp.GenerateVAPIDKeys()
	if err != nil {
		t.Fatalf("GenerateVAPIDKeys: %v", err)
	}

	// Subscription's p256dh is an uncompressed P-256 point: 0x04 || X || Y.
	curve := elliptic.P256()
	_, x, y, err := elliptic.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatalf("subscription key gen: %v", err)
	}
	p256dh := elliptic.Marshal(curve, x, y)

	authSecret := make([]byte, 16)
	if _, err := rand.Read(authSecret); err != nil {
		t.Fatalf("auth secret: %v", err)
	}

	return testKeys{
		vapidPub:  vpub,
		vapidPriv: vpriv,
		subP256dh: base64.RawURLEncoding.EncodeToString(p256dh),
		subAuth:   base64.RawURLEncoding.EncodeToString(authSecret),
	}
}

// subscriptionJSON constructs the JSON blob the browser would have produced
// from PushSubscription.toJSON(), pointed at the given endpoint.
func subscriptionJSON(t *testing.T, endpoint string, k testKeys) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"endpoint": endpoint,
		"keys": map[string]string{
			"p256dh": k.subP256dh,
			"auth":   k.subAuth,
		},
	})
	if err != nil {
		t.Fatalf("marshal subscription: %v", err)
	}
	return string(b)
}

// newProvider builds a Provider wired to point at the supplied test server.
// All requests, regardless of the endpoint in the subscription, get rewritten
// to srv.URL so the test owns every HTTP call.
func newProvider(t *testing.T, srv *httptest.Server, k testKeys, opts ...notifywp.Option) *notifywp.Provider {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	httpClient := &http.Client{
		Transport: &rewriteTransport{target: u, inner: http.DefaultTransport},
	}

	cfg := notify.WebPushConfig{
		Provider:     "vapid",
		VAPIDPublic:  k.vapidPub,
		VAPIDPrivate: k.vapidPriv,
		ContactEmail: "mailto:ops@example.com",
	}
	opts = append([]notifywp.Option{notifywp.WithHTTPClient(httpClient)}, opts...)
	p, err := notifywp.New(cfg, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func TestNew_Validation(t *testing.T) {
	t.Parallel()

	k := newTestKeys(t)

	cases := []struct {
		name string
		cfg  notify.WebPushConfig
		want string
	}{
		{
			name: "missing public key",
			cfg:  notify.WebPushConfig{VAPIDPrivate: k.vapidPriv, ContactEmail: "mailto:ops@example.com"},
			want: "VAPIDPublic is required",
		},
		{
			name: "missing private key",
			cfg:  notify.WebPushConfig{VAPIDPublic: k.vapidPub, ContactEmail: "mailto:ops@example.com"},
			want: "VAPIDPrivate is required",
		},
		{
			name: "missing contact",
			cfg:  notify.WebPushConfig{VAPIDPublic: k.vapidPub, VAPIDPrivate: k.vapidPriv},
			want: "ContactEmail is required",
		},
		{
			name: "malformed contact",
			cfg:  notify.WebPushConfig{VAPIDPublic: k.vapidPub, VAPIDPrivate: k.vapidPriv, ContactEmail: "nope"},
			want: "ContactEmail must be a mailto",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := notifywp.New(tc.cfg)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestKindAndName(t *testing.T) {
	t.Parallel()
	k := newTestKeys(t)
	p, err := notifywp.New(notify.WebPushConfig{
		VAPIDPublic:  k.vapidPub,
		VAPIDPrivate: k.vapidPriv,
		ContactEmail: "mailto:ops@example.com",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := p.Kind(); got != notify.ChannelWebPush {
		t.Fatalf("Kind = %q, want %q", got, notify.ChannelWebPush)
	}
	if got := p.Name(); got != "vapid" {
		t.Fatalf("Name = %q, want %q", got, "vapid")
	}
}

func TestSend_HappyPath201(t *testing.T) {
	t.Parallel()
	k := newTestKeys(t)

	var (
		gotAuth        atomic.Value // string
		gotTTL         atomic.Value // string
		gotUrgency     atomic.Value // string
		gotContentType atomic.Value // string
		gotCalls       atomic.Int32
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCalls.Add(1)
		gotAuth.Store(r.Header.Get("Authorization"))
		gotTTL.Store(r.Header.Get("TTL"))
		gotUrgency.Store(r.Header.Get("Urgency"))
		gotContentType.Store(r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	p := newProvider(t, srv, k, notifywp.WithTTL(120), notifywp.WithUrgency("high"))

	// A realistic-looking push endpoint; rewriteTransport will redirect to srv.
	sub := subscriptionJSON(t, "https://fcm.googleapis.com/wp/test-endpoint", k)

	rec, err := p.Send(context.Background(), notify.Message{
		To:    sub,
		Title: "hello",
		Body:  "world",
		Data:  map[string]string{"deep_link": "/x/y"},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if rec.Status != notify.StatusDelivered {
		t.Fatalf("Status = %q, want %q", rec.Status, notify.StatusDelivered)
	}
	if rec.ProviderMessageID != "" {
		t.Fatalf("ProviderMessageID = %q, want empty (push has no id)", rec.ProviderMessageID)
	}

	if gotCalls.Load() != 1 {
		t.Fatalf("server saw %d calls, want 1", gotCalls.Load())
	}
	if auth, _ := gotAuth.Load().(string); !strings.HasPrefix(auth, "vapid t=") || !strings.Contains(auth, ", k=") {
		t.Fatalf("Authorization header missing/malformed: %q", auth)
	}
	if ttl, _ := gotTTL.Load().(string); ttl != "120" {
		t.Fatalf("TTL header = %q, want %q", ttl, "120")
	}
	if urg, _ := gotUrgency.Load().(string); urg != "high" {
		t.Fatalf("Urgency header = %q, want %q", urg, "high")
	}
	if ct, _ := gotContentType.Load().(string); ct != "application/octet-stream" {
		t.Fatalf("Content-Type header = %q, want %q", ct, "application/octet-stream")
	}
}

func TestSend_410GoneReturnsSentinel(t *testing.T) {
	t.Parallel()
	k := newTestKeys(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGone)
	}))
	defer srv.Close()

	p := newProvider(t, srv, k)
	sub := subscriptionJSON(t, "https://updates.push.services.mozilla.com/wpush/v2/gAAAA", k)

	rec, err := p.Send(context.Background(), notify.Message{To: sub, Title: "x", Body: "y"})
	if !errors.Is(err, notifywp.ErrSubscriptionGone) {
		t.Fatalf("expected ErrSubscriptionGone, got %v", err)
	}
	if rec.Status != notify.StatusFailed {
		t.Fatalf("Status = %q, want %q", rec.Status, notify.StatusFailed)
	}
}

func TestSend_400Failure(t *testing.T) {
	t.Parallel()
	k := newTestKeys(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("malformed body"))
	}))
	defer srv.Close()

	p := newProvider(t, srv, k)
	sub := subscriptionJSON(t, "https://fcm.googleapis.com/wp/whatever", k)

	rec, err := p.Send(context.Background(), notify.Message{To: sub, Title: "t", Body: "b"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if errors.Is(err, notifywp.ErrSubscriptionGone) {
		t.Fatalf("400 should NOT match ErrSubscriptionGone sentinel: %v", err)
	}
	if !strings.Contains(err.Error(), "400") {
		t.Fatalf("error %q should mention status 400", err.Error())
	}
	if !strings.Contains(err.Error(), "malformed body") {
		t.Fatalf("error %q should include response body", err.Error())
	}
	if rec.Status != notify.StatusFailed {
		t.Fatalf("Status = %q, want %q", rec.Status, notify.StatusFailed)
	}
}

func TestSend_500Failure(t *testing.T) {
	t.Parallel()
	k := newTestKeys(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := newProvider(t, srv, k)
	sub := subscriptionJSON(t, "https://fcm.googleapis.com/wp/abc", k)

	rec, err := p.Send(context.Background(), notify.Message{To: sub})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("error %q should mention status 500", err.Error())
	}
	if rec.Status != notify.StatusFailed {
		t.Fatalf("Status = %q, want %q", rec.Status, notify.StatusFailed)
	}
}

func TestSend_MalformedSubscriptionJSON(t *testing.T) {
	t.Parallel()
	k := newTestKeys(t)

	// The handler must NEVER be reached — bad inputs short-circuit before the HTTP call.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("server should not be called for malformed subscription")
	}))
	defer srv.Close()

	p := newProvider(t, srv, k)

	rec, err := p.Send(context.Background(), notify.Message{To: "not-json"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "decode subscription") {
		t.Fatalf("error %q should mention decode subscription", err.Error())
	}
	if rec.Status != notify.StatusFailed {
		t.Fatalf("Status = %q, want %q", rec.Status, notify.StatusFailed)
	}
}

func TestSend_EmptySubscription(t *testing.T) {
	t.Parallel()
	k := newTestKeys(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("server should not be called for empty subscription")
	}))
	defer srv.Close()

	p := newProvider(t, srv, k)

	rec, err := p.Send(context.Background(), notify.Message{To: ""})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("error %q should mention empty", err.Error())
	}
	if rec.Status != notify.StatusFailed {
		t.Fatalf("Status = %q, want %q", rec.Status, notify.StatusFailed)
	}
}

func TestSend_MissingKeysField(t *testing.T) {
	t.Parallel()
	k := newTestKeys(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("server should not be called for subscription missing keys")
	}))
	defer srv.Close()

	p := newProvider(t, srv, k)

	// Valid JSON, valid endpoint, but `keys` is absent — the encryption step
	// would fail with a generic base64 error; we reject early instead.
	raw, _ := json.Marshal(map[string]any{
		"endpoint": "https://fcm.googleapis.com/wp/abc",
	})

	rec, err := p.Send(context.Background(), notify.Message{To: string(raw)})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "missing keys") {
		t.Fatalf("error %q should mention missing keys", err.Error())
	}
	if rec.Status != notify.StatusFailed {
		t.Fatalf("Status = %q, want %q", rec.Status, notify.StatusFailed)
	}
}

func TestSend_MissingEndpoint(t *testing.T) {
	t.Parallel()
	k := newTestKeys(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("server should not be called for subscription missing endpoint")
	}))
	defer srv.Close()

	p := newProvider(t, srv, k)

	raw, _ := json.Marshal(map[string]any{
		"keys": map[string]string{"p256dh": k.subP256dh, "auth": k.subAuth},
	})

	rec, err := p.Send(context.Background(), notify.Message{To: string(raw)})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "missing endpoint") {
		t.Fatalf("error %q should mention missing endpoint", err.Error())
	}
	if rec.Status != notify.StatusFailed {
		t.Fatalf("Status = %q, want %q", rec.Status, notify.StatusFailed)
	}
}

func TestSend_DefaultTTLAndUrgency(t *testing.T) {
	t.Parallel()
	k := newTestKeys(t)

	var (
		gotTTL     atomic.Value
		gotUrgency atomic.Value
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTTL.Store(r.Header.Get("TTL"))
		gotUrgency.Store(r.Header.Get("Urgency"))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	p := newProvider(t, srv, k) // no WithTTL / WithUrgency

	sub := subscriptionJSON(t, "https://fcm.googleapis.com/wp/abc", k)
	if _, err := p.Send(context.Background(), notify.Message{To: sub, Title: "h", Body: "b"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if ttl, _ := gotTTL.Load().(string); ttl != "60" {
		t.Fatalf("default TTL = %q, want 60", ttl)
	}
	if urg, _ := gotUrgency.Load().(string); urg != "normal" {
		t.Fatalf("default Urgency = %q, want normal", urg)
	}
}

// Make sure the Provider satisfies notify.Provider statically — a regression
// here would prevent registration into the orchestrator.
var _ notify.Provider = (*notifywp.Provider)(nil)
