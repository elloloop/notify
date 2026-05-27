package server

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"

	"github.com/elloloop/notify"
	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
	"github.com/elloloop/notify/gen/go/notify/v1/notifyv1connect"
)

func makeConfig() Config {
	cfg := Config{
		LogLevel:        "error",
		ShutdownTimeout: 5 * time.Second,
		Auth: AuthConfig{
			DevMode:   true,
			JWTLeeway: 30 * time.Second,
		},
	}
	cfg.Store.Driver = "memory"
	cfg.LiveConnections.Enabled = true
	cfg.LiveConnections.HeartbeatInterval = time.Second
	cfg.LiveConnections.RetryInterval = time.Second
	return cfg
}

func TestServer_NewAndRunShutdown(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := makeConfig()
	srv, err := NewWithDeps(cfg, Dependencies{Logger: logger})
	if err != nil {
		t.Fatalf("NewWithDeps: %v", err)
	}

	// Bind on :0 across all three listeners so the OS picks free ports.
	clientLn := mustListen(t)
	internalLn := mustListen(t)
	metricsLn := mustListen(t)
	srv.UseTestListeners(clientLn, internalLn, metricsLn)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	// Health endpoint becomes 200 once Run flips ready.
	if err := waitForHealthOK("http://" + metricsLn.Addr().String() + "/healthz"); err != nil {
		t.Fatalf("healthz: %v", err)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestServer_HealthzReports503BeforeReady(t *testing.T) {
	state := newHealthState()
	rec := newRecorder()
	state.ServeHTTP(rec, nil)
	if rec.code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", rec.code)
	}
	state.markReady()
	rec2 := newRecorder()
	state.ServeHTTP(rec2, nil)
	if rec2.code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec2.code)
	}
}

func TestServer_MetricsEndpointServesText(t *testing.T) {
	state := newHealthState()
	state.markReady()
	mux := newMetricsMux(state)

	srv := httpTestServer(t, mux)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct == "" || ct[:10] != "text/plain" {
		t.Fatalf("content-type = %q", ct)
	}
}

func TestServer_NewWithUnknownDriverErrors(t *testing.T) {
	cfg := makeConfig()
	cfg.Store.Driver = "magic"
	_, err := NewWithDeps(cfg, Dependencies{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestServer_NewRequiresJWTOrDev(t *testing.T) {
	cfg := makeConfig()
	cfg.Auth.DevMode = false
	cfg.Auth.JWTSecret = ""
	_, err := NewWithDeps(cfg, Dependencies{})
	if err == nil {
		t.Fatal("expected error for missing jwt secret")
	}
}

func TestServer_BuildValidator(t *testing.T) {
	v, err := buildValidator(AuthConfig{JWTSecret: "x", JWTLeeway: time.Second})
	if err != nil {
		t.Fatalf("buildValidator: %v", err)
	}
	if _, ok := v.(*JWTValidator); !ok {
		t.Fatalf("expected JWTValidator, got %T", v)
	}

	v2, err := buildValidator(AuthConfig{DevMode: true})
	if err != nil {
		t.Fatalf("buildValidator(dev): %v", err)
	}
	if _, ok := v2.(DevValidator); !ok {
		t.Fatalf("expected DevValidator, got %T", v2)
	}

	if _, err := buildValidator(AuthConfig{}); err == nil {
		t.Fatal("expected error for empty config")
	}
}

func TestServer_BuildStore(t *testing.T) {
	store, closer, err := buildStore(notifyStoreConfigMemory())
	if err != nil {
		t.Fatalf("buildStore: %v", err)
	}
	if store == nil {
		t.Fatal("store nil")
	}
	if closer != nil {
		t.Fatalf("memory should have no closer, got %T", closer)
	}

	if _, _, err := buildStore(notifyStoreConfigPostgres()); err == nil {
		t.Fatal("expected directive to use Dependencies.Store for postgres")
	}
	if _, _, err := buildStore(notifyStoreConfigEntdb()); err == nil {
		t.Fatal("expected directive to use Dependencies.Store for entdb")
	}
	if _, _, err := buildStore(notifyStoreConfigUnknown()); err == nil {
		t.Fatal("expected error for unknown driver")
	}
}

func TestServer_InternalServiceRoundTrip(t *testing.T) {
	// Boot a real server with the in-memory store + the in-app provider,
	// then call NotificationInternalService.Notify and assert the response.
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := makeConfig()
	cfg.Auth.InternalToken = "internal-secret"
	cfg.Auth.DevMode = false
	cfg.Auth.JWTSecret = "jwt-secret"
	srv, err := NewWithDeps(cfg, Dependencies{Logger: logger})
	if err != nil {
		t.Fatalf("NewWithDeps: %v", err)
	}
	clientLn := mustListen(t)
	internalLn := mustListen(t)
	metricsLn := mustListen(t)
	srv.UseTestListeners(clientLn, internalLn, metricsLn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-runErr
	})

	if err := waitForCondition(3*time.Second, func() bool {
		return tcpReachable(internalLn.Addr().String())
	}); err != nil {
		t.Fatalf("internal listener: %v", err)
	}

	httpClient := h2cClient()
	client := notifyv1connect.NewNotificationInternalServiceClient(httpClient, "http://"+internalLn.Addr().String())

	req := connect.NewRequest(&notifyv1.NotifyRequest{
		TenantId:       "t",
		NotificationId: "evt-1",
		UserIds:        []string{"u1"},
		Channels:       []notifyv1.DeliveryChannel{notifyv1.DeliveryChannel_DELIVERY_CHANNEL_IN_APP},
		Title:          "ping",
	})
	req.Header().Set("X-Notify-Internal-Token", "internal-secret")
	resp, err := client.Notify(context.Background(), req)
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if resp.Msg.GetDelivered() != 1 {
		t.Fatalf("Delivered = %d, want 1", resp.Msg.GetDelivered())
	}

	// Bad token rejected.
	req2 := connect.NewRequest(&notifyv1.NotifyRequest{
		TenantId:       "t",
		NotificationId: "evt-2",
		UserIds:        []string{"u1"},
	})
	req2.Header().Set("X-Notify-Internal-Token", "wrong")
	if _, err := client.Notify(context.Background(), req2); err == nil {
		t.Fatal("expected error for bad token")
	} else if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("code = %v", connect.CodeOf(err))
	}
}

func TestServer_StreamUnimplementedWhenLiveOff(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := makeConfig()
	cfg.LiveConnections.Enabled = false
	srv, err := NewWithDeps(cfg, Dependencies{Logger: logger})
	if err != nil {
		t.Fatalf("NewWithDeps: %v", err)
	}
	// The in-app provider must NOT have been registered when live is off.
	// We can't introspect ProviderRegistry directly, but the orchestrator's
	// Enabled() set is a fine indirection.
	if srv.registry != nil {
		t.Fatalf("registry should be nil when live connections off")
	}
}

func TestServer_CloseFuncFor(t *testing.T) {
	called := false
	c := CloseFuncFor(func() error { called = true; return nil })
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !called {
		t.Fatal("inner not called")
	}
}

// ─── tiny helpers ───────────────────────────────────────────────────────

func mustListen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return ln
}

func waitForHealthOK(url string) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:gosec,noctx -- test-only call
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errEval{"health never reported 200"}
}

func tcpReachable(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func h2cClient() *http.Client {
	// Plaintext HTTP/2 dialer for the local listener — connect-go's
	// internal-service client uses unary RPCs that work fine over HTTP/1.1
	// too, but explicitly forcing H2C keeps parity with production.
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(_ context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}
}

type recorder struct {
	code   int
	header http.Header
	body   []byte
}

func newRecorder() *recorder { return &recorder{header: make(http.Header), code: http.StatusOK} }

func (r *recorder) Header() http.Header  { return r.header }
func (r *recorder) WriteHeader(c int)    { r.code = c }
func (r *recorder) Write(p []byte) (int, error) {
	r.body = append(r.body, p...)
	return len(p), nil
}

func httpTestServer(t *testing.T, mux http.Handler) *httpServerWithURL {
	t.Helper()
	ln := mustListen(t)
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return &httpServerWithURL{URL: "http://" + ln.Addr().String(), srv: srv, ln: ln}
}

type httpServerWithURL struct {
	URL string
	srv *http.Server
	ln  net.Listener
}

func (h *httpServerWithURL) Close()         { _ = h.srv.Close(); _ = h.ln.Close() }
func (h *httpServerWithURL) Client() *http.Client { return http.DefaultClient }

type errEval struct{ msg string }

func (e errEval) Error() string { return e.msg }

// The buildStore tests use notify.StoreConfig directly via these helpers so a
// future field addition does not break compilation of the test file.

func notifyStoreConfigMemory() notify.StoreConfig {
	return notify.StoreConfig{Driver: "memory"}
}
func notifyStoreConfigPostgres() notify.StoreConfig {
	return notify.StoreConfig{Driver: "postgres"}
}
func notifyStoreConfigEntdb() notify.StoreConfig {
	return notify.StoreConfig{Driver: "entdb"}
}
func notifyStoreConfigUnknown() notify.StoreConfig {
	return notify.StoreConfig{Driver: "???"}
}
