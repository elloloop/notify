package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/elloloop/notify"
	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
	"github.com/elloloop/notify/gen/go/notify/v1/notifyv1connect"
	"github.com/elloloop/notify/realtime"
	"github.com/elloloop/notify/store/memory"
)

// StoreCloser is anything that wants notification of process shutdown. The
// postgres and entdb store drivers implement Close (their pool / SDK client
// lifecycle); memory does not, so we accept "nothing to do" too.
type StoreCloser interface {
	Close() error
}

// closeFunc adapts a parameterless cleanup into the StoreCloser shape.
type closeFunc func() error

func (f closeFunc) Close() error { return f() }

// Dependencies holds the concrete pluggable bits Server.New needs. Production
// callers leave it zero and Server.New builds the defaults from Config; tests
// stamp the fields directly so they never touch a real Postgres / EntDB / etc.
//
// Every field is optional. NewWithDeps documents the resolution order so a
// test that overrides just the Store still gets the in-app provider for free.
type Dependencies struct {
	// Store is the durable backend. When nil, NewWithDeps builds one from
	// Config.Store (memory / postgres / entdb).
	Store notify.Store
	// StoreCloser, when non-nil, is invoked on Shutdown so the test can
	// observe its store being released.
	StoreCloser StoreCloser
	// Providers, when non-nil, replaces the default registry. Wiring an
	// explicit *ProviderRegistry from a test removes the need to set any
	// channel-block env vars.
	Providers *notify.ProviderRegistry
	// AuthValidator overrides the JWT validator; used by tests that issue
	// fake-signed tokens. In production it is built from Config.Auth.
	AuthValidator AuthValidator
	// Clock overrides the time source the notifier and stream handler use.
	Clock func() time.Time
	// Logger overrides the slog logger; nil → newJSONLogger at Config level.
	Logger *slog.Logger
}

// Server is a running notify daemon. It owns a *notify.Notifier, three
// http.Servers (client, internal, metrics), an in-memory realtime engine
// (when enabled) and the lifecycle plumbing that ties them together.
type Server struct {
	cfg    Config
	logger *slog.Logger

	notifier *notify.Notifier
	store    notify.Store
	closer   StoreCloser

	registry *realtime.Registry[*notifyv1.StreamEvent]
	retries  *realtime.RetryTracker

	auth         AuthValidator
	clientServer *http.Server
	internalSrv  *http.Server
	metricsSrv   *http.Server

	health *healthState

	// Listeners are kept separately so Run can swap them out for
	// httptest-driven listeners. clientListener is the only one a test
	// reaches into; the others are constructed lazily.
	clientListener   net.Listener
	internalListener net.Listener
	metricsListener  net.Listener
}

// New builds a Server from Config, constructing every dependency from
// configuration. Test code should use NewWithDeps and stamp the bits it
// cares about.
func New(cfg Config) (*Server, error) {
	return NewWithDeps(cfg, Dependencies{})
}

// NewWithDeps is the testable constructor: it accepts a partially-stamped
// Dependencies and fills in the gaps from Config. The resolution order is
// described per-field; nothing magical happens beyond "deps wins over env".
func NewWithDeps(cfg Config, deps Dependencies) (*Server, error) {
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	if deps.Logger == nil {
		deps.Logger = newJSONLogger(cfg.SlogLevel())
	}
	logger := deps.Logger

	store := deps.Store
	closer := deps.StoreCloser
	if store == nil {
		s, c, err := buildStore(cfg.Store)
		if err != nil {
			return nil, fmt.Errorf("server: build store: %w", err)
		}
		store = s
		if closer == nil {
			closer = c
		}
	}

	auth := deps.AuthValidator
	if auth == nil {
		a, err := buildValidator(cfg.Auth)
		if err != nil {
			return nil, fmt.Errorf("server: build auth: %w", err)
		}
		auth = a
	}

	providers := deps.Providers
	if providers == nil {
		providers = notify.NewProviderRegistry()
	}

	var (
		registry *realtime.Registry[*notifyv1.StreamEvent]
		retries  *realtime.RetryTracker
	)
	if cfg.LiveConnections.Enabled {
		registry = realtime.NewRegistry[*notifyv1.StreamEvent](logger)
		retries = realtime.NewRetryTracker(cfg.LiveConnections.RetryMaxAttempts, cfg.LiveConnections.RetryInterval, logger)
		// Only register the in-app provider when the orchestrator was not
		// already given one by the caller — tests sometimes register their
		// own fake.
		if _, ok := providers.Get(notify.ChannelInApp); !ok {
			providers.Register(newInAppProvider(registry))
		}
	}

	notifier := notify.NewNotifier(store, providers, notify.WithClock(deps.Clock))

	s := &Server{
		cfg:      cfg,
		logger:   logger,
		notifier: notifier,
		store:    store,
		closer:   closer,
		registry: registry,
		retries:  retries,
		auth:     auth,
		health:   newHealthState(),
	}

	clientHandler := newClientHandler(store, s.maybeStreamHandler(deps.Clock), func() int64 {
		return deps.Clock().UnixMilli()
	})
	internalHandler := newInternalHandler(notifier)

	// HTTP servers + listeners.
	s.clientServer = s.buildClientServer(clientHandler, auth)
	s.internalSrv = s.buildInternalServer(internalHandler, auth)
	s.metricsSrv = &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.MetricsPort),
		Handler:           newMetricsMux(s.health),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s, nil
}

// maybeStreamHandler returns a *streamHandler when live connections are
// enabled, nil otherwise. nil signals StreamEvents to return Unimplemented.
func (s *Server) maybeStreamHandler(clock func() time.Time) *streamHandler {
	if s.registry == nil {
		return nil
	}
	return newStreamHandler(s.registry, s.retries, streamHandlerOptions{
		heartbeatInterval: s.cfg.LiveConnections.HeartbeatInterval,
		bufferSize:        DefaultEventBufferPerConn,
		logger:            s.logger,
		clock:             clock,
	})
}

// buildClientServer wires the NotificationClientService handler with the
// unary auth + logging + recovery interceptors, returning a ready-to-serve
// http.Server. The mux is h2c-wrapped so the container can speak HTTP/2
// without TLS termination at the process boundary (a reverse proxy / mesh
// handles TLS in production deployments).
func (s *Server) buildClientServer(handler *clientHandler, auth AuthValidator) *http.Server {
	shim := newStreamAuthShim(handler, auth)

	interceptors := connect.WithInterceptors(
		newRecoveryInterceptor(s.logger),
		newLoggingInterceptor(s.logger, time.Now),
		NewClientAuthInterceptor(auth),
	)
	path, h := notifyv1connect.NewNotificationClientServiceHandler(shim, interceptors)

	mux := http.NewServeMux()
	mux.Handle(path, h)

	return &http.Server{
		Addr:              ":" + strconv.Itoa(s.cfg.ClientPort),
		Handler:           h2c.NewHandler(mux, &http2.Server{}),
		ReadHeaderTimeout: 10 * time.Second,
	}
}

// buildInternalServer wires NotificationInternalService with the internal-
// token interceptor + the standard logging/recovery pair.
func (s *Server) buildInternalServer(handler *internalHandler, _ AuthValidator) *http.Server {
	interceptors := connect.WithInterceptors(
		newRecoveryInterceptor(s.logger),
		newLoggingInterceptor(s.logger, time.Now),
		NewInternalAuthInterceptor(s.cfg.Auth.InternalToken, s.cfg.Auth.DevMode),
	)
	path, h := notifyv1connect.NewNotificationInternalServiceHandler(handler, interceptors)

	mux := http.NewServeMux()
	mux.Handle(path, h)

	return &http.Server{
		Addr:              ":" + strconv.Itoa(s.cfg.InternalPort),
		Handler:           h2c.NewHandler(mux, &http2.Server{}),
		ReadHeaderTimeout: 10 * time.Second,
	}
}

// Run starts every listener and blocks until ctx is canceled (or a SIGINT /
// SIGTERM arrives). On exit Shutdown is invoked with the configured timeout
// and the function returns any error encountered.
//
// In tests, callers should pass a context they cancel themselves; in
// production main() passes context.Background() and we wire signal handling
// here so the binary works the same way on any platform.
func (s *Server) Run(ctx context.Context) error {
	// Wire signal handling here so cmd/notifyd stays at ~10 lines.
	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := s.bindListeners(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	startServer := func(name string, srv *http.Server, ln net.Listener) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.logger.Info("server_listen", "name", name, "addr", ln.Addr().String())
			err := srv.Serve(ln)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("%s: %w", name, err)
			}
		}()
	}
	startServer("client", s.clientServer, s.clientListener)
	startServer("internal", s.internalSrv, s.internalListener)
	startServer("metrics", s.metricsSrv, s.metricsListener)

	s.health.markReady()

	// Wait for context cancellation OR a fatal listener error.
	select {
	case <-signalCtx.Done():
		s.logger.Info("server_shutdown_signal")
	case err := <-errCh:
		s.logger.Error("server_listener_failed", "error", err.Error())
		_ = s.Shutdown(context.Background())
		return err
	}

	if err := s.Shutdown(context.Background()); err != nil {
		s.logger.Warn("server_shutdown_error", "error", err.Error())
		return err
	}
	wg.Wait()
	return nil
}

// Addr returns the live client-server listener address, or the empty string
// when Run has not yet bound it. Tests use this to compose Connect clients
// after starting the server; a "" return is the sentinel that Run hasn't been
// reached, never a panic.
func (s *Server) Addr() string {
	if s.clientListener == nil {
		return ""
	}
	return s.clientListener.Addr().String()
}

// MetricsAddr returns the live metrics-listener address (or "" if not bound).
func (s *Server) MetricsAddr() string {
	if s.metricsListener == nil {
		return ""
	}
	return s.metricsListener.Addr().String()
}

// InternalAddr returns the live internal-service listener address.
func (s *Server) InternalAddr() string {
	if s.internalListener == nil {
		return ""
	}
	return s.internalListener.Addr().String()
}

// bindListeners creates real TCP listeners for each configured port. Tests
// can override these by stamping non-nil listeners onto the Server before
// calling Run (see UseTestListeners).
func (s *Server) bindListeners() error {
	if s.clientListener == nil {
		ln, err := net.Listen("tcp", s.clientServer.Addr)
		if err != nil {
			return fmt.Errorf("client listener: %w", err)
		}
		s.clientListener = ln
	}
	if s.internalListener == nil {
		ln, err := net.Listen("tcp", s.internalSrv.Addr)
		if err != nil {
			_ = s.clientListener.Close()
			return fmt.Errorf("internal listener: %w", err)
		}
		s.internalListener = ln
	}
	if s.metricsListener == nil {
		ln, err := net.Listen("tcp", s.metricsSrv.Addr)
		if err != nil {
			_ = s.clientListener.Close()
			_ = s.internalListener.Close()
			return fmt.Errorf("metrics listener: %w", err)
		}
		s.metricsListener = ln
	}
	return nil
}

// UseTestListeners swaps the production TCP listeners for test-supplied ones.
// Pass nil for any listener to keep the production behaviour for that port.
// Calling this AFTER bindListeners has run is a no-op for the slots already
// bound; tests always call it before Run.
func (s *Server) UseTestListeners(client, internal, metrics net.Listener) {
	if client != nil {
		s.clientListener = client
	}
	if internal != nil {
		s.internalListener = internal
	}
	if metrics != nil {
		s.metricsListener = metrics
	}
}

// Shutdown gracefully stops all three http.Servers, cancels any in-flight
// retries, and closes the store. The configured ShutdownTimeout caps the
// total time spent here. Safe to call multiple times.
func (s *Server) Shutdown(ctx context.Context) error {
	s.health.markNotReady()

	timeout := s.cfg.ShutdownTimeout
	if timeout <= 0 {
		timeout = DefaultShutdownTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var firstErr error
	for _, srv := range []*http.Server{s.clientServer, s.internalSrv, s.metricsSrv} {
		if srv == nil {
			continue
		}
		if err := srv.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.retries != nil {
		s.retries.CancelAll()
	}
	if s.closer != nil {
		if err := s.closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// buildStore resolves the configured driver into a concrete notify.Store and a
// matching Closer. Memory has no Close; postgres + entdb do (their lifecycle
// matters because TCP connections leak otherwise).
func buildStore(cfg notify.StoreConfig) (notify.Store, StoreCloser, error) {
	switch cfg.Driver {
	case "memory":
		return memory.New(), nil, nil
	case "postgres":
		// Importing the postgres driver from this constructor would pull in
		// pgx + testcontainers into every binary that imports the server
		// package, even when those binaries pick the memory driver. The
		// container's main() instead constructs the driver via the small
		// adapter exposed by the cmd package and passes it via Dependencies.
		// The dual-mode template makes this explicit in the error message.
		return nil, nil, fmt.Errorf("server: postgres driver: configure via Dependencies.Store; cmd/notifyd wires it")
	case "entdb":
		return nil, nil, fmt.Errorf("server: entdb driver: configure via Dependencies.Store; cmd/notifyd wires it")
	default:
		return nil, nil, fmt.Errorf("server: unknown store driver %q", cfg.Driver)
	}
}

// buildValidator picks the right AuthValidator from configuration.
func buildValidator(cfg AuthConfig) (AuthValidator, error) {
	if cfg.JWTSecret != "" {
		v, err := NewJWTValidator(cfg.JWTSecret, cfg.JWTIssuer, cfg.JWTAudience, cfg.JWTLeeway)
		if err != nil {
			return nil, err
		}
		return v, nil
	}
	if cfg.DevMode {
		return DevValidator{}, nil
	}
	return nil, errors.New("auth: jwt secret required (or set NOTIFY_AUTH_DEV_MODE=true)")
}

// CloseFuncFor adapts a parameterless Close into the StoreCloser shape. It is
// exported so cmd/notifyd (and tests) can register cleanup for stores that do
// not directly satisfy the StoreCloser interface.
func CloseFuncFor(f func() error) StoreCloser {
	return closeFunc(f)
}
