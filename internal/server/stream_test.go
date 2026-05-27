package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
	"github.com/elloloop/notify/gen/go/notify/v1/notifyv1connect"
	"github.com/elloloop/notify/realtime"
)

// streamTestRig spins up a real Connect server bound to an httptest listener
// and returns a client + the streamHandler so tests can assert against both
// sides without standing up the full Server. The h2c wrapper is critical:
// connect-go's server-streaming RPCs require HTTP/2 even on plaintext.
type streamTestRig struct {
	server  *httptest.Server
	client  notifyv1connect.NotificationClientServiceClient
	stream  *streamHandler
	reg     *realtime.Registry[*notifyv1.StreamEvent]
	retries *realtime.RetryTracker
}

func newStreamTestRig(t *testing.T, heartbeat time.Duration) *streamTestRig {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	reg := realtime.NewRegistry[*notifyv1.StreamEvent](logger)
	retries := realtime.NewRetryTracker(0, time.Second, logger)
	sh := newStreamHandler(reg, retries, streamHandlerOptions{
		heartbeatInterval: heartbeat,
		bufferSize:        16,
		logger:            logger,
	})

	ch := newClientHandler(&fakeStore{}, sh, fixedClock(0))
	shim := newStreamAuthShim(ch, DevValidator{})

	mux := http.NewServeMux()
	path, h := notifyv1connect.NewNotificationClientServiceHandler(shim,
		connect.WithInterceptors(NewClientAuthInterceptor(DevValidator{})))
	mux.Handle(path, h)
	srv := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	// srv.Client() trusts the test server's TLS cert and speaks HTTP/2
	// natively when EnableHTTP2 is set; the resulting Connect client can do
	// server-streaming RPCs end-to-end.
	client := notifyv1connect.NewNotificationClientServiceClient(srv.Client(), srv.URL)

	return &streamTestRig{server: srv, client: client, stream: sh, reg: reg, retries: retries}
}

func TestStream_InitialHeartbeatCarriesSessionID(t *testing.T) {
	rig := newStreamTestRig(t, time.Hour) // far-future heartbeat
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := connect.NewRequest(&notifyv1.StreamEventsRequest{
		DeviceType: notifyv1.DeviceType_DEVICE_TYPE_BROWSER,
	})
	req.Header().Set("Authorization", "Bearer dev:alice:tenant-1")

	stream, err := rig.client.StreamEvents(ctx, req)
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	if !stream.Receive() {
		t.Fatalf("Receive returned false: %v", stream.Err())
	}
	ev := stream.Msg()
	if ev.GetSessionId() == "" {
		t.Fatalf("session id empty on handshake event")
	}
	if ev.GetHeartbeat() == nil {
		t.Fatalf("first event should be a heartbeat, got %+v", ev)
	}
	if ev.GetHeartbeat().GetTimestampMs() == 0 {
		t.Fatalf("heartbeat timestamp_ms = 0")
	}
}

func TestStream_RegistryDeliversEvent(t *testing.T) {
	rig := newStreamTestRig(t, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := connect.NewRequest(&notifyv1.StreamEventsRequest{
		DeviceType: notifyv1.DeviceType_DEVICE_TYPE_BROWSER,
	})
	req.Header().Set("Authorization", "Bearer dev:alice:tenant-1")

	stream, err := rig.client.StreamEvents(ctx, req)
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	// Drain the initial heartbeat.
	if !stream.Receive() {
		t.Fatalf("first recv: %v", stream.Err())
	}

	// Wait for the connection to register, then push an event.
	if err := waitForCondition(2*time.Second, func() bool {
		return rig.reg.Count("alice") == 1
	}); err != nil {
		t.Fatalf("registration: %v", err)
	}

	pushed := rig.reg.Push("alice", &notifyv1.StreamEvent{
		Event: &notifyv1.StreamEvent_Notification{
			Notification: &notifyv1.NotificationEvent{
				Notification: &notifyv1.Notification{Id: "n-1", Title: "ping"},
			},
		},
	})
	if pushed != 1 {
		t.Fatalf("Push returned %d, want 1", pushed)
	}

	if !stream.Receive() {
		t.Fatalf("recv pushed event: %v", stream.Err())
	}
	got := stream.Msg()
	if got.GetNotification() == nil || got.GetNotification().GetNotification().GetId() != "n-1" {
		t.Fatalf("event = %+v", got)
	}
	if got.GetSessionId() == "" {
		t.Fatalf("session id missing")
	}
}

func TestStream_HeartbeatTickerEmits(t *testing.T) {
	rig := newStreamTestRig(t, 30*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := connect.NewRequest(&notifyv1.StreamEventsRequest{
		DeviceType: notifyv1.DeviceType_DEVICE_TYPE_BROWSER,
	})
	req.Header().Set("Authorization", "Bearer dev:bob:t2")

	stream, err := rig.client.StreamEvents(ctx, req)
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	// First heartbeat is the handshake; the second is the ticker fire.
	for i := 0; i < 2; i++ {
		if !stream.Receive() {
			t.Fatalf("heartbeat %d: %v", i, stream.Err())
		}
		if stream.Msg().GetHeartbeat() == nil {
			t.Fatalf("expected heartbeat, got %+v", stream.Msg())
		}
	}
}

func TestStream_ClientCancelUnregisters(t *testing.T) {
	rig := newStreamTestRig(t, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())

	req := connect.NewRequest(&notifyv1.StreamEventsRequest{
		DeviceType: notifyv1.DeviceType_DEVICE_TYPE_BROWSER,
	})
	req.Header().Set("Authorization", "Bearer dev:carol:t3")

	stream, err := rig.client.StreamEvents(ctx, req)
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	if !stream.Receive() {
		t.Fatalf("recv handshake: %v", stream.Err())
	}

	if err := waitForCondition(2*time.Second, func() bool {
		return rig.reg.Count("carol") == 1
	}); err != nil {
		t.Fatalf("registration: %v", err)
	}

	cancel()
	_ = stream.Close()

	if err := waitForCondition(2*time.Second, func() bool {
		return rig.reg.Count("carol") == 0
	}); err != nil {
		t.Fatalf("unregister: %v", err)
	}
}

func TestStream_NoClaims(t *testing.T) {
	// The streamAuthShim path is used by the server, but we can exercise
	// the streamHandler's no-claims branch directly without the network.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := realtime.NewRegistry[*notifyv1.StreamEvent](logger)
	sh := newStreamHandler(reg, realtime.NewRetryTracker(0, time.Second, logger), streamHandlerOptions{
		heartbeatInterval: time.Hour,
		bufferSize:        4,
		logger:            logger,
	})

	err := sh.serve(context.Background(), connect.NewRequest(&notifyv1.StreamEventsRequest{}), nil)
	if err == nil || connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("err=%v code=%v", err, connect.CodeOf(err))
	}
}

func TestStream_BackpressureDropsWithoutBlocking(t *testing.T) {
	// Drive the registry directly with a tiny buffer; trySend should drop
	// events once the buffer is full and never block the pusher.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := realtime.NewRegistry[*notifyv1.StreamEvent](logger)
	conn := realtime.NewConn[*notifyv1.StreamEvent]("u", "t", "browser", 2)
	reg.Register(conn)
	t.Cleanup(func() { reg.Unregister(conn.ID) })

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Push 10 events into a buffer of 2 — should not deadlock.
		for i := 0; i < 10; i++ {
			reg.Push("u", &notifyv1.StreamEvent{SessionId: "x"})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("push deadlocked under backpressure")
	}

	// Buffer holds exactly its capacity; nothing was sent past the limit.
	if got := len(conn.EventCh); got > 2 {
		t.Fatalf("buffer length = %d, want <= 2", got)
	}
}

// waitForCondition polls cond at 5ms intervals until it returns true or the
// deadline elapses. It is the smallest pattern that keeps stream tests
// deterministic without the package depending on testify.
func waitForCondition(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	if cond() {
		return nil
	}
	return errors.New("timed out waiting for condition")
}

