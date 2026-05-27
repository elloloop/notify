package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
	"github.com/elloloop/notify/realtime"
)

// streamHandler is the StreamEvents server-streaming impl. It is constructed
// once at server start and shared by every concurrent stream — concurrency is
// safe because the registry / retry tracker are already goroutine-safe and
// the handler itself stamps no per-connection state on its receiver.
type streamHandler struct {
	registry          *realtime.Registry[*notifyv1.StreamEvent]
	retries           *realtime.RetryTracker
	heartbeatInterval time.Duration
	bufferSize        int
	logger            *slog.Logger
	clock             func() time.Time
}

// streamHandlerOptions exposes the few knobs tests need to override (clock,
// heartbeat interval) without changing the production constructor surface.
type streamHandlerOptions struct {
	heartbeatInterval time.Duration
	bufferSize        int
	logger            *slog.Logger
	clock             func() time.Time
}

func newStreamHandler(reg *realtime.Registry[*notifyv1.StreamEvent], retries *realtime.RetryTracker, opts streamHandlerOptions) *streamHandler {
	if opts.heartbeatInterval <= 0 {
		opts.heartbeatInterval = DefaultHeartbeatInterval
	}
	if opts.bufferSize <= 0 {
		opts.bufferSize = DefaultEventBufferPerConn
	}
	if opts.logger == nil {
		opts.logger = slog.Default()
	}
	if opts.clock == nil {
		opts.clock = time.Now
	}
	return &streamHandler{
		registry:          reg,
		retries:           retries,
		heartbeatInterval: opts.heartbeatInterval,
		bufferSize:        opts.bufferSize,
		logger:            opts.logger,
		clock:             opts.clock,
	}
}

// serve is the StreamEvents body. The sequence is deliberately tight:
//
//  1. Read claims from ctx (the auth middleware put them there).
//  2. Build a Conn and register it under the user.
//  3. Emit one handshake heartbeat carrying the session_id so the client
//     can finish its end of the handshake without waiting for traffic.
//  4. Tick the heartbeat + pump events from Conn.EventCh until ctx
//     cancels or Send returns an error.
//  5. Unregister the connection on the way out, no matter how we exit.
//
// The function is a single goroutine — fan-out lives in the registry, and we
// only read from one channel here.
func (h *streamHandler) serve(
	ctx context.Context,
	req *connect.Request[notifyv1.StreamEventsRequest],
	stream *connect.ServerStream[notifyv1.StreamEvent],
) error {
	claims, ok := ClaimsFromContext(ctx)
	if !ok {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("notify: claims missing on context"))
	}

	deviceType := deviceTypeFromProto(req.Msg.GetDeviceType())
	conn := realtime.NewConn[*notifyv1.StreamEvent](claims.UserID, claims.TenantID, deviceType, h.bufferSize)
	h.registry.Register(conn)
	defer h.registry.Unregister(conn.ID)

	h.logger.Info("stream_open",
		"connection_id", conn.ID,
		"user_id", claims.UserID,
		"tenant_id", claims.TenantID,
		"device_type", deviceType,
	)
	defer h.logger.Info("stream_close",
		"connection_id", conn.ID,
		"user_id", claims.UserID,
	)

	// Initial handshake heartbeat — carries the server-assigned session_id so
	// the client can target AckDataChange back at this exact connection.
	if err := stream.Send(h.heartbeat(conn.ID)); err != nil {
		return err
	}

	// Heartbeat ticker. New on every call because each stream has its own
	// emit cadence; the registry is shared, but the timer is not.
	ticker := time.NewTicker(h.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Client cancel or server shutdown. Returning a non-nil error for
			// ctx.Done() is the connect-go contract; the caller's gRPC code
			// surfaces it as CANCELED.
			return ctx.Err()
		case <-ticker.C:
			if err := stream.Send(h.heartbeat(conn.ID)); err != nil {
				return err
			}
		case ev, ok := <-conn.EventCh:
			if !ok {
				// EventCh is never closed in practice (see realtime/conn.go's
				// invariant comment); a closed channel here is a defensive
				// exit, not a normal code path.
				return nil
			}
			ev.SessionId = conn.ID
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
}

// heartbeat builds a StreamEvent carrying the current server clock and the
// connection's session_id. Centralising the construction here means tests can
// override h.clock and the wire shape stays consistent.
func (h *streamHandler) heartbeat(sessionID string) *notifyv1.StreamEvent {
	return &notifyv1.StreamEvent{
		SessionId: sessionID,
		Event: &notifyv1.StreamEvent_Heartbeat{
			Heartbeat: &notifyv1.HeartbeatEvent{
				TimestampMs: h.clock().UnixMilli(),
			},
		},
	}
}
