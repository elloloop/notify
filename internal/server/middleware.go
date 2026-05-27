package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"connectrpc.com/connect"

	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
)

// loggingInterceptor logs every unary RPC call: procedure, user_id when
// authenticated, latency, and the resulting status (OK / connect code). The
// log line is intentionally low-cardinality so it survives long-tail
// ingestion.
type loggingInterceptor struct {
	logger *slog.Logger
	clock  func() time.Time
}

func newLoggingInterceptor(logger *slog.Logger, clock func() time.Time) loggingInterceptor {
	if logger == nil {
		logger = slog.Default()
	}
	if clock == nil {
		clock = time.Now
	}
	return loggingInterceptor{logger: logger, clock: clock}
}

func (l loggingInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		start := l.clock()
		resp, err := next(ctx, req)
		latency := l.clock().Sub(start)

		userID := ""
		if c, ok := ClaimsFromContext(ctx); ok {
			userID = c.UserID
		}

		fields := []any{
			"procedure", req.Spec().Procedure,
			"latency_ms", latency.Milliseconds(),
			"user_id", userID,
		}
		if err != nil {
			code := connect.CodeOf(err)
			fields = append(fields, "code", code.String(), "error", err.Error())
			l.logger.Warn("rpc", fields...)
			return resp, err
		}
		l.logger.Info("rpc", fields...)
		return resp, err
	}
}

// WrapStreamingClient is unused server-side but required by the
// connect.Interceptor interface.
func (l loggingInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// WrapStreamingHandler logs the streaming RPC after it returns. We do not
// log per-message because that explodes log volume on long-lived streams.
func (l loggingInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		start := l.clock()
		err := next(ctx, conn)
		latency := l.clock().Sub(start)
		userID := ""
		if c, ok := ClaimsFromContext(ctx); ok {
			userID = c.UserID
		}
		fields := []any{
			"procedure", conn.Spec().Procedure,
			"latency_ms", latency.Milliseconds(),
			"user_id", userID,
			"stream", true,
		}
		if err != nil {
			code := connect.CodeOf(err)
			fields = append(fields, "code", code.String(), "error", err.Error())
			l.logger.Warn("rpc", fields...)
			return err
		}
		l.logger.Info("rpc", fields...)
		return nil
	}
}

// recoveryInterceptor turns any panic from a handler into a connect.CodeInternal
// error so the process keeps serving requests. Panics are logged with the
// stack so post-mortem analysis is possible.
type recoveryInterceptor struct {
	logger *slog.Logger
}

func newRecoveryInterceptor(logger *slog.Logger) recoveryInterceptor {
	if logger == nil {
		logger = slog.Default()
	}
	return recoveryInterceptor{logger: logger}
}

func (r recoveryInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (resp connect.AnyResponse, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				r.logger.Error("rpc_panic",
					"procedure", req.Spec().Procedure,
					"panic", fmt.Sprint(rec),
					"stack", string(debug.Stack()),
				)
				err = connect.NewError(connect.CodeInternal, errors.New("internal server error"))
			}
		}()
		return next(ctx, req)
	}
}

// WrapStreamingClient passes through; client-side interceptors are not used.
func (r recoveryInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// WrapStreamingHandler installs the same recover() on streaming handlers so a
// panic inside StreamEvents does not crash the process.
func (r recoveryInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				r.logger.Error("rpc_panic",
					"procedure", conn.Spec().Procedure,
					"panic", fmt.Sprint(rec),
					"stack", string(debug.Stack()),
				)
				err = connect.NewError(connect.CodeInternal, errors.New("internal server error"))
			}
		}()
		return next(ctx, conn)
	}
}

// streamAuthShim adapts the client-facing service so the streaming handler
// can pull Claims off the context. Connect's UnaryInterceptorFunc does not run
// against streaming RPCs, so streaming auth happens here at the handler edge.
type streamAuthShim struct {
	inner *clientHandler
	auth  AuthValidator
}

func newStreamAuthShim(inner *clientHandler, auth AuthValidator) *streamAuthShim {
	return &streamAuthShim{inner: inner, auth: auth}
}

// StreamEvents authenticates the streaming request and then delegates.
func (s *streamAuthShim) StreamEvents(
	ctx context.Context,
	req *connect.Request[notifyv1.StreamEventsRequest],
	stream *connect.ServerStream[notifyv1.StreamEvent],
) error {
	ctx, err := authenticateStreamReq(ctx, s.auth, req.Header().Get("Authorization"))
	if err != nil {
		return err
	}
	return s.inner.StreamEvents(ctx, req, stream)
}

// The unary RPCs go through the unary auth interceptor — the shim only needs
// to forward them.
func (s *streamAuthShim) GetNotifications(
	ctx context.Context,
	req *connect.Request[notifyv1.GetNotificationsRequest],
) (*connect.Response[notifyv1.GetNotificationsResponse], error) {
	return s.inner.GetNotifications(ctx, req)
}

func (s *streamAuthShim) AckNotification(
	ctx context.Context,
	req *connect.Request[notifyv1.AckNotificationRequest],
) (*connect.Response[notifyv1.AckNotificationResponse], error) {
	return s.inner.AckNotification(ctx, req)
}

func (s *streamAuthShim) AckDataChange(
	ctx context.Context,
	req *connect.Request[notifyv1.AckDataChangeRequest],
) (*connect.Response[notifyv1.AckDataChangeResponse], error) {
	return s.inner.AckDataChange(ctx, req)
}

func (s *streamAuthShim) RegisterPushToken(
	ctx context.Context,
	req *connect.Request[notifyv1.RegisterPushTokenRequest],
) (*connect.Response[notifyv1.RegisterPushTokenResponse], error) {
	return s.inner.RegisterPushToken(ctx, req)
}
