package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
)

func newCapturingLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(handler), buf
}

// readLogRecords splits the buffer on newline and parses each line as a
// structured log record so tests can assert on individual fields without
// fussing with regex.
func readLogRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("parse log line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

func TestLoggingInterceptor_LogsSuccess(t *testing.T) {
	logger, buf := newCapturingLogger()
	interceptor := newLoggingInterceptor(logger, time.Now)

	next := connect.UnaryFunc(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		return connect.NewResponse(&notifyv1.AckNotificationResponse{}), nil
	})

	ctx := withClaims(context.Background(), Claims{UserID: "u-1"})
	req := newFakeUnaryReq(nil)
	if _, err := interceptor.WrapUnary(next)(ctx, req); err != nil {
		t.Fatalf("interceptor: %v", err)
	}

	recs := readLogRecords(t, buf)
	if len(recs) != 1 {
		t.Fatalf("expected 1 log record, got %d", len(recs))
	}
	r := recs[0]
	if r["msg"] != "rpc" {
		t.Fatalf("msg = %v", r["msg"])
	}
	if r["user_id"] != "u-1" {
		t.Fatalf("user_id = %v", r["user_id"])
	}
	if r["procedure"] == nil {
		t.Fatalf("procedure missing")
	}
	if _, ok := r["latency_ms"]; !ok {
		t.Fatalf("latency_ms missing: %+v", r)
	}
}

func TestLoggingInterceptor_LogsError(t *testing.T) {
	logger, buf := newCapturingLogger()
	interceptor := newLoggingInterceptor(logger, time.Now)
	next := connect.UnaryFunc(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("dead"))
	})
	if _, err := interceptor.WrapUnary(next)(context.Background(), newFakeUnaryReq(nil)); err == nil {
		t.Fatal("expected error")
	}
	recs := readLogRecords(t, buf)
	if len(recs) != 1 {
		t.Fatalf("recs = %d", len(recs))
	}
	if recs[0]["code"] != "unavailable" {
		t.Fatalf("code = %v", recs[0]["code"])
	}
}

func TestRecoveryInterceptor_CatchesPanic(t *testing.T) {
	logger, buf := newCapturingLogger()
	interceptor := newRecoveryInterceptor(logger)
	next := connect.UnaryFunc(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		panic("kaboom")
	})
	_, err := interceptor.WrapUnary(next)(context.Background(), newFakeUnaryReq(nil))
	if err == nil {
		t.Fatal("expected error")
	}
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("code = %v, want internal", connect.CodeOf(err))
	}
	if !strings.Contains(buf.String(), "rpc_panic") {
		t.Fatalf("expected rpc_panic log, got %q", buf.String())
	}
}

func TestRecoveryInterceptor_NoPanicPassesThrough(t *testing.T) {
	logger, _ := newCapturingLogger()
	interceptor := newRecoveryInterceptor(logger)
	called := false
	next := connect.UnaryFunc(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		called = true
		return nil, nil
	})
	if _, err := interceptor.WrapUnary(next)(context.Background(), newFakeUnaryReq(nil)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("next was not invoked")
	}
}

func TestLoggingInterceptor_NilLoggerFallback(t *testing.T) {
	// Pass nil logger; constructor should substitute slog.Default()
	// without panicking.
	interceptor := newLoggingInterceptor(nil, nil)
	next := connect.UnaryFunc(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		return connect.NewResponse(&notifyv1.AckNotificationResponse{}), nil
	})
	if _, err := interceptor.WrapUnary(next)(context.Background(), newFakeUnaryReq(nil)); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
}

func TestRecoveryInterceptor_StreamingHandlerCatchesPanic(t *testing.T) {
	logger, buf := newCapturingLogger()
	interceptor := newRecoveryInterceptor(logger)
	next := connect.StreamingHandlerFunc(func(_ context.Context, _ connect.StreamingHandlerConn) error {
		panic("stream-kaboom")
	})
	err := interceptor.WrapStreamingHandler(next)(context.Background(), &fakeStreamConn{})
	if err == nil {
		t.Fatal("expected error")
	}
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("code = %v", connect.CodeOf(err))
	}
	if !strings.Contains(buf.String(), "rpc_panic") {
		t.Fatalf("expected rpc_panic log")
	}
}

// fakeStreamConn is the tiniest connect.StreamingHandlerConn implementation
// that makes the recovery interceptor's panic-on-streaming path testable
// without spinning up an HTTP server. The interceptor only ever calls
// Spec() on the conn, so the rest of the methods return zero values.
type fakeStreamConn struct{}

func (fakeStreamConn) Spec() connect.Spec {
	return connect.Spec{Procedure: "/test/Stream"}
}
func (fakeStreamConn) Peer() connect.Peer          { return connect.Peer{} }
func (fakeStreamConn) Receive(any) error           { return errors.New("not implemented") }
func (fakeStreamConn) RequestHeader() http.Header  { return http.Header{} }
func (fakeStreamConn) Send(any) error              { return errors.New("not implemented") }
func (fakeStreamConn) ResponseHeader() http.Header { return http.Header{} }
func (fakeStreamConn) ResponseTrailer() http.Header {
	return http.Header{}
}
