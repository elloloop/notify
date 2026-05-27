package server

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/elloloop/notify"
	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
	"github.com/elloloop/notify/realtime"
)

func TestInAppProvider_KindAndName(t *testing.T) {
	p := newInAppProvider(nil)
	if p.Kind() != notify.ChannelInApp {
		t.Fatalf("Kind = %q", p.Kind())
	}
	if p.Name() == "" {
		t.Fatal("Name empty")
	}
}

func TestInAppProvider_SendNoRegistry(t *testing.T) {
	p := newInAppProvider(nil)
	r, err := p.Send(context.Background(), notify.Message{
		Notification: &notify.Notification{ID: "n", Title: "t"},
		To:           "u",
	})
	if err == nil {
		t.Fatal("expected error with nil registry")
	}
	if r.Status != notify.StatusFailed {
		t.Fatalf("status = %q", r.Status)
	}
}

func TestInAppProvider_SendNoNotification(t *testing.T) {
	reg := realtime.NewRegistry[*notifyv1.StreamEvent](slog.New(slog.NewTextHandler(io.Discard, nil)))
	p := newInAppProvider(reg)
	r, err := p.Send(context.Background(), notify.Message{To: "u"})
	if err == nil {
		t.Fatal("expected error for missing notification")
	}
	if r.Status != notify.StatusFailed {
		t.Fatalf("status = %q", r.Status)
	}
}

func TestInAppProvider_SendPushesToRegistry(t *testing.T) {
	reg := realtime.NewRegistry[*notifyv1.StreamEvent](slog.New(slog.NewTextHandler(io.Discard, nil)))
	conn := realtime.NewConn[*notifyv1.StreamEvent]("u1", "t1", "browser", 4)
	reg.Register(conn)
	t.Cleanup(func() { reg.Unregister(conn.ID) })

	p := newInAppProvider(reg)
	r, err := p.Send(context.Background(), notify.Message{
		Notification: &notify.Notification{
			ID: "row-1", Title: "hello", Body: "world",
			Channel: notify.ChannelInApp, Status: notify.StatusDelivered,
		},
		To: "u1",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if r.Status != notify.StatusDelivered {
		t.Fatalf("status = %q, want Delivered (1 live connection)", r.Status)
	}

	select {
	case ev := <-conn.EventCh:
		if ev.GetNotification() == nil {
			t.Fatalf("expected NotificationEvent, got %+v", ev)
		}
		if ev.GetNotification().GetNotification().GetTitle() != "hello" {
			t.Fatalf("title not propagated: %+v", ev)
		}
	default:
		t.Fatal("no event delivered")
	}
}

// TestInAppProvider_SendNoLiveConnections — when the recipient has zero
// connections registered, the row must NOT be marked Delivered. Returning
// Pending keeps the lifecycle honest: the user has not seen this notification
// yet, and the catch-up path (GetNotifications + the UnreadFilterAndCount
// conformance subtest) depends on Pending = unread.
func TestInAppProvider_SendNoLiveConnections(t *testing.T) {
	reg := realtime.NewRegistry[*notifyv1.StreamEvent](slog.New(slog.NewTextHandler(io.Discard, nil)))
	p := newInAppProvider(reg)
	r, err := p.Send(context.Background(), notify.Message{
		Notification: &notify.Notification{ID: "row-2", Title: "offline"},
		To:           "u-without-any-connections",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if r.Status != notify.StatusPending {
		t.Fatalf("status = %q, want Pending (0 live connections)", r.Status)
	}
}

// TestInAppProvider_SendMultipleConnectionsAllDelivered — fan-out to N>1
// connections, all of which receive the same event. Status is Delivered.
func TestInAppProvider_SendMultipleConnectionsAllDelivered(t *testing.T) {
	reg := realtime.NewRegistry[*notifyv1.StreamEvent](slog.New(slog.NewTextHandler(io.Discard, nil)))
	conns := []*realtime.Conn[*notifyv1.StreamEvent]{
		realtime.NewConn[*notifyv1.StreamEvent]("u1", "t1", "browser", 4),
		realtime.NewConn[*notifyv1.StreamEvent]("u1", "t1", "android", 4),
		realtime.NewConn[*notifyv1.StreamEvent]("u1", "t1", "ios", 4),
	}
	for _, c := range conns {
		reg.Register(c)
		t.Cleanup(func() { reg.Unregister(c.ID) })
	}

	p := newInAppProvider(reg)
	r, err := p.Send(context.Background(), notify.Message{
		Notification: &notify.Notification{ID: "row-3", Title: "fan-out"},
		To:           "u1",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if r.Status != notify.StatusDelivered {
		t.Fatalf("status = %q, want Delivered (3 live connections)", r.Status)
	}
	for i, c := range conns {
		select {
		case <-c.EventCh:
		default:
			t.Errorf("conn[%d] (%s) did not receive the event", i, c.DeviceType)
		}
	}
}
