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
		t.Fatalf("status = %q", r.Status)
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
