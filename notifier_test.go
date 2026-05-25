package notify_test

import (
	"context"
	"testing"
	"time"

	"github.com/elloloop/notify"
	"github.com/elloloop/notify/store/memory"
)

// fakeProvider records every Send so a test can assert the Notifier dispatched.
type fakeProvider struct {
	kind notify.ChannelKind
	sent int
	last notify.Message
}

func (f *fakeProvider) Kind() notify.ChannelKind { return f.kind }
func (f *fakeProvider) Name() string             { return "fake" }
func (f *fakeProvider) Send(_ context.Context, msg notify.Message) (notify.Receipt, error) {
	f.sent++
	f.last = msg
	return notify.Receipt{Status: notify.StatusDelivered}, nil
}

func TestNotify_InApp_PersistsAndDelivers(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	reg := notify.NewProviderRegistry()
	fp := &fakeProvider{kind: notify.ChannelInApp}
	reg.Register(fp)

	clock := func() time.Time { return time.UnixMilli(1_700_000_000_000) }
	n := notify.NewNotifier(store, reg, notify.WithClock(clock))

	res, err := n.Notify(ctx, notify.NotifyRequest{
		TenantID:       "acme",
		NotificationID: "evt-1",
		UserIDs:        []string{"u1", "u2"},
		Channels:       []notify.ChannelKind{notify.ChannelInApp},
		SubjectRef:     "task-42",
		SubjectType:    "todo",
		Title:          "hello",
	})
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if res.Delivered != 2 || res.Pending != 0 || res.Failed != 0 {
		t.Fatalf("result = %+v, want Delivered:2", res)
	}
	if fp.sent != 2 {
		t.Fatalf("provider Send called %d times, want 2", fp.sent)
	}

	// Both rows persisted and marked delivered.
	items, _, unread, err := store.QueryUserNotifications(ctx, notify.Query{TenantID: "acme", UserID: "u1"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(items) != 1 || items[0].Status != notify.StatusDelivered {
		t.Fatalf("u1 rows = %#v", items)
	}
	if items[0].SubjectRef != "task-42" || items[0].SubjectType != "todo" {
		t.Fatalf("subject not persisted: %+v", items[0])
	}
	if unread != 1 {
		t.Fatalf("unread = %d, want 1 (delivered is still unread)", unread)
	}
}

func TestNotify_NoProvider_StaysPending(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	n := notify.NewNotifier(store, notify.NewProviderRegistry())

	res, err := n.Notify(ctx, notify.NotifyRequest{
		TenantID:       "acme",
		NotificationID: "evt-1",
		UserIDs:        []string{"u1"},
		Channels:       []notify.ChannelKind{notify.ChannelEmail},
		Title:          "hi",
	})
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if res.Pending != 1 || res.Delivered != 0 {
		t.Fatalf("result = %+v, want Pending:1", res)
	}
}
