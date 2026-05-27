package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/elloloop/notify"
	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
	"github.com/elloloop/notify/store/memory"
)

// fakeStore is a tiny notify.Store double that records every call. The
// production drivers are conformance-tested elsewhere; here we only need to
// observe what the handlers ask the store to do.
type fakeStore struct {
	mu sync.Mutex

	// Programmable behaviour.
	createErr        error
	updateErr        error
	queryErr         error
	upsertErr        error
	queryItems       []*notify.Notification
	queryNextCursor  string
	queryUnreadCount int

	// getResult is returned by GetNotification when the (tenant, user, id)
	// arguments match. A nil getResult or a mismatching tuple → ErrNotFound.
	// getErr (when non-nil) overrides everything else and is returned as the
	// only result of GetNotification — useful for "internal error" paths.
	getResult *notify.Notification
	getErr    error

	// Observed calls.
	created     []*notify.Notification
	updates     []updateCall
	queries     []notify.Query
	upserts     []*notify.Device
	getCalls    []getCall
	listDevices int
}

type updateCall struct {
	tenant string
	id     string
	status notify.DeliveryStatus
	atMS   int64
}

type getCall struct {
	tenant string
	user   string
	id     string
}

func (s *fakeStore) CreateNotification(_ context.Context, n *notify.Notification) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return false, s.createErr
	}
	n.ID = "row-" + n.NotificationID + "-" + n.UserID + "-" + string(n.Channel)
	s.created = append(s.created, n)
	return true, nil
}

func (s *fakeStore) GetNotification(_ context.Context, tenantID, userID, id string) (*notify.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls = append(s.getCalls, getCall{tenant: tenantID, user: userID, id: id})
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.getResult == nil {
		return nil, notify.ErrNotFound
	}
	if s.getResult.TenantID != tenantID || s.getResult.UserID != userID || s.getResult.ID != id {
		return nil, notify.ErrNotFound
	}
	return s.getResult, nil
}

func (s *fakeStore) UpdateStatus(_ context.Context, tenantID, id string, st notify.DeliveryStatus, atMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, updateCall{tenant: tenantID, id: id, status: st, atMS: atMS})
	return s.updateErr
}

func (s *fakeStore) QueryUserNotifications(_ context.Context, q notify.Query) ([]*notify.Notification, string, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries = append(s.queries, q)
	if s.queryErr != nil {
		return nil, "", 0, s.queryErr
	}
	return s.queryItems, s.queryNextCursor, s.queryUnreadCount, nil
}

func (s *fakeStore) UpsertDevice(_ context.Context, d *notify.Device) (*notify.Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.upsertErr != nil {
		return nil, s.upsertErr
	}
	d.ID = "device-" + d.UserID + "-" + d.DeviceType
	s.upserts = append(s.upserts, d)
	return d, nil
}

func (s *fakeStore) ListDevices(_ context.Context, _, _ string) ([]*notify.Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listDevices++
	return nil, nil
}

// fakeProvider records every Send for assertions.
type fakeProvider struct {
	kind   notify.ChannelKind
	sendErr error
	sent    []notify.Message
	mu      sync.Mutex
}

func (f *fakeProvider) Kind() notify.ChannelKind { return f.kind }
func (f *fakeProvider) Name() string             { return "fake" }
func (f *fakeProvider) Send(_ context.Context, m notify.Message) (notify.Receipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, m)
	if f.sendErr != nil {
		return notify.Receipt{Status: notify.StatusFailed}, f.sendErr
	}
	return notify.Receipt{Status: notify.StatusDelivered}, nil
}

func fixedClock(ms int64) func() int64 {
	return func() int64 { return ms }
}

func ctxWithClaims(uid, tid string) context.Context {
	return withClaims(context.Background(), Claims{UserID: uid, TenantID: tid})
}

// ─── NotificationInternalService.Notify ──────────────────────────────────

func TestInternalHandler_Notify_HappyPath(t *testing.T) {
	store := memory.New()
	reg := notify.NewProviderRegistry()
	fp := &fakeProvider{kind: notify.ChannelInApp}
	reg.Register(fp)
	notifier := notify.NewNotifier(store, reg, notify.WithClock(func() time.Time {
		return time.UnixMilli(1700000000000)
	}))

	h := newInternalHandler(notifier)
	req := connect.NewRequest(&notifyv1.NotifyRequest{
		TenantId:       "acme",
		NotificationId: "evt-1",
		UserIds:        []string{"u1", "u2"},
		Channels:       []notifyv1.DeliveryChannel{notifyv1.DeliveryChannel_DELIVERY_CHANNEL_IN_APP},
		SubjectRef:     "task-42",
		SubjectType:    "todo",
		Title:          "hello",
		Body:           "world",
	})
	resp, err := h.Notify(context.Background(), req)
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if resp.Msg.GetDelivered() != 2 {
		t.Fatalf("Delivered = %d, want 2", resp.Msg.GetDelivered())
	}
	if fp.sent[0].Title != "hello" || fp.sent[0].Body != "world" {
		t.Fatalf("first send = %+v", fp.sent[0])
	}
}

func TestInternalHandler_Notify_AddressMap(t *testing.T) {
	store := memory.New()
	reg := notify.NewProviderRegistry()
	fp := &fakeProvider{kind: notify.ChannelEmail}
	reg.Register(fp)
	notifier := notify.NewNotifier(store, reg)
	h := newInternalHandler(notifier)

	req := connect.NewRequest(&notifyv1.NotifyRequest{
		TenantId:       "acme",
		NotificationId: "evt-2",
		UserIds:        []string{"u1"},
		Channels:       []notifyv1.DeliveryChannel{notifyv1.DeliveryChannel_DELIVERY_CHANNEL_EMAIL},
		Title:          "hi",
		Addresses: map[string]*notifyv1.ChannelAddresses{
			"u1": {ByChannel: map[string]string{"email": "u1@example.com"}},
		},
	})
	resp, err := h.Notify(context.Background(), req)
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if resp.Msg.GetDelivered() != 1 {
		t.Fatalf("Delivered = %d, want 1", resp.Msg.GetDelivered())
	}
	if fp.sent[0].To != "u1@example.com" {
		t.Fatalf("To = %q, want u1@example.com", fp.sent[0].To)
	}
}

func TestInternalHandler_Notify_Validation(t *testing.T) {
	store := memory.New()
	notifier := notify.NewNotifier(store, nil)
	h := newInternalHandler(notifier)

	cases := []struct {
		name string
		req  *notifyv1.NotifyRequest
	}{
		{"missing tenant", &notifyv1.NotifyRequest{NotificationId: "n", UserIds: []string{"u"}}},
		{"missing notification id", &notifyv1.NotifyRequest{TenantId: "t", UserIds: []string{"u"}}},
		{"empty users", &notifyv1.NotifyRequest{TenantId: "t", NotificationId: "n"}},
		{"whitespace tenant", &notifyv1.NotifyRequest{TenantId: "   ", NotificationId: "n", UserIds: []string{"u"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.Notify(context.Background(), connect.NewRequest(tc.req))
			if err == nil {
				t.Fatal("expected error")
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("code = %v", connect.CodeOf(err))
			}
		})
	}
}

func TestInternalHandler_Notify_StoreError(t *testing.T) {
	fs := &fakeStore{createErr: errors.New("disk full")}
	notifier := notify.NewNotifier(fs, nil)
	h := newInternalHandler(notifier)

	req := connect.NewRequest(&notifyv1.NotifyRequest{
		TenantId:       "t",
		NotificationId: "n",
		UserIds:        []string{"u1"},
		Channels:       []notifyv1.DeliveryChannel{notifyv1.DeliveryChannel_DELIVERY_CHANNEL_IN_APP},
	})
	_, err := h.Notify(context.Background(), req)
	if err == nil {
		t.Fatal("expected error")
	}
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("code = %v", connect.CodeOf(err))
	}
}

// ─── Client service handlers ────────────────────────────────────────────

func TestClientHandler_GetNotifications_HappyPath(t *testing.T) {
	fs := &fakeStore{
		queryItems: []*notify.Notification{
			{
				ID: "n1", NotificationID: "k1", UserID: "u1", TenantID: "t1",
				Title: "T1", Body: "B1", Channel: notify.ChannelInApp,
				Status: notify.StatusDelivered, CreatedAtMS: 100,
			},
		},
		queryNextCursor:  "cursor-next",
		queryUnreadCount: 5,
	}
	h := newClientHandler(fs, nil, fixedClock(0))
	ctx := ctxWithClaims("u1", "t1")
	resp, err := h.GetNotifications(ctx, connect.NewRequest(&notifyv1.GetNotificationsRequest{Limit: 10}))
	if err != nil {
		t.Fatalf("GetNotifications: %v", err)
	}
	if len(resp.Msg.GetNotifications()) != 1 {
		t.Fatalf("notifications = %d", len(resp.Msg.GetNotifications()))
	}
	got := resp.Msg.GetNotifications()[0]
	if got.GetId() != "n1" || got.GetTitle() != "T1" {
		t.Fatalf("unexpected mapping: %+v", got)
	}
	if got.GetChannel() != notifyv1.DeliveryChannel_DELIVERY_CHANNEL_IN_APP {
		t.Fatalf("channel = %v", got.GetChannel())
	}
	if got.GetStatus() != notifyv1.DeliveryStatus_DELIVERY_STATUS_DELIVERED {
		t.Fatalf("status = %v", got.GetStatus())
	}
	if resp.Msg.GetNextCursor() != "cursor-next" || resp.Msg.GetUnreadCount() != 5 {
		t.Fatalf("paging = %+v", resp.Msg)
	}
	if len(fs.queries) != 1 || fs.queries[0].UserID != "u1" || fs.queries[0].TenantID != "t1" {
		t.Fatalf("query = %+v", fs.queries)
	}
}

func TestClientHandler_GetNotifications_NoClaims(t *testing.T) {
	h := newClientHandler(&fakeStore{}, nil, fixedClock(0))
	_, err := h.GetNotifications(context.Background(), connect.NewRequest(&notifyv1.GetNotificationsRequest{}))
	if err == nil {
		t.Fatal("expected error")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("code = %v", connect.CodeOf(err))
	}
}

func TestClientHandler_GetNotifications_CursorDecode(t *testing.T) {
	fs := &fakeStore{}
	h := newClientHandler(fs, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")

	// The wire cursor is the decimal-encoded created-at-ms string the store
	// itself emits; the handler must pass that through to Query.CursorMS.
	_, err := h.GetNotifications(ctx, connect.NewRequest(&notifyv1.GetNotificationsRequest{Cursor: "12345"}))
	if err != nil {
		t.Fatalf("GetNotifications: %v", err)
	}
	if fs.queries[0].CursorMS == nil || *fs.queries[0].CursorMS != 12345 {
		t.Fatalf("cursor not decoded: %+v", fs.queries[0])
	}
}

// TestClientHandler_GetNotifications_RealRoundTrip closes the loop the earlier
// version of this PR silently broke: feed the server's NextCursor back in as
// the next request's Cursor and assert the handler decodes it. A response that
// is not consumable by a subsequent request fails this test loudly.
func TestClientHandler_GetNotifications_RealRoundTrip(t *testing.T) {
	fs := &fakeStore{queryNextCursor: "1700000000000"}
	h := newClientHandler(fs, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")

	first, err := h.GetNotifications(ctx, connect.NewRequest(&notifyv1.GetNotificationsRequest{}))
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	next := first.Msg.GetNextCursor()
	if next == "" {
		t.Fatal("first page emitted empty NextCursor")
	}

	// Feed it back as the cursor; the handler must accept it without error.
	_, err = h.GetNotifications(ctx, connect.NewRequest(&notifyv1.GetNotificationsRequest{Cursor: next}))
	if err != nil {
		t.Fatalf("second page with %q: %v", next, err)
	}
	if got := fs.queries[1].CursorMS; got == nil || *got != 1700000000000 {
		t.Fatalf("second page CursorMS = %v, want 1700000000000", got)
	}
}

func TestClientHandler_GetNotifications_BadCursor(t *testing.T) {
	h := newClientHandler(&fakeStore{}, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")
	_, err := h.GetNotifications(ctx, connect.NewRequest(&notifyv1.GetNotificationsRequest{Cursor: "not-an-int"}))
	if err == nil {
		t.Fatal("expected error")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("code = %v", connect.CodeOf(err))
	}
}

func TestClientHandler_GetNotifications_StoreError(t *testing.T) {
	fs := &fakeStore{queryErr: errors.New("boom")}
	h := newClientHandler(fs, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")
	_, err := h.GetNotifications(ctx, connect.NewRequest(&notifyv1.GetNotificationsRequest{}))
	if err == nil || connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("err=%v code=%v", err, connect.CodeOf(err))
	}
}

// ownedNotif is a helper for the AckNotification ownership-check tests — it
// builds a Notification whose tenant + user line up with the calling claims so
// the handler's Get-first check passes.
func ownedNotif(tenant, user, id string) *notify.Notification {
	return &notify.Notification{ID: id, TenantID: tenant, UserID: user}
}

func TestClientHandler_AckNotification_HappyPath(t *testing.T) {
	fs := &fakeStore{getResult: ownedNotif("tenant-a", "u", "row-1")}
	h := newClientHandler(fs, nil, fixedClock(999))
	ctx := ctxWithClaims("u", "tenant-a")
	_, err := h.AckNotification(ctx, connect.NewRequest(&notifyv1.AckNotificationRequest{Id: "row-1"}))
	if err != nil {
		t.Fatalf("AckNotification: %v", err)
	}
	if len(fs.getCalls) != 1 {
		t.Fatalf("expected one Get for ownership check, got %d", len(fs.getCalls))
	}
	if got := fs.getCalls[0]; got.tenant != "tenant-a" || got.user != "u" || got.id != "row-1" {
		t.Fatalf("ownership get = %+v", got)
	}
	if len(fs.updates) != 1 {
		t.Fatalf("updates = %v", fs.updates)
	}
	u := fs.updates[0]
	if u.id != "row-1" || u.tenant != "tenant-a" || u.status != notify.StatusRead || u.atMS != 999 {
		t.Fatalf("update = %+v", u)
	}
}

func TestClientHandler_AckNotification_Validation(t *testing.T) {
	h := newClientHandler(&fakeStore{}, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")
	_, err := h.AckNotification(ctx, connect.NewRequest(&notifyv1.AckNotificationRequest{Id: "   "}))
	if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("err=%v code=%v", err, connect.CodeOf(err))
	}
}

// TestClientHandler_AckNotification_NotFound — the row simply doesn't exist.
// The ownership Get fails first; UpdateStatus is never reached.
func TestClientHandler_AckNotification_NotFound(t *testing.T) {
	fs := &fakeStore{} // getResult unset → GetNotification always returns ErrNotFound
	h := newClientHandler(fs, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")
	_, err := h.AckNotification(ctx, connect.NewRequest(&notifyv1.AckNotificationRequest{Id: "x"}))
	if err == nil || connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("err=%v code=%v", err, connect.CodeOf(err))
	}
	if len(fs.updates) != 0 {
		t.Fatalf("UpdateStatus must NOT be called when ownership check fails: %+v", fs.updates)
	}
}

// TestClientHandler_AckNotification_CrossUser — the canonical privacy bug
// regression test. user "alice" must NOT be able to ack a row owned by "bob"
// in the same tenant by guessing the id.
func TestClientHandler_AckNotification_CrossUser(t *testing.T) {
	fs := &fakeStore{getResult: ownedNotif("tenant-a", "bob", "row-1")}
	h := newClientHandler(fs, nil, fixedClock(0))
	ctx := ctxWithClaims("alice", "tenant-a")
	_, err := h.AckNotification(ctx, connect.NewRequest(&notifyv1.AckNotificationRequest{Id: "row-1"}))
	if err == nil || connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("cross-user ack must surface as NotFound, got code=%v err=%v", connect.CodeOf(err), err)
	}
	if len(fs.updates) != 0 {
		t.Fatalf("UpdateStatus must NOT be called for a cross-user ack: %+v", fs.updates)
	}
}

// TestClientHandler_AckNotification_CrossTenant — the same row exists in
// tenant T1 but the caller is in T2; the lookup must miss, never silently
// transition T1's row.
func TestClientHandler_AckNotification_CrossTenant(t *testing.T) {
	fs := &fakeStore{getResult: ownedNotif("tenant-1", "u", "row-1")}
	h := newClientHandler(fs, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "tenant-2")
	_, err := h.AckNotification(ctx, connect.NewRequest(&notifyv1.AckNotificationRequest{Id: "row-1"}))
	if err == nil || connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("cross-tenant ack must surface as NotFound, got code=%v err=%v", connect.CodeOf(err), err)
	}
	if len(fs.updates) != 0 {
		t.Fatalf("UpdateStatus must NOT be called for a cross-tenant ack: %+v", fs.updates)
	}
}

// TestClientHandler_AckNotification_LookupInternalError — the ownership Get
// itself fails (disk, network, …); the handler must return Internal, not
// NotFound (which would leak "row absent" semantics).
func TestClientHandler_AckNotification_LookupInternalError(t *testing.T) {
	fs := &fakeStore{getErr: errors.New("disk")}
	h := newClientHandler(fs, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")
	_, err := h.AckNotification(ctx, connect.NewRequest(&notifyv1.AckNotificationRequest{Id: "x"}))
	if err == nil || connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("err=%v code=%v", err, connect.CodeOf(err))
	}
}

// TestClientHandler_AckNotification_InternalError — the ownership Get
// succeeds but UpdateStatus fails with a generic error.
func TestClientHandler_AckNotification_InternalError(t *testing.T) {
	fs := &fakeStore{
		getResult: ownedNotif("t", "u", "x"),
		updateErr: errors.New("disk"),
	}
	h := newClientHandler(fs, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")
	_, err := h.AckNotification(ctx, connect.NewRequest(&notifyv1.AckNotificationRequest{Id: "x"}))
	if err == nil || connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("err=%v code=%v", err, connect.CodeOf(err))
	}
}

func TestClientHandler_AckNotification_NoClaims(t *testing.T) {
	h := newClientHandler(&fakeStore{}, nil, fixedClock(0))
	_, err := h.AckNotification(context.Background(), connect.NewRequest(&notifyv1.AckNotificationRequest{Id: "x"}))
	if err == nil || connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("err=%v code=%v", err, connect.CodeOf(err))
	}
}

func TestClientHandler_AckDataChange_NoStreamSubsystem(t *testing.T) {
	h := newClientHandler(&fakeStore{}, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")
	_, err := h.AckDataChange(ctx, connect.NewRequest(&notifyv1.AckDataChangeRequest{IdempotencyKey: "k", SessionId: "s"}))
	if err != nil {
		t.Fatalf("AckDataChange without stream subsystem should be no-op, got %v", err)
	}
}

func TestClientHandler_AckDataChange_Validation(t *testing.T) {
	h := newClientHandler(&fakeStore{}, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")
	cases := []*notifyv1.AckDataChangeRequest{
		{IdempotencyKey: "", SessionId: "s"},
		{IdempotencyKey: "k", SessionId: ""},
	}
	for i, req := range cases {
		_, err := h.AckDataChange(ctx, connect.NewRequest(req))
		if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("case %d: err=%v code=%v", i, err, connect.CodeOf(err))
		}
	}
}

func TestClientHandler_RegisterPushToken_HappyPath(t *testing.T) {
	fs := &fakeStore{}
	h := newClientHandler(fs, nil, fixedClock(42))
	ctx := ctxWithClaims("u1", "t1")
	_, err := h.RegisterPushToken(ctx, connect.NewRequest(&notifyv1.RegisterPushTokenRequest{
		DeviceType: notifyv1.DeviceType_DEVICE_TYPE_ANDROID,
		Token:      "tok-abc",
	}))
	if err != nil {
		t.Fatalf("RegisterPushToken: %v", err)
	}
	if len(fs.upserts) != 1 {
		t.Fatalf("upserts = %v", fs.upserts)
	}
	d := fs.upserts[0]
	if d.UserID != "u1" || d.TenantID != "t1" || d.DeviceType != "android" || d.Token != "tok-abc" || d.LastActiveMS != 42 {
		t.Fatalf("upsert = %+v", d)
	}
}

func TestClientHandler_RegisterPushToken_Validation(t *testing.T) {
	fs := &fakeStore{}
	h := newClientHandler(fs, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")

	cases := []struct {
		name string
		req  *notifyv1.RegisterPushTokenRequest
	}{
		{"unspecified device", &notifyv1.RegisterPushTokenRequest{Token: "t"}},
		{"empty token", &notifyv1.RegisterPushTokenRequest{DeviceType: notifyv1.DeviceType_DEVICE_TYPE_IOS}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.RegisterPushToken(ctx, connect.NewRequest(tc.req))
			if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("err=%v code=%v", err, connect.CodeOf(err))
			}
		})
	}
}

func TestClientHandler_RegisterPushToken_StoreError(t *testing.T) {
	fs := &fakeStore{upsertErr: errors.New("boom")}
	h := newClientHandler(fs, nil, fixedClock(0))
	ctx := ctxWithClaims("u", "t")
	_, err := h.RegisterPushToken(ctx, connect.NewRequest(&notifyv1.RegisterPushTokenRequest{
		DeviceType: notifyv1.DeviceType_DEVICE_TYPE_BROWSER,
		Token:      "t",
	}))
	if err == nil || connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("err=%v code=%v", err, connect.CodeOf(err))
	}
}

func TestClientHandler_StreamEvents_DisabledReturnsUnimplemented(t *testing.T) {
	h := newClientHandler(&fakeStore{}, nil, fixedClock(0))
	err := h.StreamEvents(context.Background(),
		connect.NewRequest(&notifyv1.StreamEventsRequest{}),
		nil)
	if err == nil || connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("err=%v code=%v", err, connect.CodeOf(err))
	}
}

// ─── Cursor helpers ─────────────────────────────────────────────────────

func TestDecodeCursor(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int64
		err  bool
	}{
		{"happy", "1700000000000", 1700000000000, false},
		{"one", "1", 1, false},
		{"not_int", "abc", 0, true},
		{"empty", "", 0, true},
		{"negative", "-5", 0, true},
		{"zero", "0", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeCursor(tc.in)
			if tc.err {
				if err == nil {
					t.Fatalf("want error, got nil (out=%d)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// ─── Proto ↔ domain mapping coverage ────────────────────────────────────

func TestChannelKindMapping_RoundTrip(t *testing.T) {
	kinds := []notify.ChannelKind{
		notify.ChannelInApp,
		notify.ChannelEmail,
		notify.ChannelWebPush,
		notify.ChannelMobilePush,
		notify.ChannelSMS,
		notify.ChannelWhatsApp,
	}
	for _, k := range kinds {
		out := channelKindFromProto(channelKindToProto(k))
		if out != k {
			t.Errorf("%q round-trip = %q", k, out)
		}
	}
}

func TestStatusMapping_RoundTrip(t *testing.T) {
	cases := []struct {
		domain notify.DeliveryStatus
		proto  notifyv1.DeliveryStatus
	}{
		{notify.StatusPending, notifyv1.DeliveryStatus_DELIVERY_STATUS_PENDING},
		{notify.StatusDelivered, notifyv1.DeliveryStatus_DELIVERY_STATUS_DELIVERED},
		{notify.StatusAcked, notifyv1.DeliveryStatus_DELIVERY_STATUS_ACKED},
		{notify.StatusRead, notifyv1.DeliveryStatus_DELIVERY_STATUS_READ},
		{notify.StatusFailed, notifyv1.DeliveryStatus_DELIVERY_STATUS_FAILED},
	}
	for _, tc := range cases {
		if got := statusToProto(tc.domain); got != tc.proto {
			t.Errorf("statusToProto(%q) = %v, want %v", tc.domain, got, tc.proto)
		}
	}
}

func TestDeviceTypeMapping(t *testing.T) {
	cases := []struct {
		in   notifyv1.DeviceType
		want string
	}{
		{notifyv1.DeviceType_DEVICE_TYPE_BROWSER, "browser"},
		{notifyv1.DeviceType_DEVICE_TYPE_ANDROID, "android"},
		{notifyv1.DeviceType_DEVICE_TYPE_IOS, "ios"},
		{notifyv1.DeviceType_DEVICE_TYPE_UNSPECIFIED, ""},
	}
	for _, tc := range cases {
		if got := deviceTypeFromProto(tc.in); got != tc.want {
			t.Errorf("deviceTypeFromProto(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNotifyRequestFromProto_FullMapping(t *testing.T) {
	in := &notifyv1.NotifyRequest{
		TenantId:       "t",
		NotificationId: "n",
		UserIds:        []string{"u1", "u2"},
		Channels: []notifyv1.DeliveryChannel{
			notifyv1.DeliveryChannel_DELIVERY_CHANNEL_EMAIL,
			notifyv1.DeliveryChannel_DELIVERY_CHANNEL_SMS,
		},
		SubjectRef:  "ref",
		SubjectType: "todo",
		Title:       "T",
		Body:        "B",
		Addresses: map[string]*notifyv1.ChannelAddresses{
			"u1": {ByChannel: map[string]string{"email": "a@example.com", "sms": "+15555550000"}},
			"u2": {ByChannel: map[string]string{"unknown": "x"}}, // filtered out
		},
	}
	out := notifyRequestFromProto(in)
	if out.TenantID != "t" || out.NotificationID != "n" || out.SubjectRef != "ref" {
		t.Fatalf("scalars: %+v", out)
	}
	if !sameStrings(out.UserIDs, []string{"u1", "u2"}) {
		t.Fatalf("user ids: %+v", out.UserIDs)
	}
	if len(out.Channels) != 2 {
		t.Fatalf("channels: %+v", out.Channels)
	}
	if out.Addresses["u1"][notify.ChannelEmail] != "a@example.com" {
		t.Fatalf("u1 email = %q", out.Addresses["u1"][notify.ChannelEmail])
	}
	if _, ok := out.Addresses["u2"]; ok {
		t.Fatalf("u2 unknown channel should be filtered, got %+v", out.Addresses["u2"])
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestValidateNotifyRequest_ErrorMessages(t *testing.T) {
	// Spot-check that error messages stay greppable for ops.
	err := validateNotifyRequest(&notifyv1.NotifyRequest{NotificationId: "n", UserIds: []string{"u"}})
	if err == nil || !strings.Contains(err.Error(), "tenant_id") {
		t.Fatalf("err = %v, want tenant_id mention", err)
	}
}
