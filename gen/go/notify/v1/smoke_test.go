// Smoke test for the generated notify v1 stubs.
//
// This test does not exercise any RPC; it constructs every request and
// response message type and reads every public field/enum it should expose.
// The intent is to catch regressions where a future buf regeneration drops
// a field, renames a generated type, or changes an enum constant — any of
// those would either fail to compile or fail one of the assertions below.

package notifyv1_test

import (
	"testing"

	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
)

func TestEnumsAreStable(t *testing.T) {
	// Field numbers and enum values in the proto are stable forever;
	// these assertions are the tripwire that catches any reallocation.
	cases := []struct {
		name string
		got  int32
		want int32
	}{
		{"DELIVERY_CHANNEL_UNSPECIFIED", int32(notifyv1.DeliveryChannel_DELIVERY_CHANNEL_UNSPECIFIED), 0},
		{"DELIVERY_CHANNEL_IN_APP", int32(notifyv1.DeliveryChannel_DELIVERY_CHANNEL_IN_APP), 1},
		{"DELIVERY_CHANNEL_EMAIL", int32(notifyv1.DeliveryChannel_DELIVERY_CHANNEL_EMAIL), 2},
		{"DELIVERY_CHANNEL_WEB_PUSH", int32(notifyv1.DeliveryChannel_DELIVERY_CHANNEL_WEB_PUSH), 3},
		{"DELIVERY_CHANNEL_MOBILE_PUSH", int32(notifyv1.DeliveryChannel_DELIVERY_CHANNEL_MOBILE_PUSH), 4},
		{"DELIVERY_CHANNEL_SMS", int32(notifyv1.DeliveryChannel_DELIVERY_CHANNEL_SMS), 5},
		{"DELIVERY_CHANNEL_WHATSAPP", int32(notifyv1.DeliveryChannel_DELIVERY_CHANNEL_WHATSAPP), 6},
		{"DELIVERY_STATUS_UNSPECIFIED", int32(notifyv1.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED), 0},
		{"DELIVERY_STATUS_PENDING", int32(notifyv1.DeliveryStatus_DELIVERY_STATUS_PENDING), 1},
		{"DELIVERY_STATUS_DELIVERED", int32(notifyv1.DeliveryStatus_DELIVERY_STATUS_DELIVERED), 2},
		{"DELIVERY_STATUS_ACKED", int32(notifyv1.DeliveryStatus_DELIVERY_STATUS_ACKED), 3},
		{"DELIVERY_STATUS_READ", int32(notifyv1.DeliveryStatus_DELIVERY_STATUS_READ), 4},
		{"DELIVERY_STATUS_FAILED", int32(notifyv1.DeliveryStatus_DELIVERY_STATUS_FAILED), 5},
		{"DEVICE_TYPE_UNSPECIFIED", int32(notifyv1.DeviceType_DEVICE_TYPE_UNSPECIFIED), 0},
		{"DEVICE_TYPE_BROWSER", int32(notifyv1.DeviceType_DEVICE_TYPE_BROWSER), 1},
		{"DEVICE_TYPE_ANDROID", int32(notifyv1.DeviceType_DEVICE_TYPE_ANDROID), 2},
		{"DEVICE_TYPE_IOS", int32(notifyv1.DeviceType_DEVICE_TYPE_IOS), 3},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, c.got, c.want)
		}
	}
}

func TestNotifyRequestRoundTrip(t *testing.T) {
	// Build a fully-populated NotifyRequest and read every field back.
	// If `buf generate` ever drops a field, this stops compiling.
	req := &notifyv1.NotifyRequest{
		TenantId:       "tenant-a",
		NotificationId: "notif-1",
		UserIds:        []string{"u1", "u2"},
		Channels: []notifyv1.DeliveryChannel{
			notifyv1.DeliveryChannel_DELIVERY_CHANNEL_EMAIL,
			notifyv1.DeliveryChannel_DELIVERY_CHANNEL_IN_APP,
		},
		SubjectRef:  "subj-1",
		SubjectType: "todo",
		Title:       "hello",
		Body:        "world",
		Addresses: map[string]*notifyv1.ChannelAddresses{
			"u1": {ByChannel: map[string]string{"email": "a@example.com"}},
			"u2": {ByChannel: map[string]string{"sms": "+15555550123"}},
		},
	}
	if req.GetTenantId() != "tenant-a" {
		t.Fatalf("tenant_id round-trip failed")
	}
	if req.GetNotificationId() != "notif-1" {
		t.Fatalf("notification_id round-trip failed")
	}
	if len(req.GetUserIds()) != 2 {
		t.Fatalf("user_ids round-trip failed")
	}
	if len(req.GetChannels()) != 2 {
		t.Fatalf("channels round-trip failed")
	}
	if req.GetSubjectRef() != "subj-1" || req.GetSubjectType() != "todo" {
		t.Fatalf("subject round-trip failed")
	}
	if req.GetTitle() != "hello" || req.GetBody() != "world" {
		t.Fatalf("title/body round-trip failed")
	}
	if got := req.GetAddresses()["u1"].GetByChannel()["email"]; got != "a@example.com" {
		t.Fatalf("addresses[u1][email] round-trip failed: got %q", got)
	}

	resp := &notifyv1.NotifyResponse{Delivered: 3, Pending: 2, Failed: 1}
	if resp.GetDelivered() != 3 || resp.GetPending() != 2 || resp.GetFailed() != 1 {
		t.Fatalf("NotifyResponse round-trip failed")
	}
}

func TestStreamEventOneofVariants(t *testing.T) {
	// Each event variant should populate the oneof correctly. If a future
	// regeneration drops a variant, the type assertion below stops compiling.
	notif := &notifyv1.StreamEvent{
		SessionId: "sess-1",
		Event: &notifyv1.StreamEvent_Notification{
			Notification: &notifyv1.NotificationEvent{
				Notification: &notifyv1.Notification{
					Id:             "store-1",
					NotificationId: "notif-1",
					SubjectRef:     "subj",
					SubjectType:    "todo",
					Title:          "t",
					Body:           "b",
					Channel:        notifyv1.DeliveryChannel_DELIVERY_CHANNEL_IN_APP,
					Status:         notifyv1.DeliveryStatus_DELIVERY_STATUS_DELIVERED,
					CreatedAtMs:    1,
					DeliveredAtMs:  2,
					AckAtMs:        3,
					ReadAtMs:       4,
				},
			},
		},
	}
	if _, ok := notif.GetEvent().(*notifyv1.StreamEvent_Notification); !ok {
		t.Fatalf("notification oneof variant did not stick")
	}
	if notif.GetNotification().GetNotification().GetId() != "store-1" {
		t.Fatalf("notification getters chain broken")
	}

	dc := &notifyv1.StreamEvent{
		SessionId: "sess-1",
		Event: &notifyv1.StreamEvent_DataChange{
			DataChange: &notifyv1.DataChangeEvent{
				IdempotencyKey: "k1",
				SubjectRef:     "subj",
				SubjectType:    "todo",
			},
		},
	}
	if _, ok := dc.GetEvent().(*notifyv1.StreamEvent_DataChange); !ok {
		t.Fatalf("data_change oneof variant did not stick")
	}
	if dc.GetDataChange().GetIdempotencyKey() != "k1" {
		t.Fatalf("data_change getters broken")
	}

	hb := &notifyv1.StreamEvent{
		SessionId: "sess-1",
		Event:     &notifyv1.StreamEvent_Heartbeat{Heartbeat: &notifyv1.HeartbeatEvent{TimestampMs: 42}},
	}
	if _, ok := hb.GetEvent().(*notifyv1.StreamEvent_Heartbeat); !ok {
		t.Fatalf("heartbeat oneof variant did not stick")
	}
	if hb.GetHeartbeat().GetTimestampMs() != 42 {
		t.Fatalf("heartbeat getters broken")
	}
}

func TestClientServiceMessagesRoundTrip(t *testing.T) {
	getReq := &notifyv1.GetNotificationsRequest{Cursor: "c1", Limit: 50, UnreadOnly: true}
	if getReq.GetCursor() != "c1" || getReq.GetLimit() != 50 || !getReq.GetUnreadOnly() {
		t.Fatalf("GetNotificationsRequest round-trip failed")
	}
	getResp := &notifyv1.GetNotificationsResponse{
		Notifications: []*notifyv1.Notification{{Id: "n1"}},
		NextCursor:    "c2",
		UnreadCount:   7,
	}
	if len(getResp.GetNotifications()) != 1 || getResp.GetNextCursor() != "c2" || getResp.GetUnreadCount() != 7 {
		t.Fatalf("GetNotificationsResponse round-trip failed")
	}

	ackN := &notifyv1.AckNotificationRequest{Id: "store-id"}
	if ackN.GetId() != "store-id" {
		t.Fatalf("AckNotificationRequest round-trip failed")
	}
	_ = &notifyv1.AckNotificationResponse{}

	ackDC := &notifyv1.AckDataChangeRequest{IdempotencyKey: "k1", SessionId: "s1"}
	if ackDC.GetIdempotencyKey() != "k1" || ackDC.GetSessionId() != "s1" {
		t.Fatalf("AckDataChangeRequest round-trip failed")
	}
	_ = &notifyv1.AckDataChangeResponse{}

	rpt := &notifyv1.RegisterPushTokenRequest{
		DeviceType: notifyv1.DeviceType_DEVICE_TYPE_IOS,
		Token:      "tok-1",
	}
	if rpt.GetDeviceType() != notifyv1.DeviceType_DEVICE_TYPE_IOS || rpt.GetToken() != "tok-1" {
		t.Fatalf("RegisterPushTokenRequest round-trip failed")
	}
	_ = &notifyv1.RegisterPushTokenResponse{}

	se := &notifyv1.StreamEventsRequest{DeviceType: notifyv1.DeviceType_DEVICE_TYPE_BROWSER}
	if se.GetDeviceType() != notifyv1.DeviceType_DEVICE_TYPE_BROWSER {
		t.Fatalf("StreamEventsRequest round-trip failed")
	}
}
