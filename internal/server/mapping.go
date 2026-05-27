package server

import (
	"github.com/elloloop/notify"
	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
)

// channelKindToProto converts a notify.ChannelKind into its wire-stable
// DeliveryChannel enum. Unknown values map to UNSPECIFIED so a bad value
// never silently becomes a different channel.
func channelKindToProto(k notify.ChannelKind) notifyv1.DeliveryChannel {
	switch k {
	case notify.ChannelInApp:
		return notifyv1.DeliveryChannel_DELIVERY_CHANNEL_IN_APP
	case notify.ChannelEmail:
		return notifyv1.DeliveryChannel_DELIVERY_CHANNEL_EMAIL
	case notify.ChannelWebPush:
		return notifyv1.DeliveryChannel_DELIVERY_CHANNEL_WEB_PUSH
	case notify.ChannelMobilePush:
		return notifyv1.DeliveryChannel_DELIVERY_CHANNEL_MOBILE_PUSH
	case notify.ChannelSMS:
		return notifyv1.DeliveryChannel_DELIVERY_CHANNEL_SMS
	case notify.ChannelWhatsApp:
		return notifyv1.DeliveryChannel_DELIVERY_CHANNEL_WHATSAPP
	default:
		return notifyv1.DeliveryChannel_DELIVERY_CHANNEL_UNSPECIFIED
	}
}

// channelKindFromProto is the inverse direction; UNSPECIFIED maps to the empty
// kind so the caller can decide whether to reject or treat as "all channels".
func channelKindFromProto(c notifyv1.DeliveryChannel) notify.ChannelKind {
	switch c {
	case notifyv1.DeliveryChannel_DELIVERY_CHANNEL_IN_APP:
		return notify.ChannelInApp
	case notifyv1.DeliveryChannel_DELIVERY_CHANNEL_EMAIL:
		return notify.ChannelEmail
	case notifyv1.DeliveryChannel_DELIVERY_CHANNEL_WEB_PUSH:
		return notify.ChannelWebPush
	case notifyv1.DeliveryChannel_DELIVERY_CHANNEL_MOBILE_PUSH:
		return notify.ChannelMobilePush
	case notifyv1.DeliveryChannel_DELIVERY_CHANNEL_SMS:
		return notify.ChannelSMS
	case notifyv1.DeliveryChannel_DELIVERY_CHANNEL_WHATSAPP:
		return notify.ChannelWhatsApp
	default:
		return ""
	}
}

// statusToProto converts a domain status into the wire enum.
func statusToProto(s notify.DeliveryStatus) notifyv1.DeliveryStatus {
	switch s {
	case notify.StatusPending:
		return notifyv1.DeliveryStatus_DELIVERY_STATUS_PENDING
	case notify.StatusDelivered:
		return notifyv1.DeliveryStatus_DELIVERY_STATUS_DELIVERED
	case notify.StatusAcked:
		return notifyv1.DeliveryStatus_DELIVERY_STATUS_ACKED
	case notify.StatusRead:
		return notifyv1.DeliveryStatus_DELIVERY_STATUS_READ
	case notify.StatusFailed:
		return notifyv1.DeliveryStatus_DELIVERY_STATUS_FAILED
	default:
		return notifyv1.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED
	}
}

// deviceTypeFromProto translates the wire enum into the string form the
// notify.Device row expects. UNSPECIFIED maps to "" so handlers can reject it.
func deviceTypeFromProto(d notifyv1.DeviceType) string {
	switch d {
	case notifyv1.DeviceType_DEVICE_TYPE_BROWSER:
		return "browser"
	case notifyv1.DeviceType_DEVICE_TYPE_ANDROID:
		return "android"
	case notifyv1.DeviceType_DEVICE_TYPE_IOS:
		return "ios"
	default:
		return ""
	}
}

// notificationToProto converts a stored *notify.Notification into the client
// view used in NotificationEvent / GetNotificationsResponse. A nil input
// returns nil so callers can pass query results through unconditionally.
func notificationToProto(n *notify.Notification) *notifyv1.Notification {
	if n == nil {
		return nil
	}
	return &notifyv1.Notification{
		Id:             n.ID,
		NotificationId: n.NotificationID,
		SubjectRef:     n.SubjectRef,
		SubjectType:    n.SubjectType,
		Title:          n.Title,
		Body:           n.Body,
		Channel:        channelKindToProto(n.Channel),
		Status:         statusToProto(n.Status),
		CreatedAtMs:    n.CreatedAtMS,
		DeliveredAtMs:  n.DeliveredAtMS,
		AckAtMs:        n.AckAtMS,
		ReadAtMs:       n.ReadAtMS,
	}
}

// channelNameForAddress is the string key used in NotifyRequest.addresses for
// each delivery channel — the lowercase suffix documented in the proto.
func channelNameForAddress(k notify.ChannelKind) string {
	switch k {
	case notify.ChannelEmail:
		return "email"
	case notify.ChannelSMS:
		return "sms"
	case notify.ChannelWhatsApp:
		return "whatsapp"
	case notify.ChannelMobilePush:
		return "mobile_push"
	case notify.ChannelWebPush:
		return "web_push"
	case notify.ChannelInApp:
		return "in_app"
	default:
		return ""
	}
}

// addressKeyToKind is the inverse mapping for the inner address map keys.
func addressKeyToKind(name string) notify.ChannelKind {
	switch name {
	case "email":
		return notify.ChannelEmail
	case "sms":
		return notify.ChannelSMS
	case "whatsapp":
		return notify.ChannelWhatsApp
	case "mobile_push":
		return notify.ChannelMobilePush
	case "web_push":
		return notify.ChannelWebPush
	case "in_app":
		return notify.ChannelInApp
	default:
		return ""
	}
}
