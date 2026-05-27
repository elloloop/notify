package server

import (
	"context"
	"errors"

	"github.com/elloloop/notify"
	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
	"github.com/elloloop/notify/realtime"
)

// inAppProvider is the notify.Provider implementation that bridges the
// orchestrator (notify.Notifier) and the realtime engine. The Notifier hands
// us a notify.Message; we translate it into a *notifyv1.StreamEvent and push
// it through the per-user Registry. Disconnected users (no live connections)
// simply receive no live push — the row remains stored for later catch-up via
// GetNotifications. This is the documented "store-only when offline" path.
//
// The provider is registered under ChannelInApp only when the
// LiveConnections subsystem is enabled (see Server.New). When the subsystem
// is OFF the provider is absent and the orchestrator's existing "no provider
// configured" branch flags the row as Pending.
type inAppProvider struct {
	registry *realtime.Registry[*notifyv1.StreamEvent]
}

func newInAppProvider(reg *realtime.Registry[*notifyv1.StreamEvent]) *inAppProvider {
	return &inAppProvider{registry: reg}
}

// Kind reports ChannelInApp; the orchestrator looks the provider up by this.
func (p *inAppProvider) Kind() notify.ChannelKind { return notify.ChannelInApp }

// Name is "in_app_realtime" so observability surfaces distinguish it from the
// hypothetical future "in_app_durable_only" provider.
func (p *inAppProvider) Name() string { return "in_app_realtime" }

// Send delivers the rendered notify.Message to every live connection of the
// target user via Registry.Push. The send is non-blocking: a backed-up client
// has its event dropped (Registry logs and counts it) rather than blocking
// the orchestrator.
//
// Return semantics. The row status reflects whether ANY live connection
// received the event, not just that the platform "tried":
//
//   - 1+ live connections accepted the event → StatusDelivered.
//   - 0 connections (user is offline) → StatusPending: the row stays available
//     for later catch-up via GetNotifications. A status of "Delivered" on a
//     row no live device received would lie to downstream consumers reading
//     the row's lifecycle later (the unread filter, the read-receipt analytics
//     in the api gateway, etc.).
//
// If the provider has been constructed with a nil registry the call returns
// an error so the orchestrator records StatusFailed — this should never happen
// in practice (Server.New rejects it) but is defensive against future
// refactors.
func (p *inAppProvider) Send(_ context.Context, msg notify.Message) (notify.Receipt, error) {
	if p.registry == nil {
		return notify.Receipt{Status: notify.StatusFailed}, errors.New("inapp: no registry configured")
	}
	if msg.Notification == nil {
		return notify.Receipt{Status: notify.StatusFailed}, errors.New("inapp: message has no notification row")
	}
	ev := &notifyv1.StreamEvent{
		Event: &notifyv1.StreamEvent_Notification{
			Notification: &notifyv1.NotificationEvent{
				Notification: notificationToProto(msg.Notification),
			},
		},
	}
	delivered := p.registry.Push(msg.To, ev)
	if delivered == 0 {
		return notify.Receipt{Status: notify.StatusPending}, nil
	}
	return notify.Receipt{Status: notify.StatusDelivered}, nil
}
