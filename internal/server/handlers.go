package server

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"connectrpc.com/connect"

	"github.com/elloloop/notify"
	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
)

// internalHandler implements NotificationInternalService.Notify.
// It is a thin translation layer over notify.Notifier — the orchestrator does
// the actual work and this handler only converts wire types in and out.
type internalHandler struct {
	notifier *notify.Notifier
}

func newInternalHandler(n *notify.Notifier) *internalHandler {
	return &internalHandler{notifier: n}
}

// Notify maps NotifyRequest → notify.NotifyRequest, calls the orchestrator,
// and maps the result. The full proto-to-domain conversion is in
// notifyRequestFromProto so it can be unit-tested independently.
func (h *internalHandler) Notify(
	ctx context.Context,
	req *connect.Request[notifyv1.NotifyRequest],
) (*connect.Response[notifyv1.NotifyResponse], error) {
	in := req.Msg
	if in == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("notify: nil request"))
	}
	if err := validateNotifyRequest(in); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	domainReq := notifyRequestFromProto(in)
	res, err := h.notifier.Notify(ctx, domainReq)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("notify: %w", err))
	}
	return connect.NewResponse(&notifyv1.NotifyResponse{
		Delivered: int32(res.Delivered),
		Pending:   int32(res.Pending),
		Failed:    int32(res.Failed),
	}), nil
}

func validateNotifyRequest(r *notifyv1.NotifyRequest) error {
	if strings.TrimSpace(r.GetTenantId()) == "" {
		return errors.New("tenant_id is required")
	}
	if strings.TrimSpace(r.GetNotificationId()) == "" {
		return errors.New("notification_id is required")
	}
	if len(r.GetUserIds()) == 0 {
		return errors.New("user_ids must be non-empty")
	}
	return nil
}

// notifyRequestFromProto converts the wire request into a domain
// notify.NotifyRequest. The Addresses map is flattened from the
// nested-message form (ChannelAddresses) into a plain map[user][kind]string.
func notifyRequestFromProto(in *notifyv1.NotifyRequest) notify.NotifyRequest {
	channels := make([]notify.ChannelKind, 0, len(in.GetChannels()))
	for _, c := range in.GetChannels() {
		k := channelKindFromProto(c)
		if k != "" {
			channels = append(channels, k)
		}
	}
	var addresses map[string]map[notify.ChannelKind]string
	if len(in.GetAddresses()) > 0 {
		addresses = make(map[string]map[notify.ChannelKind]string, len(in.GetAddresses()))
		for userID, ca := range in.GetAddresses() {
			if ca == nil {
				continue
			}
			byChannel := make(map[notify.ChannelKind]string, len(ca.GetByChannel()))
			for name, addr := range ca.GetByChannel() {
				k := addressKeyToKind(name)
				if k == "" {
					continue
				}
				byChannel[k] = addr
			}
			if len(byChannel) > 0 {
				addresses[userID] = byChannel
			}
		}
	}
	return notify.NotifyRequest{
		TenantID:       in.GetTenantId(),
		NotificationID: in.GetNotificationId(),
		UserIDs:        in.GetUserIds(),
		Channels:       channels,
		SubjectRef:     in.GetSubjectRef(),
		SubjectType:    in.GetSubjectType(),
		Title:          in.GetTitle(),
		Body:           in.GetBody(),
		Addresses:      addresses,
	}
}

// clientHandler implements NotificationClientService.{GetNotifications,
// AckNotification, AckDataChange, RegisterPushToken}. StreamEvents has its
// own handler type because it owns the per-stream lifecycle (see stream.go).
type clientHandler struct {
	store    notify.Store
	stream   *streamHandler // optional; nil when live connections are disabled
	now      func() int64
}

func newClientHandler(store notify.Store, stream *streamHandler, now func() int64) *clientHandler {
	return &clientHandler{store: store, stream: stream, now: now}
}

// GetNotifications pages a user's inbox in newest-first order. The cursor is
// the same opaque string the previous response returned; clients never
// inspect it.
func (h *clientHandler) GetNotifications(
	ctx context.Context,
	req *connect.Request[notifyv1.GetNotificationsRequest],
) (*connect.Response[notifyv1.GetNotificationsResponse], error) {
	claims, ok := ClaimsFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("notify: claims missing on context"))
	}

	q := notify.Query{
		TenantID:   claims.TenantID,
		UserID:     claims.UserID,
		Limit:      int(req.Msg.GetLimit()),
		UnreadOnly: req.Msg.GetUnreadOnly(),
	}
	if c := req.Msg.GetCursor(); c != "" {
		ms, err := decodeCursor(c)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("notify: cursor: %w", err))
		}
		q.CursorMS = &ms
	}

	items, next, unread, err := h.store.QueryUserNotifications(ctx, q)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("notify: query: %w", err))
	}
	out := &notifyv1.GetNotificationsResponse{
		Notifications: make([]*notifyv1.Notification, 0, len(items)),
		NextCursor:    next,
		UnreadCount:   int32(unread),
	}
	for _, n := range items {
		out.Notifications = append(out.Notifications, notificationToProto(n))
	}
	return connect.NewResponse(out), nil
}

// AckNotification marks one row Read. The store key is the platform's id
// (Notification.ID), not the producer's idempotency key — handlers never
// translate between them.
func (h *clientHandler) AckNotification(
	ctx context.Context,
	req *connect.Request[notifyv1.AckNotificationRequest],
) (*connect.Response[notifyv1.AckNotificationResponse], error) {
	claims, ok := ClaimsFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("notify: claims missing on context"))
	}
	id := strings.TrimSpace(req.Msg.GetId())
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}
	if err := h.store.UpdateStatus(ctx, claims.TenantID, id, notify.StatusRead, h.now()); err != nil {
		if errors.Is(err, notify.ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("notify: ack: %w", err))
	}
	return connect.NewResponse(&notifyv1.AckNotificationResponse{}), nil
}

// AckDataChange cancels any pending retries for the (idempotency_key,
// session_id) pair on the in-memory RetryTracker. Sessions older than the
// process lifetime are silently ignored — the tracker treats unknown entries
// as no-ops.
func (h *clientHandler) AckDataChange(
	ctx context.Context,
	req *connect.Request[notifyv1.AckDataChangeRequest],
) (*connect.Response[notifyv1.AckDataChangeResponse], error) {
	if _, ok := ClaimsFromContext(ctx); !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("notify: claims missing on context"))
	}
	key := strings.TrimSpace(req.Msg.GetIdempotencyKey())
	sess := strings.TrimSpace(req.Msg.GetSessionId())
	if key == "" || sess == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("idempotency_key and session_id are required"))
	}
	if h.stream == nil {
		// Live connections disabled; ack is a no-op from the client's POV.
		return connect.NewResponse(&notifyv1.AckDataChangeResponse{}), nil
	}
	h.stream.retries.Ack(key, sess)
	return connect.NewResponse(&notifyv1.AckDataChangeResponse{}), nil
}

// RegisterPushToken upserts the (user, device_type) → token row. Re-calling
// rotates the token in place — the store's UpsertDevice contract is the
// source of truth on uniqueness.
func (h *clientHandler) RegisterPushToken(
	ctx context.Context,
	req *connect.Request[notifyv1.RegisterPushTokenRequest],
) (*connect.Response[notifyv1.RegisterPushTokenResponse], error) {
	claims, ok := ClaimsFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("notify: claims missing on context"))
	}
	dt := deviceTypeFromProto(req.Msg.GetDeviceType())
	if dt == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("device_type must be set"))
	}
	token := strings.TrimSpace(req.Msg.GetToken())
	if token == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("token is required"))
	}
	d := &notify.Device{
		TenantID:     claims.TenantID,
		UserID:       claims.UserID,
		DeviceType:   dt,
		Token:        token,
		LastActiveMS: h.now(),
	}
	if _, err := h.store.UpsertDevice(ctx, d); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("notify: upsert device: %w", err))
	}
	return connect.NewResponse(&notifyv1.RegisterPushTokenResponse{}), nil
}

// StreamEvents delegates to the streamHandler; if live connections are off the
// service returns Unimplemented so clients know to fall back to polling.
func (h *clientHandler) StreamEvents(
	ctx context.Context,
	req *connect.Request[notifyv1.StreamEventsRequest],
	stream *connect.ServerStream[notifyv1.StreamEvent],
) error {
	if h.stream == nil {
		return connect.NewError(connect.CodeUnimplemented, errors.New("notify: live connections disabled"))
	}
	return h.stream.serve(ctx, req, stream)
}

// encodeCursor / decodeCursor wrap the cursor-ms-int with base64 so the wire
// representation is opaque. Both directions are tiny and easy to test.
func encodeCursor(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return encodeCursorFor([]byte(strconv.FormatInt(ms, 10)))
}

// encodeCursorFor is the raw encode step, factored out so tests can feed
// pathological payloads (e.g. negative integers, empty strings) into
// decodeCursor without going through the sanitising encodeCursor path.
func encodeCursorFor(raw []byte) string {
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(c string) (int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return 0, err
	}
	ms, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, err
	}
	if ms <= 0 {
		return 0, errors.New("non-positive cursor")
	}
	return ms, nil
}
