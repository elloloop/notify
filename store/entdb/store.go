package entdb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	sdk "github.com/elloloop/tenant-shard-db/sdk/go/entdb/v2"
	"google.golang.org/protobuf/proto"

	"github.com/elloloop/notify"
	pb "github.com/elloloop/notify/gen/go/entdb_notify"
)

// Store implements notify.Store on top of tenant-shard-db.
type Store struct {
	c *Client
}

// New returns a Store wrapping the SDK client + tenant id.
func New(client *sdk.DbClient, tenantID string) *Store {
	return &Store{c: NewClient(client, tenantID)}
}

// NewFromClient wraps an existing *Client (used by tests that share an
// SDK handle across subtests but allocate a fresh tenant per subtest).
func NewFromClient(c *Client) *Store {
	return &Store{c: c}
}

var _ notify.Store = (*Store)(nil)

// notificationToProto / notificationFromProto / deviceToProto / deviceFromProto
// move data between the notify.Store domain types and the proto-on-the-wire
// shape. The notification's ID is not on the proto — EntDB assigns node ids
// server-side; the typed payload only carries the caller-supplied
// idempotency key (NotificationID).
func notificationToProto(n *notify.Notification) *pb.UserNotification {
	if n == nil {
		return nil
	}
	return &pb.UserNotification{
		NotificationId: n.NotificationID,
		TenantId:       n.TenantID,
		UserId:         n.UserID,
		SubjectRef:     n.SubjectRef,
		SubjectType:    n.SubjectType,
		Title:          n.Title,
		Body:           n.Body,
		Channel:        string(n.Channel),
		DeliveryStatus: string(n.Status),
		CreatedAtMs:    n.CreatedAtMS,
		DeliveredAtMs:  n.DeliveredAtMS,
		AckAtMs:        n.AckAtMS,
		ReadAtMs:       n.ReadAtMS,
		CompositeKey:   notificationCompositeKey(n.TenantID, n.UserID, n.NotificationID),
	}
}

func notificationFromProto(id string, p *pb.UserNotification) *notify.Notification {
	if p == nil {
		return nil
	}
	return &notify.Notification{
		ID:             id,
		NotificationID: p.GetNotificationId(),
		TenantID:       p.GetTenantId(),
		UserID:         p.GetUserId(),
		SubjectRef:     p.GetSubjectRef(),
		SubjectType:    p.GetSubjectType(),
		Title:          p.GetTitle(),
		Body:           p.GetBody(),
		Channel:        notify.ChannelKind(p.GetChannel()),
		Status:         notify.DeliveryStatus(p.GetDeliveryStatus()),
		CreatedAtMS:    p.GetCreatedAtMs(),
		DeliveredAtMS:  p.GetDeliveredAtMs(),
		AckAtMS:        p.GetAckAtMs(),
		ReadAtMS:       p.GetReadAtMs(),
	}
}

func deviceToProto(d *notify.Device) *pb.DeviceRegistration {
	if d == nil {
		return nil
	}
	return &pb.DeviceRegistration{
		TenantId:     d.TenantID,
		UserId:       d.UserID,
		DeviceType:   d.DeviceType,
		Token:        d.Token,
		CreatedAtMs:  d.CreatedAtMS,
		LastActiveMs: d.LastActiveMS,
		CompositeKey: deviceCompositeKey(d.TenantID, d.UserID, d.DeviceType),
	}
}

func deviceFromProto(id string, p *pb.DeviceRegistration) *notify.Device {
	if p == nil {
		return nil
	}
	return &notify.Device{
		ID:           id,
		TenantID:     p.GetTenantId(),
		UserID:       p.GetUserId(),
		DeviceType:   p.GetDeviceType(),
		Token:        p.GetToken(),
		CreatedAtMS:  p.GetCreatedAtMs(),
		LastActiveMS: p.GetLastActiveMs(),
	}
}

// CreateNotification is idempotent on (TenantID, UserID, NotificationID).
// The triple is materialized into a composite_key value declared
// (entdb.field).unique in proto/entdb_notify/notify.proto and enforced
// server-side by tenant-shard-db v2 schema-aware mode (registered via
// sdk.WithSchema at client construction).
//
// Implementation. Submit an optimistic Create + Commit(WithWaitApplied(true))
// — the server's WAL applier accepts or rejects the row before Commit
// returns. On success the returned id IS the canonical row id; on a
// composite-key collision the SDK surfaces a typed *sdk.UniqueConstraintError
// and we resolve the canonical id via findNotificationByKey.
//
// This replaces the pre-v2.0.3 query-then-create + post-commit reconciliation
// pattern (no longer needed now that wait_applied is real — issue #606).
// Idempotent re-create returns created=false without mutating the stored
// row; memory's reference Store has the same property.
func (s *Store) CreateNotification(ctx context.Context, n *notify.Notification) (bool, error) {
	if n == nil {
		return false, errors.New("entdb: CreateNotification: nil notification")
	}
	ctx = ctxOrBackground(ctx)
	actor := notifActor(n.UserID)

	msg := notificationToProto(n)
	canonicalID, err := s.c.commitCreate(ctx, actor, msg)
	if err == nil {
		n.ID = canonicalID
		return true, nil
	}

	var uce *sdk.UniqueConstraintError
	if !errors.As(err, &uce) {
		return false, err
	}
	// Server-enforced unique constraint rejected the write: either a
	// deliberate re-create with the same idempotency key, or the loser of
	// a concurrent same-key race. Resolve to the canonical row.
	key := notificationCompositeKey(n.TenantID, n.UserID, n.NotificationID)
	existingID, ferr := s.c.findNotificationByKey(ctx, actor, key)
	if ferr != nil {
		return false, ferr
	}
	if existingID == "" {
		return false, fmt.Errorf("entdb: unique constraint hit but key %q not resolvable", key)
	}
	n.ID = existingID
	return false, nil
}

// GetNotification returns the row scoped to (tenantID, userID). A mismatch on
// either field surfaces as notify.ErrNotFound — the UserIsolation conformance
// subtest asserts cross-user and cross-tenant reads MUST miss.
func (s *Store) GetNotification(ctx context.Context, tenantID, userID, id string) (*notify.Notification, error) {
	ctx = ctxOrBackground(ctx)
	got, err := s.c.getUserNotification(ctx, notifActor(userID), id)
	if err != nil {
		return nil, err
	}
	if got == nil {
		// systemActor read so a cross-user query surfaces as "not found"
		// rather than as "access denied".
		got, err = s.c.getUserNotification(ctx, systemActor, id)
		if err != nil {
			return nil, err
		}
		if got == nil {
			return nil, notify.ErrNotFound
		}
	}
	if got.GetTenantId() != tenantID || got.GetUserId() != userID {
		return nil, notify.ErrNotFound
	}
	return notificationFromProto(id, got), nil
}

// UpdateStatus sets delivery_status and the timestamp field corresponding to
// the new status. UpdateFields names both fields explicitly so a proto3 zero
// value (e.g. atMS=0 clearing a timestamp) still goes on the wire — proto3
// Update would otherwise drop the zero-value write.
//
// wait_applied (issue #606) makes the post-commit visibility a non-issue
// even under concurrent UpdateStatus writers on the same row: each goroutine
// blocks on its OWN offset, not on a specific value reading back, so the
// pre-v2.0.4 value-poll deadlock (issue #600) does not apply.
func (s *Store) UpdateStatus(ctx context.Context, tenantID, id string, status notify.DeliveryStatus, atMS int64) error {
	ctx = ctxOrBackground(ctx)

	// Look up the row first to verify tenant scope and route the write
	// under the row's owning user actor. systemActor is required because
	// the caller has no userID.
	got, err := s.c.getUserNotification(ctx, systemActor, id)
	if err != nil {
		return err
	}
	if got == nil || got.GetTenantId() != tenantID {
		return notify.ErrNotFound
	}
	actor := notifActor(got.GetUserId())

	patch := &pb.UserNotification{DeliveryStatus: string(status)}
	fields := []string{"delivery_status"}
	switch status {
	case notify.StatusDelivered:
		patch.DeliveredAtMs = atMS
		fields = append(fields, "delivered_at_ms")
	case notify.StatusAcked:
		patch.AckAtMs = atMS
		fields = append(fields, "ack_at_ms")
	case notify.StatusRead:
		patch.ReadAtMs = atMS
		fields = append(fields, "read_at_ms")
	}
	return s.c.commitUpdateFields(ctx, actor, id, patch, fields...)
}

// QueryUserNotifications pages newest-first via CursorMS, walks past EntDB's
// per-call row cap via the SDK's v1.24+ keyset auto-follow, and computes
// unread separately from the page window.
func (s *Store) QueryUserNotifications(ctx context.Context, q notify.Query) ([]*notify.Notification, string, int, error) {
	ctx = ctxOrBackground(ctx)
	limit := notify.ClampLimit(q.Limit)

	// systemActor read so UserIsolation's cross-user query surfaces as
	// empty rather than ACCESS_DENIED. The transport-level WHERE filter
	// scopes to the exact user; a defensive post-filter below catches any
	// stale-shard read.
	nodes, err := s.c.queryUserNotifications(ctx, systemActor, q.TenantID, q.UserID)
	if err != nil {
		return nil, "", 0, err
	}

	// The raw transport returns the Node (id + payload struct), but we
	// rehydrate each entry via sdk.Get against the node id. That extra
	// hop preserves wire fidelity for fields whose proto kind structpb
	// historically mangled (notably int64 above 2^53); v2's EntValue wire
	// path closes that bug class but the typed read is still belt-and-braces.
	rows := make([]*notify.Notification, 0, len(nodes))
	unread := 0
	for _, node := range nodes {
		id, msg, uerr := s.c.unmarshalNotification(ctx, systemActor, node)
		if uerr != nil {
			return nil, "", 0, uerr
		}
		if msg == nil {
			continue
		}
		if msg.GetTenantId() != q.TenantID || msg.GetUserId() != q.UserID {
			continue
		}
		if notify.DeliveryStatus(msg.GetDeliveryStatus()) != notify.StatusRead {
			unread++
		}
		if q.UnreadOnly && notify.DeliveryStatus(msg.GetDeliveryStatus()) == notify.StatusRead {
			continue
		}
		if q.CursorMS != nil && msg.GetCreatedAtMs() >= *q.CursorMS {
			continue
		}
		rows = append(rows, notificationFromProto(id, msg))
	}

	// Newest-first; tiebreak on id so subtests with identical CreatedAtMS
	// see a stable order.
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].CreatedAtMS != rows[j].CreatedAtMS {
			return rows[i].CreatedAtMS > rows[j].CreatedAtMS
		}
		return rows[i].ID > rows[j].ID
	})

	nextCursor := ""
	if len(rows) > limit {
		rows = rows[:limit]
		nextCursor = strconv.FormatInt(rows[len(rows)-1].CreatedAtMS, 10)
	}
	return rows, nextCursor, unread, nil
}

// UpsertDevice inserts a new device row or, when one already exists for the
// composite key (TenantID, UserID, DeviceType), rotates the token and
// last_active_ms on the canonical row.
//
// Implementation. Submit an optimistic Create + Commit(WithWaitApplied(true)).
// First-time inserts succeed and we read the row back. A composite-key
// collision (idempotent re-register, or a concurrent race-loser) surfaces
// as *sdk.UniqueConstraintError; the handler resolves the canonical id via
// findDeviceByKey and rotates the token + last_active_ms via
// commitUpdateFields. Race-losers may stack their rotations against the
// same canonical id; wait_applied means each rotation lands at its own
// commit offset and the final state converges on whichever applier-batch
// landed last (memory's reference is also last-write-wins).
func (s *Store) UpsertDevice(ctx context.Context, d *notify.Device) (*notify.Device, error) {
	if d == nil {
		return nil, errors.New("entdb: UpsertDevice: nil device")
	}
	ctx = ctxOrBackground(ctx)
	actor := notifActor(d.UserID)

	canonicalID, err := s.c.commitCreate(ctx, actor, deviceToProto(d))
	if err == nil {
		// First-time insert: the values we just sent ARE the row.
		d.ID = canonicalID
		return s.readDeviceBack(ctx, actor, canonicalID, d)
	}

	var uce *sdk.UniqueConstraintError
	if !errors.As(err, &uce) {
		return nil, err
	}
	// Existing row for this composite key — rotate token + last_active_ms.
	key := deviceCompositeKey(d.TenantID, d.UserID, d.DeviceType)
	existingID, ferr := s.c.findDeviceByKey(ctx, actor, key)
	if ferr != nil {
		return nil, ferr
	}
	if existingID == "" {
		return nil, fmt.Errorf("entdb: UpsertDevice: unique constraint hit but key %q not resolvable", key)
	}
	patch := &pb.DeviceRegistration{
		Token:        d.Token,
		LastActiveMs: d.LastActiveMS,
	}
	if err := s.c.commitUpdateFields(ctx, actor, existingID, patch, "token", "last_active_ms"); err != nil {
		return nil, err
	}
	return s.readDeviceBack(ctx, actor, existingID, d)
}

// readDeviceBack does the post-mutate Get used by UpsertDevice. On a
// vanished-row sentinel it falls back to a synthesized device built from
// the caller's input + the canonical id — the caller at least gets the id
// and the values they just wrote.
func (s *Store) readDeviceBack(ctx context.Context, actor, id string, fallback *notify.Device) (*notify.Device, error) {
	got, err := s.c.getDevice(ctx, actor, id)
	if err != nil {
		return nil, err
	}
	if got == nil {
		return &notify.Device{
			ID:           id,
			TenantID:     fallback.TenantID,
			UserID:       fallback.UserID,
			DeviceType:   fallback.DeviceType,
			Token:        fallback.Token,
			CreatedAtMS:  fallback.CreatedAtMS,
			LastActiveMS: fallback.LastActiveMS,
		}, nil
	}
	return deviceFromProto(id, got), nil
}

// ListDevices returns every device row for a user, ordered by device type
// for deterministic output (matches memory's contract).
func (s *Store) ListDevices(ctx context.Context, tenantID, userID string) ([]*notify.Device, error) {
	ctx = ctxOrBackground(ctx)
	nodes, err := s.c.queryDevices(ctx, systemActor, tenantID, userID)
	if err != nil {
		return nil, err
	}
	var out []*notify.Device
	for _, node := range nodes {
		id, msg, uerr := s.c.unmarshalDevice(ctx, systemActor, node)
		if uerr != nil {
			return nil, uerr
		}
		if msg == nil {
			continue
		}
		if msg.GetTenantId() != tenantID || msg.GetUserId() != userID {
			continue
		}
		out = append(out, deviceFromProto(id, msg))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeviceType < out[j].DeviceType })
	return out, nil
}

// Compile-time interface check; also ensures proto.Marshal is reachable
// through the build graph for any future raw-wire helper added here.
var _ proto.Message = (*pb.UserNotification)(nil)
