package entdb

import (
	"context"
	"errors"
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

// notificationToProto converts a notify.Notification to its protobuf
// representation. ID is intentionally not on the proto — EntDB assigns
// node ids server-side; the typed payload only carries the
// caller-supplied NotificationID (idempotency key).
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
// The driver materializes that triple into a composite_key value and
// relies on tenant-shard-db v2.0.1's server-enforced unique constraint
// (declared in proto/entdb_notify/notify.proto and registered via
// sdk.WithSchema at client construction) to serialize concurrent same-key
// creates: only one row persists no matter how many goroutines race.
//
// Implementation note. The Go SDK does NOT expose `wait_applied` on
// Plan.Commit, so the server's ALREADY_EXISTS does not surface as a typed
// gRPC error to the loser; instead, every racer's Commit "succeeds" with
// a pre-allocated UUID, and the WAL applier accepts exactly one of those
// UUIDs (the rest are silently rejected by the schema's unique index).
// [Client.commitCreateUnique] reconciles this by resolving the canonical
// row via sdk.GetByKey on composite_key — winner sees pre-allocated id
// == canonical id (returns created=true); loser sees them differ
// (returns created=false with the canonical id).
//
// A pre-emptive GetByKey is retained for the warm-path "I'm just calling
// CreateNotification twice with the same key in sequence" case — it
// short-circuits the second submit and avoids the extra Commit round
// trip. The race-safety guarantee comes from the post-commit reconcile,
// not from this pre-check.
func (s *Store) CreateNotification(ctx context.Context, n *notify.Notification) (bool, error) {
	if n == nil {
		return false, errors.New("entdb: CreateNotification: nil notification")
	}
	ctx = ctxOrBackground(ctx)
	actor := notifActor(n.UserID)

	key := notificationCompositeKey(n.TenantID, n.UserID, n.NotificationID)
	existingID, err := s.c.findNotificationByKey(ctx, actor, key)
	if err != nil {
		return false, err
	}
	if existingID != "" {
		n.ID = existingID
		// Idempotent re-create must not mutate the stored row — the
		// caller's mutated fields stay on the caller's struct but never
		// reach EntDB. Memory's reference Store has the same property.
		return false, nil
	}

	msg := notificationToProto(n)
	canonicalID, won, err := s.c.commitCreateUnique(ctx, actor, msg, notifByCompositeKey, key)
	if err != nil {
		return false, err
	}
	n.ID = canonicalID
	return won, nil
}

// GetNotification returns the row scoped to (tenantID, userID). Mismatch
// surfaces as notify.ErrNotFound — the conformance suite asserts cross-user
// and cross-tenant reads miss.
func (s *Store) GetNotification(ctx context.Context, tenantID, userID, id string) (*notify.Notification, error) {
	ctx = ctxOrBackground(ctx)
	got, err := s.c.getUserNotification(ctx, notifActor(userID), id)
	if err != nil {
		return nil, err
	}
	if got == nil {
		// systemActor read so a cross-user query still surfaces as
		// "not found" rather than as "access denied".
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

// UpdateStatus sets delivery_status and the timestamp field corresponding
// to the new status. Uses UpdateFields so a 0 timestamp (e.g. clearing an
// ack on a re-read) still goes on the wire — proto3 Update would otherwise
// silently drop a zero-value write.
func (s *Store) UpdateStatus(ctx context.Context, tenantID, id string, status notify.DeliveryStatus, atMS int64) error {
	ctx = ctxOrBackground(ctx)

	// Look up the row first so (a) we can verify tenant scope and
	// (b) we can route the write under the row's owning user actor.
	// systemActor is required because the caller has no userID.
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
	// wait=false: concurrent UpdateStatus writers on the same row
	// observe each other's values during the wait window and the
	// deadline expires even though the write committed. The
	// conformance ConcurrentUpdateStatus_NoError subtest is the canary
	// — accepting eventual consistency on the status field is the
	// pragmatic fix.
	return s.c.commitUpdateFields(ctx, actor, id, patch, false, fields...)
}

// QueryUserNotifications pages newest-first via CursorMS, walks past
// EntDB's per-call row cap (the SDK's auto-follow is on by default in
// v1.24.0+), and computes unread separately from the page window.
func (s *Store) QueryUserNotifications(ctx context.Context, q notify.Query) ([]*notify.Notification, string, int, error) {
	ctx = ctxOrBackground(ctx)
	limit := notify.ClampLimit(q.Limit)

	// systemActor read so the suite's UserIsolation subtest, which
	// queries cross-user, surfaces an empty result rather than
	// ACCESS_DENIED. The transport-level WHERE filter scopes to the
	// exact user.
	nodes, err := s.c.queryUserNotifications(ctx, systemActor, q.TenantID, q.UserID)
	if err != nil {
		return nil, "", 0, err
	}

	// The transport returns the Node (payload struct), but our typed
	// reader rehydrates each entry via sdk.Get against the node id.
	// That extra hop is unavoidable: the typed wire format and the
	// raw transport's structpb shape are NOT 1:1 for fields whose
	// proto kind doesn't survive structpb (notably int64 above 2^53),
	// so we re-read via the typed path to preserve fidelity.
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
		// Defensive: the transport's WHERE filter already scopes to
		// tenant + user, but a stale read could surface mismatched
		// rows during shard rebalances. Drop anything off-scope.
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

	// Newest-first; tiebreak on id so subtests with identical
	// CreatedAtMS see a stable order.
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

// UpsertDevice inserts a new device row or updates the token /
// last_active_ms timestamps on the existing row keyed by (TenantID,
// UserID, DeviceType). With tenant-shard-db v2.0.1's server-enforced
// composite_key unique constraint, concurrent racers with the same key
// produce exactly one row: the winner persists, every loser's Create is
// silently rejected by the WAL applier, and the loser's submitted-but-
// not-persisted UUID is reconciled against the canonical row via
// [Client.commitCreateUnique]. The loser then runs UpdateFields against
// the canonical id, preserving the "last write wins token rotation"
// semantics memory's Store models — racers all observe the same
// canonical row at the end of the dust-up.
func (s *Store) UpsertDevice(ctx context.Context, d *notify.Device) (*notify.Device, error) {
	if d == nil {
		return nil, errors.New("entdb: UpsertDevice: nil device")
	}
	ctx = ctxOrBackground(ctx)
	actor := notifActor(d.UserID)

	key := deviceCompositeKey(d.TenantID, d.UserID, d.DeviceType)

	// Fast path: a pre-existing row for this composite key is just a
	// rotate-in-place update. This is the warm path memory hits and
	// the single-threaded DeviceUpsertRotation subtest exercises;
	// wait=true gives strict read-your-writes against the rotated
	// token.
	existingID, err := s.c.findDeviceByKey(ctx, actor, key)
	if err != nil {
		return nil, err
	}
	if existingID != "" {
		return s.rotateDeviceToken(ctx, actor, existingID, d, true)
	}

	// Cold path: submit a Create. Multiple racers may submit at once;
	// the server's unique-index check accepts exactly one. The loser
	// learns its canonical id via the unique-key reconcile, then runs
	// the same rotate-in-place update against it. This preserves
	// "every UpsertDevice for the same key sees a single row" against
	// arbitrary write concurrency.
	msg := deviceToProto(d)
	canonicalID, won, err := s.c.commitCreateUnique(ctx, actor, msg, deviceByCompositeKey, key)
	if err != nil {
		return nil, err
	}
	if !won {
		// Lost the create race. The canonical row holds whatever fields
		// the winner wrote; this caller's token / last_active_ms still
		// need to land so memory's "last writer wins" semantics survive
		// the loss. wait=false here because the racing-losers path runs
		// N concurrent rotates against the same canonical id and each
		// loser's post-commit poll would observe a different goroutine's
		// values; the writes themselves all commit and the final state
		// converges on whichever applier-batch landed last.
		return s.rotateDeviceToken(ctx, actor, canonicalID, d, false)
	}
	d.ID = canonicalID
	got, gerr := s.c.getDevice(ctx, actor, canonicalID)
	if gerr != nil {
		return nil, gerr
	}
	if got == nil {
		// Reconcile said the row exists but a follow-up Get could not
		// see it — surface the input as-is so the caller at least has
		// the canonical id and the values it wrote.
		return &notify.Device{
			ID:           canonicalID,
			TenantID:     d.TenantID,
			UserID:       d.UserID,
			DeviceType:   d.DeviceType,
			Token:        d.Token,
			CreatedAtMS:  d.CreatedAtMS,
			LastActiveMS: d.LastActiveMS,
		}, nil
	}
	return deviceFromProto(canonicalID, got), nil
}

// rotateDeviceToken applies a token + last_active_ms patch on the
// canonical device row and returns the post-update view. The patch
// names both fields explicitly so a 0 LastActiveMS still writes
// (proto3 Update drops zero values without an explicit field list).
//
// The wait argument controls the post-commit read-your-writes loop.
// Pass true on the fast path (single caller rotating an existing row —
// the DeviceUpsertRotation subtest wants strict RYW). Pass false on the
// race-loser path where N concurrent rotates target the same canonical
// id and the loop would observe whichever racer's values landed last,
// timing out even though every write committed (matches the pattern
// UpdateStatus uses to escape its own ConcurrentUpdateStatus_NoError
// canary).
func (s *Store) rotateDeviceToken(ctx context.Context, actor, deviceID string, d *notify.Device, wait bool) (*notify.Device, error) {
	patch := &pb.DeviceRegistration{
		Token:        d.Token,
		LastActiveMs: d.LastActiveMS,
	}
	if err := s.c.commitUpdateFields(ctx, actor, deviceID, patch, wait, "token", "last_active_ms"); err != nil {
		return nil, err
	}
	got, gerr := s.c.getDevice(ctx, actor, deviceID)
	if gerr != nil {
		return nil, gerr
	}
	if got == nil {
		return nil, errors.New("entdb: UpsertDevice: row vanished after update")
	}
	return deviceFromProto(deviceID, got), nil
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
