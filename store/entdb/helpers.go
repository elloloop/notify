package entdb

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/elloloop/tenant-shard-db/sdk/go/entdb/v2"
	"google.golang.org/protobuf/proto"

	pb "github.com/elloloop/notify/gen/go/entdb_notify"
)

// EntDB type ids — duplicated here AND declared in the proto so the SDK reads
// them at runtime and the schemaless raw-transport path can resolve them by
// number without re-walking the descriptor on every call.
const (
	typeIDUserNotification   = 1
	typeIDDeviceRegistration = 2
)

// Field ids match the proto field numbers in proto/entdb_notify/notify.proto.
// They must stay in sync — the raw transport (QueryNodes) needs the numeric
// field id because notify runs schemaless and the server cannot resolve a
// name-keyed filter without a registered schema.
const (
	unFieldNotificationID = 1
	unFieldTenantID       = 2
	unFieldUserID         = 3
	unFieldSubjectRef     = 4
	unFieldSubjectType    = 5
	unFieldTitle          = 6
	unFieldBody           = 7
	unFieldChannel        = 8
	unFieldDeliveryStatus = 9
	unFieldCreatedAtMS    = 10
	unFieldDeliveredAtMS  = 11
	unFieldAckAtMS        = 12
	unFieldReadAtMS       = 13
	unFieldCompositeKey   = 14
)

const (
	drFieldTenantID     = 1
	drFieldUserID       = 2
	drFieldDeviceType   = 3
	drFieldToken        = 4
	drFieldCreatedAtMS  = 5
	drFieldLastActiveMS = 6
	drFieldCompositeKey = 7
)

// notifByCompositeKey / deviceByCompositeKey are the hand-rolled SDK
// UniqueKey tokens for the composite_key field on each node type. The SDK
// publishes protoc-gen-entdb-keys (issue #602, fixed in v2.0.5) for codegen
// these, but wiring a buf plugin step is a separate concern; the tokens are
// constructed directly here. Equivalent on the wire — the SDK transport reads
// only TypeID + FieldID and ignores Name.
var (
	notifByCompositeKey = sdk.UniqueKey[string]{
		TypeID:  typeIDUserNotification,
		FieldID: unFieldCompositeKey,
		Name:    "composite_key",
	}
	deviceByCompositeKey = sdk.UniqueKey[string]{
		TypeID:  typeIDDeviceRegistration,
		FieldID: drFieldCompositeKey,
		Name:    "composite_key",
	}
)

// notificationCompositeKey / deviceCompositeKey produce the encoded
// composite-key value the driver writes to (and reads from) the composite_key
// field. Each part is length-prefixed so a NotificationID containing the
// joining byte does not collide with a different composition that happens to
// produce the same flat string (the KeyEdge/SeparatorBytesDoNotCollide
// conformance subtest is the canary for exactly that bug).
func notificationCompositeKey(tenantID, userID, notificationID string) string {
	return lpEncode(tenantID) + lpEncode(userID) + lpEncode(notificationID)
}

func deviceCompositeKey(tenantID, userID, deviceType string) string {
	return lpEncode(tenantID) + lpEncode(userID) + lpEncode(deviceType)
}

// lpEncode emits "<len>:<value>" so a concatenation of length-prefixed parts
// is unambiguous regardless of which bytes the parts contain. Per-component
// hashing would also work but length-prefix preserves the original string
// for debugging.
func lpEncode(s string) string {
	return fmt.Sprintf("%d:%s", len(s), s)
}

// findNotificationByKey returns the node id matching the composite_key, or
// "" with no error if no row exists. Used by callers that hit a typed
// *UniqueConstraintError on Create and need to resolve the winner's id.
func (c *Client) findNotificationByKey(ctx context.Context, actor, key string) (string, error) {
	scope, err := c.scope(actor)
	if err != nil {
		return "", err
	}
	node, err := sdk.GetByKey(ctx, scope, notifByCompositeKey, key)
	if err != nil {
		if isTenantNotOpened(err) {
			return "", nil
		}
		return "", err
	}
	if node == nil {
		return "", nil
	}
	return node.NodeID, nil
}

func (c *Client) findDeviceByKey(ctx context.Context, actor, key string) (string, error) {
	scope, err := c.scope(actor)
	if err != nil {
		return "", err
	}
	node, err := sdk.GetByKey(ctx, scope, deviceByCompositeKey, key)
	if err != nil {
		if isTenantNotOpened(err) {
			return "", nil
		}
		return "", err
	}
	if node == nil {
		return "", nil
	}
	return node.NodeID, nil
}

// commitCreate runs Plan.Create + Commit(WithWaitApplied(true)) and returns
// the canonical node id.
//
// wait_applied (issue #606, fixed in v2.0.3) blocks the response until the WAL
// applier processes the op — so on success the returned id is the row the
// server accepted, and on a unique-constraint violation the caller sees a
// typed *sdk.UniqueConstraintError (issue #601, fixed in v2.0.4). This
// replaces the pre-v2.0.3 query-then-create + post-commit reconciliation
// dance: callers no longer need to discriminate "winner pre-allocated id ==
// canonical id" from "loser pre-allocated id != canonical id"; the typed
// error tells you directly which path you took.
func (c *Client) commitCreate(ctx context.Context, actor string, msg proto.Message) (string, error) {
	scope, err := c.scope(actor)
	if err != nil {
		return "", err
	}
	plan := scope.Plan()
	plan.Create(msg)
	res, err := plan.Commit(ctx, sdk.WithWaitApplied(true))
	if err != nil {
		return "", err
	}
	return firstCreatedID(res)
}

// commitUpdate runs Plan.Update + Commit(WithWaitApplied(true)) on an
// existing node id. wait_applied guarantees the write is applied before
// Commit returns, so post-commit visibility polling is unnecessary.
func (c *Client) commitUpdate(ctx context.Context, actor, nodeID string, patch proto.Message) error {
	scope, err := c.scope(actor)
	if err != nil {
		return err
	}
	plan := scope.Plan()
	plan.Update(nodeID, patch)
	_, err = plan.Commit(ctx, sdk.WithWaitApplied(true))
	return err
}

// commitUpdateFields runs Plan.UpdateFields (named-field patch) and waits
// for the applier. Because wait_applied blocks on the WRITER'S own commit
// offset, concurrent writers to the same row each finish when their OWN
// write lands — the old value-polling helper would deadlock when a racing
// writer overwrote the expected value before the poll observed it (issue
// #600, fixed in v2.0.4 with offset-based RAW). The wait-bool parameter the
// pre-v2 API exposed is gone; this is always wait_applied.
func (c *Client) commitUpdateFields(ctx context.Context, actor, nodeID string, patch proto.Message, fields ...string) error {
	scope, err := c.scope(actor)
	if err != nil {
		return err
	}
	plan := scope.Plan()
	plan.UpdateFields(nodeID, patch, fields...)
	_, err = plan.Commit(ctx, sdk.WithWaitApplied(true))
	return err
}

// firstCreatedID extracts the canonical id from a successful CommitResult.
// Errors here would mean the server returned Success=true with an empty
// CreatedNodeIDs slice — an upstream contract violation the driver surfaces
// rather than hiding.
func firstCreatedID(res *sdk.CommitResult) (string, error) {
	if res == nil {
		return "", errors.New("entdb: nil commit result")
	}
	if !res.Success {
		if res.Error != "" {
			return "", fmt.Errorf("entdb: commit failed: %s", res.Error)
		}
		return "", errors.New("entdb: commit not successful")
	}
	if len(res.CreatedNodeIDs) == 0 {
		return "", errors.New("entdb: commit succeeded but no node id returned")
	}
	return res.CreatedNodeIDs[0], nil
}

// getUserNotification reads a UserNotification by node id; returns (nil, nil)
// when not found so callers can `if got == nil` instead of `errors.Is(..., sentinel)`.
func (c *Client) getUserNotification(ctx context.Context, actor, nodeID string) (*pb.UserNotification, error) {
	scope, err := c.scope(actor)
	if err != nil {
		return nil, err
	}
	got, err := sdk.Get[*pb.UserNotification](ctx, scope, nodeID)
	if err != nil {
		return nil, err
	}
	if got == nil || !got.ProtoReflect().IsValid() {
		return nil, nil
	}
	return got, nil
}

func (c *Client) getDevice(ctx context.Context, actor, nodeID string) (*pb.DeviceRegistration, error) {
	scope, err := c.scope(actor)
	if err != nil {
		return nil, err
	}
	got, err := sdk.Get[*pb.DeviceRegistration](ctx, scope, nodeID)
	if err != nil {
		return nil, err
	}
	if got == nil || !got.ProtoReflect().IsValid() {
		return nil, nil
	}
	return got, nil
}

// queryUserNotifications runs a non-unique (tenant, user) filter via the raw
// transport because notify runs schemaless — the server rejects name-keyed
// filters without a registered schema, so the filter map uses NUMERIC
// field-id keys. A pre-write tenant returns a fresh-tenant signal which we
// translate to an empty result (identity's queryViaTransport has the same
// pattern).
func (c *Client) queryUserNotifications(ctx context.Context, actor, tenantID, userID string) ([]*sdk.Node, error) {
	transport := c.client.Transport()
	if transport == nil {
		return nil, errors.New("entdb: raw transport unavailable")
	}
	filter := map[string]any{
		fieldKey(unFieldTenantID): tenantID,
		fieldKey(unFieldUserID):   userID,
	}
	nodes, err := transport.QueryNodes(ctx, c.tenantID, actor, typeIDUserNotification, filter, 0)
	if err != nil {
		if isTenantNotOpened(err) {
			return nil, nil
		}
		return nil, err
	}
	return nodes, nil
}

func (c *Client) queryDevices(ctx context.Context, actor, tenantID, userID string) ([]*sdk.Node, error) {
	transport := c.client.Transport()
	if transport == nil {
		return nil, errors.New("entdb: raw transport unavailable")
	}
	filter := map[string]any{
		fieldKey(drFieldTenantID): tenantID,
		fieldKey(drFieldUserID):   userID,
	}
	nodes, err := transport.QueryNodes(ctx, c.tenantID, actor, typeIDDeviceRegistration, filter, 0)
	if err != nil {
		if isTenantNotOpened(err) {
			return nil, nil
		}
		return nil, err
	}
	return nodes, nil
}

// fieldKey returns the numeric field id as a decimal string — the raw
// transport's QueryNodes filter key shape when running schemaless.
func fieldKey(fieldID int) string {
	return decString(fieldID)
}

func decString(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// unmarshalNotification rehydrates a node returned from the raw transport
// into a typed UserNotification. The raw path returns id + structpb payload
// but no typed value; we re-read via sdk.Get against the node id to
// preserve fidelity for fields whose proto kind structpb mangles (notably
// int64 above 2^53). Pre-v2.0.3 SDKs were where that mattered; with v2's
// EntValue wire path the workaround is belt-and-braces but worth keeping
// until we have a typed list helper.
func (c *Client) unmarshalNotification(ctx context.Context, actor string, node *sdk.Node) (string, *pb.UserNotification, error) {
	if node == nil {
		return "", nil, nil
	}
	msg, err := c.getUserNotification(ctx, actor, node.NodeID)
	if err != nil {
		return "", nil, err
	}
	return node.NodeID, msg, nil
}

func (c *Client) unmarshalDevice(ctx context.Context, actor string, node *sdk.Node) (string, *pb.DeviceRegistration, error) {
	if node == nil {
		return "", nil, nil
	}
	msg, err := c.getDevice(ctx, actor, node.NodeID)
	if err != nil {
		return "", nil, err
	}
	return node.NodeID, msg, nil
}
