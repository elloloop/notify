package entdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	sdk "github.com/elloloop/tenant-shard-db/sdk/go/entdb/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/elloloop/notify/gen/go/entdb_notify"
)

// EntDB type ids — these are duplicated here AND declared in the proto
// for the SDK to read at runtime. They are intentionally kept in lockstep
// so a name-keyed Query helper (which goes through the raw transport) can
// resolve the type id without re-walking the descriptor on every call.
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
// docs say to use the protoc-gen-entdb-keys codegen — that codegen is not
// wired into this repo, so the tokens are constructed directly. Equivalent
// on the wire (the SDK transport reads only TypeID + FieldID).
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
// composite-key value the driver writes to (and reads from) the
// composite_key field. The bytes "|" do not collide because both parts of
// each tuple are user-supplied raw values: a user that sends a
// NotificationID containing '|' MUST end up in a distinct row from a user
// whose user_id contains '|'.
//
// To keep separator-byte collisions impossible, we length-prefix every
// part with its rune-count followed by ':'. So "acme|u1|nid" written as
// (acme,u1,nid) and (acme|u1,nid) hash to different strings even though
// the naive '|'-joined form is the same. The KeyEdge conformance test
// covers exactly this case.
func notificationCompositeKey(tenantID, userID, notificationID string) string {
	return lpEncode(tenantID) + lpEncode(userID) + lpEncode(notificationID)
}

func deviceCompositeKey(tenantID, userID, deviceType string) string {
	return lpEncode(tenantID) + lpEncode(userID) + lpEncode(deviceType)
}

// lpEncode emits "<len>:<value>" so a concatenation of length-prefixed
// fields is unambiguous regardless of which bytes the parts contain. This
// is the simplest fix for the KeyEdge "separator bytes" probe; a
// per-component hash would also work but length-prefix preserves the
// original string for debugging.
func lpEncode(s string) string {
	return fmt.Sprintf("%d:%s", len(s), s)
}

// findNotificationByKey returns the node id matching the composite key,
// or "" with no error if no row exists. Uses the SDK's typed
// GetByKey path so the unique-index is exercised.
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
		// GetByKey on a never-indexed field can return a "no matching
		// node" sentinel — the SDK surfaces that as a nil node + nil
		// err. The error path is reserved for genuine transport / ACL
		// failures.
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

// firstCreatedID returns the first id from a successful CommitResult, or
// an error if the commit failed or yielded no created node.
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

// commitCreate runs Plan.Create + Commit and returns the new node id.
// The post-commit `waitForNodeVisible` is the read-your-writes loop:
// tenant-shard-db's WAL applier catches up asynchronously, so the next
// read can miss the row we just wrote without an explicit wait.
//
// Concurrent same-key inserts: on tenant-shard-db v2.0.1 with a
// registered schema, only one of N racing creates persists — the others
// are rejected by the server-enforced unique constraint. But Plan.Commit
// without `wait_applied` does NOT surface that rejection: the loser
// gets a synthetic "success" response carrying its pre-allocated UUID,
// then `waitForNodeVisible` times out polling for a node that was never
// written. Callers that key writes by a unique field (e.g. composite_key)
// should use [commitCreateUnique] instead — it resolves the canonical
// row via the unique-key index and discriminates winner from loser by
// comparing the returned id to the one the SDK pre-allocated. The Go
// SDK does not expose `wait_applied` on Plan.Commit (Python does); when
// upstream adds it this driver can collapse back to a single helper.
func (c *Client) commitCreate(ctx context.Context, actor string, msg proto.Message) (string, error) {
	id, err := c.commitCreateNoWait(ctx, actor, msg)
	if err != nil {
		return "", err
	}
	if err := c.waitForNodeVisible(ctx, actor, msg, id); err != nil {
		return "", err
	}
	return id, nil
}

// commitCreateNoWait runs Plan.Create + Commit without the post-commit
// visibility poll. Returns the SDK-pre-allocated UUID — which is the
// canonical row id ONLY if this caller won any racing unique-constraint
// check on the server. Loser callers receive a UUID that never actually
// landed on disk; they MUST resolve the canonical id via the unique-key
// index ([commitCreateUnique] does that).
func (c *Client) commitCreateNoWait(ctx context.Context, actor string, msg proto.Message) (string, error) {
	scope, err := c.scope(actor)
	if err != nil {
		return "", err
	}
	plan := scope.Plan()
	plan.Create(msg)
	res, err := plan.Commit(ctx)
	if err != nil {
		return "", err
	}
	return firstCreatedID(res)
}

// commitCreateUnique is the create path for protos that carry a
// unique-indexed field (composite_key on UserNotification /
// DeviceRegistration). It submits the create, then resolves the
// canonical row id via [sdk.GetByKey] with a bounded poll budget
// because the WAL applier is asynchronous.
//
// The winner sees its SDK-pre-allocated id == the canonical id (the
// server accepted the row). A loser sees them differ — the canonical
// id is the winner's row, the pre-allocated id never persisted because
// the v2 server's composite_key unique constraint rejected it. Both
// callers end up with the canonical id; `won` discriminates them so
// the loser can short-circuit follow-up writes (e.g. UpsertDevice's
// token rotation must run against the canonical id, not the loser's
// phantom id).
func (c *Client) commitCreateUnique(
	ctx context.Context,
	actor string,
	msg proto.Message,
	key sdk.UniqueKey[string],
	keyValue string,
) (canonicalID string, won bool, err error) {
	submittedID, err := c.commitCreateNoWait(ctx, actor, msg)
	if err != nil {
		return "", false, err
	}
	canonical, err := c.findByKeyWithRetry(ctx, actor, key, keyValue)
	if err != nil {
		return "", false, err
	}
	if canonical == "" {
		return "", false, fmt.Errorf("entdb: row vanished after commit for key=%q", keyValue)
	}
	return canonical, canonical == submittedID, nil
}

// findByKeyWithRetry polls sdk.GetByKey until the unique-indexed row
// becomes visible, with the same 5s ceiling [waitForNodeVisible] uses.
// Returns "" with nil error when the deadline expires without finding
// the row — that surfaces as the "row vanished" error from
// [commitCreateUnique], the diagnostic signal for a server that lost
// the write.
func (c *Client) findByKeyWithRetry(ctx context.Context, actor string, key sdk.UniqueKey[string], keyValue string) (string, error) {
	deadline := time.Now().Add(5 * time.Second)
	scope, err := c.scope(actor)
	if err != nil {
		return "", err
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		node, gerr := sdk.GetByKey(ctx, scope, key, keyValue)
		if gerr == nil && node != nil {
			return node.NodeID, nil
		}
		if gerr != nil && !isTenantNotOpened(gerr) {
			return "", gerr
		}
		if time.Now().After(deadline) {
			return "", nil
		}
		if serr := sleepOrCtx(ctx, 25*time.Millisecond); serr != nil {
			return "", serr
		}
	}
}

// commitUpdate runs Plan.Update + Commit on an existing node id. The
// patch's set fields are merged onto the stored row.
func (c *Client) commitUpdate(ctx context.Context, actor, nodeID string, patch proto.Message) error {
	scope, err := c.scope(actor)
	if err != nil {
		return err
	}
	plan := scope.Plan()
	plan.Update(nodeID, patch)
	if _, err := plan.Commit(ctx); err != nil {
		return err
	}
	return c.waitForPatchVisible(ctx, actor, nodeID, patch)
}

// commitUpdateFields runs Plan.UpdateFields, which (unlike Update) names
// the fields explicitly and so can write a proto3 zero value (0 / "" /
// false). Required for status timestamps that may legitimately be 0
// and for the "set X to its default" pattern.
//
// The post-commit visibility wait is gated behind `wait`: under
// concurrent UpdateStatus races (different goroutines writing different
// status values to the same row), the wait observes a different
// goroutine's value and the deadline expires even though the write
// committed. Callers that need immediate read-your-writes pass true;
// callers that issue racing writes pass false and accept the
// eventual-consistency window. Memory's reference is synchronous, so
// the conformance suite's single-threaded subtests pass with wait=true.
func (c *Client) commitUpdateFields(ctx context.Context, actor, nodeID string, patch proto.Message, wait bool, fields ...string) error {
	scope, err := c.scope(actor)
	if err != nil {
		return err
	}
	plan := scope.Plan()
	plan.UpdateFields(nodeID, patch, fields...)
	if _, err := plan.Commit(ctx); err != nil {
		return err
	}
	if !wait {
		return nil
	}
	return c.waitForFieldsVisible(ctx, actor, nodeID, patch, fields)
}

// getUserNotification reads a UserNotification by node id; returns
// (nil, nil) when not found.
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

// queryUserNotifications runs a non-unique tenant+user filter and
// returns matching rows. Translates the fresh-tenant signal into an empty
// result. The filter map uses NUMERIC field-id keys because notify runs
// EntDB schemaless and the server rejects name-keyed filters without a
// registered schema (identity has the same pattern in queryViaTransport).
func (c *Client) queryUserNotifications(ctx context.Context, actor, tenantID, userID string) ([]*sdk.Node, error) {
	transport := c.client.Transport()
	if transport == nil {
		return nil, errors.New("entdb: raw transport unavailable")
	}
	filter := map[string]any{
		// Numeric field ids as strings — this is the schemaless escape
		// hatch the server accepts even when no schema is registered.
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

// fieldKey returns the numeric field id as a decimal string — the
// raw transport's QueryNodes filter key shape when running schemaless.
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

// unmarshalNotification builds a typed UserNotification from a Node's
// payload using the SDK's wire-format helper exposed via Get.
//
// Reading via the raw transport returns the Node (id + payload struct)
// but no typed value; rebuilding via sdk.Get against the node id is the
// supported path. We do that one extra hop here.
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

// waitForNodeVisible polls until a typed Get for the just-written node
// succeeds, with a bounded deadline so a server that lost the write does
// not pin the test goroutine forever.
func (c *Client) waitForNodeVisible(ctx context.Context, actor string, witness proto.Message, nodeID string) error {
	deadline := time.Now().Add(5 * time.Second)
	scope, err := c.scope(actor)
	if err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, err := nodeVisible(ctx, scope, witness, nodeID)
		if err == nil && ok {
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			return fmt.Errorf("entdb: visibility timeout for %s", nodeID)
		}
		if err := sleepOrCtx(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
}

func nodeVisible(ctx context.Context, scope *sdk.Scope, witness proto.Message, nodeID string) (bool, error) {
	switch witness.(type) {
	case *pb.UserNotification:
		got, err := sdk.Get[*pb.UserNotification](ctx, scope, nodeID)
		if err != nil {
			return false, err
		}
		return got != nil && got.ProtoReflect().IsValid(), nil
	case *pb.DeviceRegistration:
		got, err := sdk.Get[*pb.DeviceRegistration](ctx, scope, nodeID)
		if err != nil {
			return false, err
		}
		return got != nil && got.ProtoReflect().IsValid(), nil
	}
	return false, fmt.Errorf("entdb: nodeVisible: unsupported %T", witness)
}

// waitForPatchVisible polls until every set field on `patch` is reflected
// on the stored node — the post-Update visibility guarantee.
func (c *Client) waitForPatchVisible(ctx context.Context, actor, nodeID string, patch proto.Message) error {
	deadline := time.Now().Add(5 * time.Second)
	scope, err := c.scope(actor)
	if err != nil {
		return err
	}
	want := patch.ProtoReflect()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		got, gerr := fetchAs(ctx, scope, patch, nodeID)
		if gerr == nil && got != nil && patchApplied(want, got.ProtoReflect()) {
			return nil
		}
		if time.Now().After(deadline) {
			if gerr != nil {
				return gerr
			}
			return fmt.Errorf("entdb: patch visibility timeout for %s", nodeID)
		}
		if err := sleepOrCtx(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
}

// waitForFieldsVisible polls until every NAMED field on the stored node
// equals the corresponding field on the patch — works for zero-value
// writes that waitForPatchVisible's Range walk skips.
func (c *Client) waitForFieldsVisible(ctx context.Context, actor, nodeID string, patch proto.Message, fields []string) error {
	deadline := time.Now().Add(5 * time.Second)
	scope, err := c.scope(actor)
	if err != nil {
		return err
	}
	wantRefl := patch.ProtoReflect()
	desc := wantRefl.Descriptor()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		got, gerr := fetchAs(ctx, scope, patch, nodeID)
		matched := false
		if gerr == nil && got != nil {
			gm := got.ProtoReflect()
			matched = true
			for _, f := range fields {
				fd := desc.Fields().ByName(protoreflect.Name(f))
				if fd == nil {
					continue
				}
				if !gm.Get(fd).Equal(wantRefl.Get(fd)) {
					matched = false
					break
				}
			}
		}
		if matched {
			return nil
		}
		if time.Now().After(deadline) {
			if gerr != nil {
				return gerr
			}
			return fmt.Errorf("entdb: field visibility timeout for %s", nodeID)
		}
		if err := sleepOrCtx(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
}

func fetchAs(ctx context.Context, scope *sdk.Scope, witness proto.Message, nodeID string) (proto.Message, error) {
	switch witness.(type) {
	case *pb.UserNotification:
		return sdk.Get[*pb.UserNotification](ctx, scope, nodeID)
	case *pb.DeviceRegistration:
		return sdk.Get[*pb.DeviceRegistration](ctx, scope, nodeID)
	}
	return nil, fmt.Errorf("entdb: fetchAs: unsupported %T", witness)
}

func patchApplied(want, got protoreflect.Message) bool {
	ok := true
	want.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if !got.Has(fd) {
			ok = false
			return false
		}
		if !got.Get(fd).Equal(v) {
			ok = false
			return false
		}
		return true
	})
	return ok
}

func sleepOrCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
