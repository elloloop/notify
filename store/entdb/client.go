// Package entdb is the tenant-shard-db (EntDB) backed implementation of
// notify.Store. The conformance suite in store/conformance verifies it
// against the in-memory reference; the per-subtest failure attribution
// lives in CONFORMANCE.md.
//
// Architecture:
//
//   - Schema is declared proto-first in proto/entdb_notify/notify.proto and
//     consumed via the generated *entdb_notify.UserNotification /
//     *entdb_notify.DeviceRegistration messages. The SDK reads type_id from
//     the proto descriptor at runtime (sdk.typeIDFromMessage), so no
//     SDK-level RegisterSchema call is required.
//   - Reads go through sdk.Get / sdk.Query / sdk.GetByKey.
//   - Writes go through sdk.Plan.Create / sdk.Plan.Update[Fields].
//   - The (TenantID, UserID, NotificationID) idempotency key and the
//     (TenantID, UserID, DeviceType) device-uniqueness key are
//     materialized into a composite_key string field that the driver hits
//     via sdk.GetByKey (the unique-index path) — EntDB has no native
//     composite-unique constraint.
package entdb

import (
	"context"
	"strings"

	sdk "github.com/elloloop/tenant-shard-db/sdk/go/entdb"
)

// systemActor is used for cross-user lookups (composite-key idempotency
// reads, device list queries spanning multiple device types). EntDB
// enforces actor-scoped row visibility — a `user:X` actor only sees rows X
// created — so notification writes use `user:<user_id>` while service-wide
// reads use `system:notify` (a tenant-admin namespace, no user-registry
// requirement).
const systemActor = "system:notify"

// Client wires an *sdk.DbClient and the tenant id together. The notify
// service is single-tenant per EntDB instance in this driver: tenants are
// represented inside notify.Store calls via TenantID on each request, but
// the underlying EntDB tenant is fixed by configuration.
type Client struct {
	client   *sdk.DbClient
	tenantID string
}

// NewClient returns a Client wrapping the SDK handle. The caller is
// responsible for sdk.Connect / sdk.Close lifecycle.
func NewClient(c *sdk.DbClient, tenantID string) *Client {
	return &Client{client: c, tenantID: tenantID}
}

func (c *Client) scope(actor string) (*sdk.Scope, error) {
	a, err := sdk.ParseActor(actor)
	if err != nil {
		return nil, err
	}
	return c.client.Tenant(c.tenantID).Actor(a), nil
}

// notifActor is the actor used for notification writes. The notify
// service treats user_id as a row value rather than an EntDB actor:
// notifications are written by the platform on behalf of recipients,
// not by the recipients themselves. Routing every write through
// `system:notify` (a tenant-admin namespace, no registry-membership
// requirement) avoids the per-user CreateUser + AddTenantMember dance
// identity has to perform and lets the conformance suite create rows
// for synthetic users like "u1" / "u2" without first registering them.
//
// The userID argument is preserved here so a future refactor (e.g.
// honouring an upstream tenant policy that requires actor-scoped writes)
// can re-route per-user without changing every call site.
func notifActor(userID string) string {
	_ = userID
	return systemActor
}

// isTenantNotOpened reports whether err is the upstream "tenant not
// opened" / sanitized "tenant not found" signal that QueryNodes returns
// against a brand-new tenant. The fresh-tenant subtests assert empty
// results in that case rather than an error — identity's helper of the
// same name documents the exact wire shape this catches across SDK
// versions.
func isTenantNotOpened(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "tenant not opened") {
		return true
	}
	if strings.Contains(msg, "tenant not found") {
		return true
	}
	// SEC-5 sanitization in v1.16+ retargets Internal errors with a
	// generic phrase; the tenant kind information leaks via the
	// "no matching node" / "no rows" wording. Be conservative: only
	// translate when we recognize the literal wording above. Anything
	// else is a real error.
	return false
}

// ctxOrBackground returns ctx unchanged if non-nil; the SDK's gRPC
// transport rejects nil contexts, so an unwary caller would deadlock.
func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
