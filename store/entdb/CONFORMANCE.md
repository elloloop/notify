# notify.Store EntDB driver — conformance report

This file is the headline deliverable of the EntDB driver. The conformance
suite in `store/conformance` is the driver-agnostic source of truth for
"does this Store honour the contract memory does?". Every subtest below
runs against a live `ghcr.io/elloloop/tenant-shard-db:1.32.1` instance via
`go test -tags=realentdb ./store/entdb/...` with `NOTIFY_ENTDB_ADDRESS`
pointing at it.

## Run conditions

- Image: `ghcr.io/elloloop/tenant-shard-db:1.32.1`
- SDK: `github.com/elloloop/tenant-shard-db/sdk/go/entdb v1.32.1`
- Server flags: `-wal-backend=memory` (the persistent and memory WAL
  exercise the same WAL applier; using memory keeps the test loop fast).
- Race detector ON (`-race`), single iteration (`-count=1`).
- Each subtest gets its own freshly-registered tenant (process-unique base
  + atomic counter), so state does not leak between subtests.

## Results — 25 PASS / 2 FAIL / 0 SKIP

The two failures are the **expected canaries** the task spec called out
upstream — they are the isolated reproductions of the tenant-shard-db
"no composite-unique constraint" gap. The driver applies the same
query-then-create idempotency pattern identity uses, which is racy by
construction (cf. identity's `ConcurrentDuplicate_OAuthIdentity_SingleRow`).
Keeping these red is the tracking signal — they unblock when upstream
lands composite-unique support.

### Core CRUD — all PASS

| Subtest | Status | Notes |
|---|---|---|
| `entdb/CreateGet` | PASS | round-trips through Plan.Create + sdk.Get |
| `entdb/GetNotFound` | PASS | `errNotFound` (nil typed return) maps to `notify.ErrNotFound` |
| `entdb/Idempotency` | PASS | composite_key lookup via sdk.GetByKey hits the second create |
| `entdb/StatusTransitions` | PASS | UpdateFields names every field so zero-value timestamps still write |
| `entdb/CursorWalk_ThreePages` | PASS | strict `<` cursor honoured; deterministic id-tiebreak |
| `entdb/UnreadFilterAndCount` | PASS | unread count computed independently of page window |
| `entdb/DeviceUpsertRotation` | PASS | composite_key keyed UpsertDevice rotates token in place |
| `entdb/UserIsolation` | PASS | (tenant,user) filter in QueryNodes + post-filter guard |

### Pagination — all PASS

| Subtest | Status | Notes |
|---|---|---|
| `entdb/Pagination/QueryUserNotifications_AllPagesReturnEveryRow` | PASS | 250 rows surfaced via SDK v1.24+ keyset auto-follow (ADR-029) — `limit=0` returns the complete set |
| `entdb/Pagination/StrictLessThanCutoff` | PASS | post-filter `created_at_ms < cursor` |

### FreshTenant — all PASS

| Subtest | Status | Notes |
|---|---|---|
| `entdb/FreshTenant/QueryUserNotifications_Empty` | PASS | `isTenantNotOpened` translates the FailedPrecondition signal into an empty result |
| `entdb/FreshTenant/GetNotification_NotFound` | PASS | sdk.Get returns nil for an unknown id; mapped to `notify.ErrNotFound` |
| `entdb/FreshTenant/ListDevices_Empty` | PASS | same path as QueryUserNotifications |

### RoundTrip — all PASS

| Subtest | Status | Notes |
|---|---|---|
| `entdb/RoundTrip/StringFields_OnCreate` | PASS | every adversarial value (emoji / unicode / SQL-shaped / 10k chars / leading-trailing space) survives the Plan.Create → sdk.Get round-trip byte-for-byte |
| `entdb/RoundTrip/Int64_Fidelity_Timestamps` | PASS | the SDK v1.24+ `EntValue` wire path (ADR-028 / #572) carries int64 losslessly — `max_int64`, `2^53+1` all round-trip exactly. The structpb-coercion bug class is closed on the SDK we mirror. |
| `entdb/RoundTrip/LargePayload_Body` | PASS | 64 KiB and 512 KiB bodies round-trip unmodified |

### Concurrency — 3 PASS / 2 FAIL (expected canaries)

| Subtest | Status | Notes |
|---|---|---|
| `entdb/Concurrency/ConcurrentCreate_DistinctKeys_NoLostWrites` | PASS | 16 distinct keys land 16 rows; WAL applier serializes correctly |
| `entdb/Concurrency/ConcurrentCreate_SameKey_SingleWinner` | **FAIL** | composite-uniqueness canary — see #C1 below |
| `entdb/Concurrency/ConcurrentUpsertDevice_SameKey_SingleRow` | **FAIL** | composite-uniqueness canary — see #C2 below |
| `entdb/Concurrency/ConcurrentUpdateStatus_NoError` | PASS | UpdateStatus skips the post-commit visibility wait under concurrent racers; the write itself does not error |
| `entdb/Concurrency/ConcurrentReadYourWrites_QueryAfterCreate` | PASS | `commitCreate`'s `waitForNodeVisible` guarantees the writer's own row is visible to its very next query |

### KeyEdge — all PASS

| Subtest | Status | Notes |
|---|---|---|
| `entdb/KeyEdge/NotificationID_LongValue` | PASS | 256-char id round-trips and re-create is idempotent |
| `entdb/KeyEdge/NotificationID_SeparatorBytesDoNotCollide` | PASS | `lpEncode` length-prefixes every composite-key part so "u1|n1\|n2" and "u1\|n1|n2" map to distinct keys |
| `entdb/KeyEdge/DeviceType_CaseSensitive_SeparateRows` | PASS | EntDB string equality is byte-exact; "android" and "Android" are distinct composite keys |

## Root-cause table for the FAIL subtests

### #C1 — `Concurrency/ConcurrentCreate_SameKey_SingleWinner`

**Symptom.** 16 goroutines call `CreateNotification` with the same
`(TenantID, UserID, NotificationID)`. All 16 return `created=true` with
16 distinct node ids, and the post-race query returns 16 rows. Memory
returns 1 winner + 15 losers, 1 row.

**Root cause — upstream tenant-shard-db.** EntDB has no native
composite-unique constraint, and the SDK does not expose an "insert if
not exists" / "create or get" primitive that closes the race at the wire
level. The driver therefore implements idempotency as a
query-then-create:

```go
existingID, _ := s.c.findNotificationByKey(ctx, actor, key)
if existingID != "" { return false }      // hit the unique-key index
return s.c.commitCreate(ctx, actor, msg)  // miss → insert
```

That sequence is NOT atomic: two goroutines both observe the empty index
between the read and the write, and both inserts succeed. The SDK has a
`*UniqueConstraintError` type the driver's loser-fallback branch is
ready to handle, but the server never raises it because the
`composite_key` field's unique-index check is not enforced for
schemaless writes (the unique annotation lives in the proto but the
server runs without a registered schema).

**Plan.** **Accept as intentionally-red canary.** Identity has the same
class of failure documented for `ConcurrentDuplicate_OAuthIdentity_SingleRow`.
The upstream fix is one of:

1. tenant-shard-db lands a server-enforced composite-unique constraint
   that survives schemaless mode (preferred).
2. tenant-shard-db lands an `INSERT … ON CONFLICT DO NOTHING` Plan
   primitive that returns the canonical id when a colliding row
   already exists.

Until either lands, do NOT add a service-level mutex — it would only
serialize WITHIN one notify-service replica, and a real deployment runs
multiple replicas. The honest signal is the failing subtest.

### #C2 — `Concurrency/ConcurrentUpsertDevice_SameKey_SingleRow`

**Symptom.** 16 goroutines call `UpsertDevice` with the same
`(TenantID, UserID, DeviceType)`. Memory ends with 1 row (token rotated
to whichever writer landed last). EntDB ends with 16 rows.

**Root cause — same as #C1.** UpsertDevice's "is there a row at this
composite key?" → "insert OR update" pivot is racy for the same reason:
the read-then-write window lets every concurrent caller take the
"insert" branch. The fallback `commitCreate → UniqueConstraintError →
findDeviceByKey → commitUpdateFields` is wired in the code, but the
server never raises the constraint error in the current schemaless
configuration.

**Plan.** Same as #C1 — keep red until upstream composite-unique
support lands.

## What the suite does NOT cover

These are intentional omissions and not driver gaps:

- **Cross-tenant ACL.** The conformance suite uses a fresh tenant per
  subtest, so it never observes cross-tenant data. The driver hard-codes
  `system:notify` as the actor for every read and write; cross-tenant
  isolation is enforced by the `tenantID` argument the driver passes to
  the SDK transport.
- **Long-lived connections.** SDK client is reused across subtests;
  reconnection behaviour is tested upstream.
- **Schema registration.** The SDK reads `(entdb.node).type_id` from the
  proto descriptor at runtime via `sdk.typeIDFromMessage` — there is no
  explicit `RegisterSchema` call. The notify service runs EntDB
  schemaless and resolves `QueryNodes` filters by numeric field id (the
  same escape hatch identity uses).

## Reproducing locally

```bash
# Boot a local EntDB on a free port.
docker run -d --rm --name notify-entdb-test -p 50061:50051 \
  ghcr.io/elloloop/tenant-shard-db:1.32.1 \
  -addr=:50051 -data-dir=/var/lib/entdb -wal-backend=memory

# Run the suite.
NOTIFY_ENTDB_ADDRESS=localhost:50061 \
  go test -tags=realentdb ./store/entdb/... -race -count=1 -v -timeout=5m

# Teardown.
docker stop notify-entdb-test
```

Expected: exit code 1, with exactly the two FAIL subtests listed above
and 25 PASS subtests. A green run on this image-and-SDK combination means
either the upstream composite-uniqueness fix has landed (great — update
this file and remove the canary callout) or somebody silenced the
failing subtests (not great — re-introduce them).

## File-by-file map

- `proto/entdb_notify/notify.proto` — proto-first schema for
  `UserNotification` and `DeviceRegistration` node types (type_ids 1
  and 2). Mirrors identity's `proto/identity/schema/schema.proto`
  pattern.
- `gen/go/entdb_notify/notify.pb.go` — generated Go stubs; `buf generate`.
- `store/entdb/client.go` — `Client` (wraps `*sdk.DbClient` + tenant id),
  actor / tenant-not-opened helpers.
- `store/entdb/helpers.go` — composite-key encoding (`lpEncode`),
  per-method commit + visibility-wait helpers, raw-transport filter
  shim. SDK `UniqueKey` tokens for `composite_key` are constructed
  inline (the protoc-gen-entdb-keys codegen is not wired into this repo;
  the inline tokens are wire-equivalent).
- `store/entdb/store.go` — `notify.Store` implementation.
- `store/entdb/realentdb_conformance_test.go` — `//go:build realentdb`
  entrypoint that boots an SDK client, registers a fresh tenant per
  subtest, and invokes `conformance.RunConformance`.
- `store/entdb/skip_test.go` — `//go:build !realentdb` placeholder that
  prints a SKIP line so the default `go test ./...` run is green.
