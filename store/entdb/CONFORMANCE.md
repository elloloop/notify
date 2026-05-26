# notify.Store EntDB driver — conformance report

This file is the headline deliverable of the EntDB driver. The conformance
suite in `store/conformance` is the driver-agnostic source of truth for
"does this Store honour the contract memory does?". Every subtest below
runs against a live `ghcr.io/elloloop/tenant-shard-db:2.0.1` instance via
`go test -tags=realentdb ./store/entdb/...` with `NOTIFY_ENTDB_ADDRESS`
pointing at it.

## Resolved in v2.0.1 — bumped from v1.32.1

The v1.32.1 report logged **two intentionally-red canaries** for the
composite-uniqueness gap (`ConcurrentCreate_SameKey_SingleWinner`,
`ConcurrentUpsertDevice_SameKey_SingleRow`). v2.0.1 lands the upstream
fixes that close both:

- **Server-enforced composite-unique constraints** via ADR-031's
  self-describing schema mode. When the SDK is constructed with
  `sdk.WithSchema(&pb.UserNotification{}, &pb.DeviceRegistration{})`,
  the server materializes the notify schema from the NAME-FREE
  descriptor on the first ExecuteAtomic and enforces `unique: true` on
  the `composite_key` field declared in `proto/entdb_notify/notify.proto`.
  Racing creates with the same key now produce exactly one row.
- **Go SDK module path fix** (upstream PR #603, our issue #598): the
  SDK now declares `module github.com/elloloop/tenant-shard-db/sdk/go/entdb/v2`,
  so the import path bumps from `.../sdk/go/entdb` to
  `.../sdk/go/entdb/v2` and `go.mod` resolves cleanly without the
  per-build vendoring hack.

Both canaries flipped from FAIL → PASS in the same change. See the
**Concurrency** table below.

## Run conditions

- Image: `ghcr.io/elloloop/tenant-shard-db:2.0.1`
  (tags `2.0.1`, `2`, `2.0`, `latest` all point at the same digest;
  upstream did not publish a leading-`v` tag for this release.)
- SDK: `github.com/elloloop/tenant-shard-db/sdk/go/entdb/v2 v2.0.1`
- Schema-aware mode: ENABLED via `sdk.WithSchema(entdbstore.SchemaMessages()...)`
- Server flags: `-addr=:50051 -data-dir=/tmp/entdb -wal-backend=memory`
  (`-data-dir` is now a required flag — v1.32.1 defaulted it).
- Race detector ON (`-race`), single iteration (`-count=1`).
- Each subtest gets its own freshly-registered tenant (process-unique
  base + atomic counter), so state does not leak between subtests.
- A serialized **schema-fingerprint warm-up create** runs once before
  any concurrent subtest — workaround for an upstream SDK data race
  on `grpcTransport.serverFingerprint` (see "Notes on v2.0.1" below).

## Results — 24 PASS / 0 FAIL / 0 SKIP

### Core CRUD — all PASS

| Subtest | Status | Notes |
|---|---|---|
| `entdb/CreateGet` | PASS | round-trips through Plan.Create + sdk.Get |
| `entdb/GetNotFound` | PASS | nil typed return maps to `notify.ErrNotFound` |
| `entdb/Idempotency` | PASS | composite_key lookup via sdk.GetByKey hits the second create |
| `entdb/StatusTransitions` | PASS | UpdateFields names every field so zero-value timestamps still write |
| `entdb/CursorWalk_ThreePages` | PASS | strict `<` cursor honoured; deterministic id-tiebreak |
| `entdb/UnreadFilterAndCount` | PASS | unread count computed independently of page window |
| `entdb/DeviceUpsertRotation` | PASS | composite_key keyed UpsertDevice rotates token in place |
| `entdb/UserIsolation` | PASS | (tenant,user) filter in QueryNodes + post-filter guard |

### Pagination — all PASS

| Subtest | Status | Notes |
|---|---|---|
| `entdb/Pagination/QueryUserNotifications_AllPagesReturnEveryRow` | PASS | 250 rows surfaced via SDK keyset auto-follow (ADR-029) — `limit=0` returns the complete set |
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
| `entdb/RoundTrip/Int64_Fidelity_Timestamps` | PASS | the SDK's `EntValue` wire path (ADR-028) carries int64 losslessly — `max_int64`, `2^53+1` all round-trip exactly |
| `entdb/RoundTrip/LargePayload_Body` | PASS | 64 KiB and 512 KiB bodies round-trip unmodified |

### Concurrency — all PASS (was 3/5 on v1.32.1)

| Subtest | Status | Notes |
|---|---|---|
| `entdb/Concurrency/ConcurrentCreate_DistinctKeys_NoLostWrites` | PASS | 16 distinct keys land 16 rows; WAL applier serializes correctly |
| `entdb/Concurrency/ConcurrentCreate_SameKey_SingleWinner` | PASS | **flipped from FAIL** — v2 server's schema-enforced `composite_key` unique index serializes the race; loser-fallback resolves canonical id via `sdk.GetByKey` |
| `entdb/Concurrency/ConcurrentUpsertDevice_SameKey_SingleRow` | PASS | **flipped from FAIL** — same mechanism; the loser path then runs a no-wait UpdateFields against the canonical id (the race-loser branch accepts eventual consistency on the token / last_active_ms fields, mirroring the UpdateStatus pattern) |
| `entdb/Concurrency/ConcurrentUpdateStatus_NoError` | PASS | UpdateStatus skips the post-commit visibility wait under concurrent racers; the write itself does not error |
| `entdb/Concurrency/ConcurrentReadYourWrites_QueryAfterCreate` | PASS | `commitCreate`'s `waitForNodeVisible` guarantees the writer's own row is visible to its very next query |

### KeyEdge — all PASS

| Subtest | Status | Notes |
|---|---|---|
| `entdb/KeyEdge/NotificationID_LongValue` | PASS | 256-char id round-trips and re-create is idempotent |
| `entdb/KeyEdge/NotificationID_SeparatorBytesDoNotCollide` | PASS | `lpEncode` length-prefixes every composite-key part so "u1|n1\|n2" and "u1\|n1|n2" map to distinct keys |
| `entdb/KeyEdge/DeviceType_CaseSensitive_SeparateRows` | PASS | EntDB string equality is byte-exact; "android" and "Android" are distinct composite keys |

## How the two canaries got fixed

The v1.32.1 driver implemented same-key idempotency as a
**query-then-create**: `findByKey → empty? → commitCreate`. That sequence
is not atomic — two goroutines could both observe the empty index, both
insert, and the server (running schemaless) would accept both rows.

The v2 driver collapses this to a **commit-then-reconcile** path. With
the schema registered via `sdk.WithSchema`, the server enforces
`unique: true` on `composite_key`; only one of N concurrent inserts
actually lands in the WAL. The Go SDK does not surface the rejection as
a typed error to the loser (it would need `wait_applied=true` on
`Plan.Commit`, which the Go SDK does not expose — see "Notes on v2.0.1"),
so the driver reconciles in code:

```go
// commitCreateUnique helpers.go
submittedID, _ := commitCreateNoWait(ctx, actor, msg)      // server pre-allocates a UUID
canonical, _  := findByKeyWithRetry(ctx, actor, key, val)  // bounded sdk.GetByKey poll
won := canonical == submittedID                            // winner: ids match; loser: differ
```

`CreateNotification` returns `(canonicalID, won)` so the caller learns
whether it was the writer. `UpsertDevice` additionally runs a no-wait
`UpdateFields` against the canonical row in the loser branch so every
racer's token / last_active_ms gets a chance to land (memory's "last
writer wins" semantics are preserved).

The pre-emptive `findByKey` short-circuit is retained as a warm-path
optimization for the "create the same notification twice in sequence"
case — it avoids the extra Commit round-trip when the row already
exists. Race-safety, however, comes from the **post-commit reconcile**,
not the pre-check.

## Notes on v2.0.1 — items worth a follow-up upstream issue

### #U1 — Go SDK `Plan.Commit` does not expose `wait_applied`

The Python SDK has `await plan.commit(wait_applied=True, timeout=...)`
(`sdk/python/entdb_sdk/scope.py:117`). The Go SDK auto-enables
`wait_applied` only when an op carries a `Precondition`; a plain
`Plan.Create()` never sets it. The consequence: the loser of a
unique-constraint race receives a `success=true` receipt with a
pre-allocated node id that never actually lands, instead of the
ALREADY_EXISTS gRPC status the server's
`server/go/internal/api/composite_unique_test.go` reference test sets
up with `WaitApplied: true`.

The driver works around this by resolving the canonical row via
`sdk.GetByKey` (see `commitCreateUnique`), but the upstream gap is
real and worth filing — Python and Go SDKs should expose the same
write-and-wait primitive. Suggested API:
`func (p *Plan) Commit(ctx, opts ...CommitOption)` with `WithWaitApplied()`.

### #U2 — Race detector trips on `grpcTransport.serverFingerprint`

`sdk/go/entdb/v2/client.go:560` writes
`t.serverFingerprint = clientFP` from inside `ExecuteAtomic` without
any synchronization. Multiple goroutines hitting `ExecuteAtomic`
concurrently against a server that has NOT yet confirmed the schema
fingerprint all take this branch and trip Go's race detector. The
logical outcome is fine — every goroutine writes the same value — but
the test fails under `-race` until the fingerprint is established.

The driver works around this by issuing a single serialized warm-up
`CreateNotification` at TestConformance setup (see
`realentdb_conformance_test.go`). Upstream should wrap the field in a
mutex or `atomic.Pointer[string]`.

### #U3 — Server-validated kind names no longer accept `string`

v1.32.1's schemaless path silently accepted `(entdb.field).kind = "string"`
because no validation ran. v2's schema-aware path rejects it with
`invalid field kind "string" for field_id 1` — the server only accepts
the wire-canonical names declared in `server/go/internal/schema/types.go`
(`str`, `int`, `float`, `bool`, `timestamp`, `json`, `bytes`, `enum`,
`ref`, `list_*`). `proto/entdb_notify/notify.proto` dropped the
`kind: "string"` overrides; the SDK derives `str` from the proto type
by default. The kind for `int64` epoch-millis fields stays
`kind: "timestamp"`.

### #U4 — Server image tag convention

Upstream publishes `2.0.1`, `2.0`, `2`, `latest` but NOT `v2.0.1`. The
task spec assumed the leading-`v`; this report uses `2.0.1` for
explicit version anchoring (the bare `2.0.1` tag points at the same
sha256 digest as `latest`, so consumers can pin either).

## What the suite does NOT cover

These are intentional omissions and not driver gaps:

- **Cross-tenant ACL.** The conformance suite uses a fresh tenant per
  subtest, so it never observes cross-tenant data. The driver hard-codes
  `system:notify` as the actor for every read and write; cross-tenant
  isolation is enforced by the `tenantID` argument the driver passes to
  the SDK transport.
- **Long-lived connections.** SDK client is reused across subtests;
  reconnection behaviour is tested upstream.
- **Server crash / WAL replay.** The driver assumes the server is
  available; failover is the operator's concern.

## Reproducing locally

```bash
# Boot a local EntDB on a free port. -data-dir is required on v2.0.1.
docker run -d --rm --name notify-entdb-test -p 50061:50051 \
  ghcr.io/elloloop/tenant-shard-db:2.0.1 \
  -addr=:50051 -data-dir=/tmp/entdb -wal-backend=memory

# Run the suite.
NOTIFY_ENTDB_ADDRESS=localhost:50061 \
  go test -tags=realentdb ./store/entdb/... -race -count=1 -v -timeout=5m

# Teardown.
docker stop notify-entdb-test
```

Expected: exit code 0 with all 24 leaf subtests PASS.

## File-by-file map

- `proto/entdb_notify/notify.proto` — proto-first schema for
  `UserNotification` and `DeviceRegistration` node types (type_ids 1
  and 2). `kind:` overrides only on the `timestamp` int64 fields;
  `composite_key` carries `unique: true` so v2's schema-aware server
  enforces the constraint.
- `gen/go/entdb_notify/notify.pb.go` — generated Go stubs; `buf generate`.
- `store/entdb/client.go` — `Client` (wraps `*sdk.DbClient` + tenant id),
  `SchemaMessages()` helper for `sdk.WithSchema` registration, actor /
  tenant-not-opened helpers.
- `store/entdb/helpers.go` — composite-key encoding (`lpEncode`),
  per-method commit + visibility-wait helpers, raw-transport filter
  shim, **`commitCreateUnique`** (the post-commit unique-key
  reconcile that closes the same-key race against v2's
  server-enforced unique constraint).
- `store/entdb/store.go` — `notify.Store` implementation.
- `store/entdb/realentdb_conformance_test.go` — `//go:build realentdb`
  entrypoint that boots an SDK client with `sdk.WithSchema(...)`,
  issues a warm-up create to establish the schema fingerprint, then
  registers a fresh tenant per subtest and invokes
  `conformance.RunConformance`.
- `store/entdb/skip_test.go` — `//go:build !realentdb` placeholder that
  prints a SKIP line so the default `go test ./...` run is green.
