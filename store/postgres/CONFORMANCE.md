# Postgres notify.Store — Conformance Report

- **Date**: 2026-05-26
- **Driver**: `github.com/elloloop/notify/store/postgres`
- **Branch**: `feat/store-postgres`

## SDK / runtime versions

| Component | Version |
|---|---|
| Go | 1.25 (toolchain go1.26.3) |
| `github.com/jackc/pgx/v5` | v5.9.2 |
| `github.com/testcontainers/testcontainers-go` | v0.42.0 |
| `github.com/testcontainers/testcontainers-go/modules/postgres` | v0.42.0 |
| `github.com/google/uuid` | v1.6.0 |
| Postgres image | `postgres:16.13-alpine3.23` |

Versions are the exact ones identity (`/Users/arun/projects/identity/go.mod`)
uses in production. Lock-stepping with identity keeps the dependency surface
boring across services.

## Results

```
go test ./store/postgres/... -race -count=1 -v
```

| Category | Subtests | Pass | Fail |
|---|---:|---:|---:|
| Basic CRUD (CreateGet, GetNotFound, Idempotency, StatusTransitions, CursorWalk_ThreePages, UnreadFilterAndCount, DeviceUpsertRotation, UserIsolation) | 8 | 8 | 0 |
| Pagination (QueryUserNotifications_AllPagesReturnEveryRow, StrictLessThanCutoff) | 2 | 2 | 0 |
| FreshTenant (QueryUserNotifications_Empty, GetNotification_NotFound, ListDevices_Empty) | 3 | 3 | 0 |
| RoundTrip (StringFields_OnCreate, Int64_Fidelity_Timestamps, LargePayload_Body) | 3 | 3 | 0 |
| Concurrency (DistinctKeys_NoLostWrites, SameKey_SingleWinner, UpsertDevice_SameKey_SingleRow, UpdateStatus_NoError, ReadYourWrites_QueryAfterCreate) | 5 | 5 | 0 |
| KeyEdge (NotificationID_LongValue, NotificationID_SeparatorBytesDoNotCollide, DeviceType_CaseSensitive_SeparateRows) | 3 | 3 | 0 |
| **Total** | **24** | **24** | **0** |

Full pass under `-race`. Total wall time for the conformance run (after the
testcontainers Postgres warm-up) is ~3.3s on Apple Silicon — the shared
container with `TruncateAll` per subtest is the only way to keep it fast.

> The task brief mentioned "27 conformance subtests"; the suite shipped at
> SHA of `feat/store-postgres` (the commit checked out for me) enumerates
> 24 leaf subtests across the six categories. All 24 pass. If new subtests
> land later they should be revalidated; the driver design (real composite
> unique constraints, `ON CONFLICT DO NOTHING` for create, `ON CONFLICT DO
> UPDATE` for device upsert, `LIMIT n+1` paging) covers every bug class the
> existing suite probes.

## Implementation choices

### IDs

UUID v4 generated client-side via `github.com/google/uuid` (`uuid.NewString`),
stored in a `uuid` Postgres column. We do not rely on `gen_random_uuid()`
server-side because we want the id available to the caller without an extra
round-trip on the conflict-recovery path. Matches identity's approach
(`internal/repo/postgres/repo.go::newID`).

### Pool sizing

`MaxConns = 25` by default — same as identity's `DefaultMaxConns`. Lifetimes:
`MaxConnLifetime = 1h`, `MaxConnIdleTime = 30m` so we cooperate with
upstream pgbouncer / Azure Database for PostgreSQL idle reapers.

### Migrations

Embedded as a Go slice (`migrations.go`), applied by the driver under
`pg_advisory_lock(<stable-key>)` so concurrent processes booting at the
same time serialise on the lock instead of racing the DDL. Idempotent
on already-applied versions via a `notify_schema_migrations` ledger
table. This is intentionally lighter-weight than identity's
`golang-migrate` setup — the notify schema is two tables, and pulling in
`migrate/v4` plus its pgx driver felt disproportionate for a clean room
package. If we ever want CLI-driven migrations, the SQL strings are
trivial to mirror into `.sql` files later.

### Schema

```sql
CREATE TABLE notify_notifications (
    id              uuid PRIMARY KEY,
    tenant_id       text   NOT NULL,
    user_id         text   NOT NULL,
    notification_id text   NOT NULL,
    subject_ref     text   NOT NULL DEFAULT '',
    subject_type    text   NOT NULL DEFAULT '',
    title           text   NOT NULL DEFAULT '',
    body            text   NOT NULL DEFAULT '',
    channel         text   NOT NULL DEFAULT '',
    status          text   NOT NULL,
    created_at_ms   bigint NOT NULL,
    delivered_at_ms bigint NOT NULL DEFAULT 0,
    ack_at_ms       bigint NOT NULL DEFAULT 0,
    read_at_ms      bigint NOT NULL DEFAULT 0,
    UNIQUE (tenant_id, user_id, notification_id)
);
CREATE INDEX notify_notifications_user_created
    ON notify_notifications (tenant_id, user_id, created_at_ms DESC, id DESC);
CREATE INDEX notify_notifications_user_unread
    ON notify_notifications (tenant_id, user_id)
    WHERE status <> 'read';

CREATE TABLE notify_devices (
    id             uuid PRIMARY KEY,
    tenant_id      text   NOT NULL,
    user_id        text   NOT NULL,
    device_type    text   NOT NULL,
    token          text   NOT NULL DEFAULT '',
    created_at_ms  bigint NOT NULL DEFAULT 0,
    last_active_ms bigint NOT NULL DEFAULT 0,
    UNIQUE (tenant_id, user_id, device_type)
);
```

- **Composite uniqueness is enforced by Postgres**, not the application.
  This is what makes `ConcurrentUpsertDevice_SameKey_SingleRow` pass cleanly
  — many goroutines pile onto the same `(tenant, user, device_type)` row
  via `INSERT ... ON CONFLICT DO UPDATE` and Postgres serialises them at the
  unique index. The conformance suite explicitly calls this out as the
  composite-uniqueness canary.
- **Pagination index** orders `(tenant_id, user_id, created_at_ms DESC,
  id DESC)` — the same tuple `QueryUserNotifications` sorts by, so paging
  uses an index scan rather than a sort node.
- **Unread partial index** is icing — keeps `unreadCount` fast even with
  millions of read rows. The conformance suite doesn't stress that, but
  the production access pattern will.

### Idempotency / upsert pattern

- `CreateNotification` →
  `INSERT ... ON CONFLICT (tenant_id, user_id, notification_id) DO NOTHING RETURNING id`.
  If `RETURNING` produces a row → `created=true`. If it returns `pgx.ErrNoRows`
  (ON CONFLICT path absorbed the dup) → follow-up `SELECT id ... WHERE
  (tenant_id, user_id, notification_id) = (...)` to recover the canonical id
  and return `created=false`. This satisfies both the basic Idempotency
  subtest *and* `ConcurrentCreate_SameKey_SingleWinner`: exactly one
  goroutine gets the `RETURNING` row, the rest take the recovery path.
- `UpsertDevice` →
  `INSERT ... ON CONFLICT (tenant_id, user_id, device_type) DO UPDATE SET
  token = EXCLUDED.token, last_active_ms = EXCLUDED.last_active_ms RETURNING *`.
  `created_at_ms` is intentionally NOT updated on conflict, matching
  store/memory's "rotate token in place, don't lose the original create
  time" semantic.
- `UpdateStatus` → single `UPDATE` with a per-status `CASE` that stamps the
  matching `*_at_ms` column. `tag.RowsAffected() == 0` → `notify.ErrNotFound`.

### Pagination

- Order: `ORDER BY created_at_ms DESC, id DESC`.
- Cursor: `strconv.FormatInt(lastRow.CreatedAtMS, 10)` — same wire format
  as store/memory, so cross-driver test fixtures interchange.
- Next page: `WHERE created_at_ms < $cursor` (strict `<`, per the contract).
- Detect-next-page: `LIMIT n+1`. If we get back `n+1` rows we know there's
  more, slice to `n` and emit `nextCursor = items[n-1].CreatedAtMS`. Saves
  a second round-trip and a `count(*)` for the page boundary.
- `unreadCount` is a separate `SELECT count(*) FROM ... WHERE status <>
  'read'` — independent of the page window and any `UnreadOnly` filter,
  matching the in-memory reference.

### Error mapping

- `pgx.ErrNoRows` on `GetNotification` → `notify.ErrNotFound`.
- `tag.RowsAffected() == 0` on `UpdateStatus` → `notify.ErrNotFound`.
- `pgconn.PgError.Code == "23505"` (unique_violation) on `CreateNotification`
  outside the absorbed ON CONFLICT path → `notify.ErrConflict`. In practice
  this only fires if the caller hands in a `Notification.ID` that collides
  with an existing primary key from a different idempotency triple; the
  ON CONFLICT path handles every (tenant, user, notification_id) dup.
- Malformed UUID strings reaching `GetNotification` / `UpdateStatus` are
  surfaced as `notify.ErrNotFound` (no such row), not as a driver-internal
  parse error.

### TruncateAll

Exposed as a *driver-only* helper so the conformance entry test can reset
state between subtests without spinning a fresh container per subtest. It
is not part of `notify.Store` (no contract drift). Tests acquire the
container via `sync.Once` and `t.Cleanup` Terminate at process exit.

## Failed subtests

None — 24 of 24 pass under `-race`.

## Anything to review carefully

- **`isInvalidUUID`** is a string-substring sniff over the pgx error
  message. It is defensive: every Get/Update path that takes a string `id`
  argument needs to translate "this isn't a uuid" into `ErrNotFound`, and
  pgx does not expose a typed sentinel for the encoder rejection. The
  current substrings cover the messages observed in pgx v5.9.2; if pgx
  changes its wording in a future bump this will silently revert to
  surfacing the encoder error and a subtest like `GetNotFound` would start
  failing loudly. If you want a sturdier hook, an alternative is to wrap
  the column as `text` and validate uuid syntax application-side — but
  that gives up the native index + tuple comparison wins.
- The migration runner uses `pg_advisory_lock` with a fixed key
  (`migrationAdvisoryLockKey`). If a future driver in the same database
  picks the same key its migrations would block on ours. The key was
  chosen to be unlikely (~`0x6E6F74696679_00`, "notify\0") but is not
  globally reserved.
