# CLAUDE.md — elloloop/notify

Generic multi-channel notification platform. Packaged dual-mode (library + thin container) like `elloloop/identity` and `elloloop/tenant-shard-db`. Pluggable providers per channel (email · sms · whatsapp · web-push · mobile-push · in-app real-time) and pluggable durable store (memory · postgres · entdb).

## Architecture

```
notify/                        ← root package: interfaces + orchestrator (no concrete deps)
├── model.go                   domain types — Notification, Device, ChannelKind, DeliveryStatus
├── store.go                   Store interface + Query + ErrNotFound / ErrConflict
├── channel.go                 Provider interface + Message + Receipt + ProviderRegistry
├── notifier.go                Notifier orchestrator (works only against interfaces)
├── config.go                  per-channel provider configs + LiveConnections toggle
realtime/                      generic in-memory engine: Registry[T] + RetryTracker + Conn[T]
store/                         store drivers (memory · postgres · entdb) + conformance suite
   conformance/                driver-agnostic Store spec — 24 leaf subtests across 6 categories
channels/                      provider implementations (email · twilio · fcm · webpush)
proto/notify/v1/               Connect/gRPC contract for the standalone container
gen/                           generated stubs (entdb options, notify proto, …) — never hand-edit
cmd/notifyd/                   thin standalone container wiring everything together
```

The root `notify` package depends on **nothing concrete**. Stores and providers are injected at construction. The container is a thin `main.go` that builds the same pieces from configuration.

## Code-quality bar — applies to existing code and every future change

- **Solid.** Every error path has a story. Cross-driver semantics use sentinel errors (`notify.ErrNotFound`, `notify.ErrConflict`). Never swallow an error silently; never paper over an upstream gap with a service-level mutex that lies about safety in multi-replica deployments — keep the conformance signal honest (entdb canaries are the canonical example).
- **DRY.** Shared logic lives behind interfaces. The conformance suite is the canonical Store spec — drivers don't re-derive semantics or duplicate test fixtures. Adversarial test data (`RoundTrip`'s `adversarialValues`) lives in one place.
- **Testable.** Every package has a `_test.go`. Concurrency tests run under `-race`. Drivers run the shared `store/conformance.RunConformance` — never their own ad-hoc tests of the same semantics. New providers will get an equivalent `providertest` harness once a second provider lands per channel.
- **Small modules.** One responsibility per package. `realtime` is the connection engine, NOT the orchestrator. `store/postgres` is one driver, not a sub-tree. Each new channel provider gets its own subpackage under `channels/`.
- **Readable.** Doc comments explain WHY, not what. Subtle invariants (e.g. `EventCh`-is-buffered-and-never-closed in `realtime/conn.go:60`) are documented inline at the point they matter.
- **No silent breaking changes.** The `notify.Store` and `notify.Provider` interfaces are stable contracts — add methods conservatively; never remove. Proto field numbers are forever.
- **Conformance is the gate.** Any new store driver must pass `store/conformance.RunConformance` against the shared suite. Any new provider (once `providertest` lands) must pass the equivalent provider suite. CI's `Conformance / <driver>` and `Conformance / <provider>` checks are what branch protection pins.

## Hard rules

- **No domain leakage into the platform.** `notify.Notification` carries opaque `SubjectRef` + `SubjectType`. It does NOT carry `todo_id`, `message_id`, or any consumer-specific field. The platform never interprets these fields.
- **No backwards dependencies on consumers.** The library and the container never call back into any consumer. JWT validation is done against `elloloop/identity` directly (or via shared HS256 secret) — never via a gateway hop in the consumer's app.
- **Proto field numbers are stable.** Once allocated, they live forever. Add fields freely; reuse / renumber is forbidden. Same rule for `(entdb.field) = { id: N }` on EntDB schema.
- **No AI/coding-agent attribution in commit messages.** No "Co-Authored-By", "Generated with", "Claude", etc. Plain human-style messages only.
- **Branch hygiene.** Feature work happens on `feat/<scope>` branches in isolated git worktrees. `main` only moves through reviewed merges (fast-forward or `--no-ff` with conflict resolution via `go mod tidy`). Never force-push `main`.

## Patterns

### Adding a new store driver

1. Create `store/<driver>/` with the implementation.
2. Add `store/<driver>/<driver>_test.go` that runs the shared suite:
   ```go
   conformance.RunConformance(t, conformance.Driver{
       Name: "<driver>",
       New:  func(t *testing.T) notify.Store { return <driver>.New(...) },
   })
   ```
3. Wire the driver into `.github/workflows/conformance.yml`'s matrix (memory · postgres · entdb · `<driver>`).
4. Run `go test ./store/<driver>/... -race -count=1`. Any subtest that doesn't pass gets a one-line root-cause attribution in `store/<driver>/CONFORMANCE.md` — see `store/postgres/CONFORMANCE.md` and `store/entdb/CONFORMANCE.md` for the format.

### Adding a new provider for an existing channel

1. Create `channels/<channel>/<provider>/` (e.g. `channels/email/ses/` alongside `channels/email/emailservice/`).
2. Implement `notify.Provider`: `Kind()`, `Name()`, `Send(ctx, Message) (Receipt, error)`.
3. Add a `<Provider>Config` struct fragment to `notify/config.go` if the global config needs to address it.
4. Run the provider against `channels/<channel>/providertest` (when that exists).

### Adding a new channel kind

1. Add the `ChannelKind` constant in `notify/model.go`.
2. Create `channels/<channel>/` with the first provider.
3. Update `NotifyRequest.Addresses` shape if the channel needs a new destination type.
4. Add `<Channel>Config` to `notify/config.go`.
5. Wire into `cmd/notifyd` so the channel turns on when configured.

## Build & test

```bash
go build ./...
go vet ./...
go test ./... -race -count=1                    # default path (memory + non-realentdb)
go test ./store/postgres/... -race              # requires Docker (testcontainers)
NOTIFY_ENTDB_ADDRESS=localhost:50061 \
  go test -tags=realentdb ./store/entdb/... -race  # requires running entdb at that addr
```

## References

- `store/conformance/conformance.go` — driver-agnostic Store spec, top of file documents the category split.
- `store/postgres/CONFORMANCE.md` — Postgres conformance report (24/24).
- `store/entdb/CONFORMANCE.md` — EntDB conformance report (24/24 on v2.0.1 schema-aware).
- Upstream `elloloop/tenant-shard-db` issues filed during the v2 bump: #598-#602, #606-#608.
- Template repo for dual-mode (library + container) packaging: [`elloloop/identity`](https://github.com/elloloop/identity).
