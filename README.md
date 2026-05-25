# notify

Multi-channel notification platform. Deliver one notification to a user over
any channel — **in-app real-time (SSE), email, web push, mobile push, SMS,
WhatsApp** — with **pluggable providers per channel** and a **pluggable durable
store**.

Packaged two ways, like [`elloloop/identity`](https://github.com/elloloop/identity):

- **Library** — `import "github.com/elloloop/notify"` and embed it in-process.
- **Container** — `ghcr.io/elloloop/notify:<version>`, a thin server that wraps
  the library and exposes the gRPC/Connect API. Pin a version and deploy it the
  same way you deploy identity / tenant-shard-db.

## Channels & providers

A channel is a *kind* (`in_app`, `email`, `web_push`, `mobile_push`, `sms`,
`whatsapp`) backed by a *provider* chosen at config time. A channel with no
provider configured is simply disabled — its notifications are stored for
catch-up but not dispatched.

| Channel | Provider options |
|---|---|
| `email` | `emailservice` (elloloop EmailService) · `ses` · `acs` · `smtp` · `sendgrid` |
| `sms` | `twilio` · `sns` · `acs` |
| `whatsapp` | `twilio` · `meta` |
| `web_push` | `vapid` |
| `mobile_push` | `fcm` · `apns` · `azure` · `aws` |
| `in_app` | built-in real-time engine (toggle on/off) |

The **in-app live-connection subsystem is optional** (`LiveConnections.Enabled`):
when off, the service maintains no client connections and in-app notifications
are store-only.

## Storage

One `Store` interface, multiple drivers, all verified against the same
conformance suite (`store/conformance`) so they behave identically:

- `store/memory` — in-process, the differential reference for tests/dev.
- `store/entdb` — [tenant-shard-db](https://github.com/elloloop/tenant-shard-db).
- `store/postgres` — Postgres via `pgx`.

```
go test ./...                       # unit + memory conformance
```

The CI `Conformance / <driver>` matrix runs the suite against `memory`,
`postgres` (testcontainers) and `entdb`.

## Layout

```
notify/            core: domain types, Store + Provider contracts, Notifier
  realtime/        generic in-memory engine: connection registry + retry tracker
  store/           Store drivers (memory, entdb, postgres) + conformance suite
  channels/        provider implementations (email, twilio, fcm, webpush, …)
  proto/notify/    the gRPC/Connect service contract
  cmd/notifyd/     thin standalone container
```

## Status

Early scaffold. Implemented: core contracts, the `Notifier` orchestrator, the
generic realtime engine, the in-memory store and the conformance suite.
In progress: entdb + postgres stores, the channel providers, the proto contract
and the standalone server.

## License

AGPL-3.0 — see [LICENSE](./LICENSE).
