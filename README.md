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
notify/                core: domain types, Store + Provider contracts, Notifier
  realtime/            generic in-memory engine: connection registry + retry tracker
  store/               Store drivers (memory, entdb, postgres) + conformance suite
  channels/            provider implementations (email, twilio, fcm, webpush, …)
  proto/notify/        the gRPC/Connect service contract
  internal/server/     standalone-container wiring (handlers, auth, observability)
  cmd/notifyd/         thin standalone container entry point
```

## Status

| Component | State |
|---|---|
| Core contracts (`Store`, `Provider`, `Notifier`) | Stable |
| Realtime engine (`Registry`, `RetryTracker`) | Stable |
| Store drivers (`memory`, `postgres`, `entdb`) | All conformance-green |
| Provider implementations (`emailservice`, `twilio`, `fcm`, `webpush`) | Stable, hand-rolled where possible |
| Proto contract (`proto/notify/v1`) | Stable; field numbers frozen forever |
| Standalone container (`cmd/notifyd`) | Implemented (wave-2) |
| Container image (`ghcr.io/elloloop/notify:<version>`) | Published by `release.yml` on tag push |
| Conformance CI matrix | `memory`, `postgres`, `entdb` jobs gate every PR |

## Deploy

The container is a multi-arch (`linux/amd64`, `linux/arm64`) `FROM scratch`
image. It listens on three ports and validates JWTs locally (HS256 against
a shared secret) — no callback into any consumer.

```bash
docker pull ghcr.io/elloloop/notify:latest

docker run --rm \
  -p 8080:8080 -p 8081:8081 -p 9090:9090 \
  -e NOTIFY_STORE_DRIVER=memory \
  -e NOTIFY_AUTH_JWT_SECRET=$(openssl rand -hex 32) \
  -e NOTIFY_INTERNAL_TOKEN=$(openssl rand -hex 32) \
  -e NOTIFY_EMAIL_PROVIDER=none \
  ghcr.io/elloloop/notify:latest
```

- `8080` — `NotificationClientService` (browser / mobile, Connect HTTP/2)
- `8081` — `NotificationInternalService` (backend producers, gRPC)
- `9090` — `/healthz` and `/metrics` (Prometheus exposition)

Verify the container is healthy:

```bash
curl http://localhost:9090/healthz
# {"status":"ok"}
```

### Local development

Set `NOTIFY_AUTH_DEV_MODE=true` and skip the secrets — the dev validator
accepts `Authorization: Bearer dev:<userid>:<tenant>` and the internal-token
check is bypassed. Never enable dev mode in production.

### Configuration reference

| Env var | Type | Default | Description |
|---|---|---|---|
| `NOTIFY_CLIENT_PORT` | int | `8080` | Public Connect/HTTP/2 listener (`NotificationClientService`). |
| `NOTIFY_INTERNAL_PORT` | int | `8081` | Private gRPC listener (`NotificationInternalService`). |
| `NOTIFY_METRICS_PORT` | int | `9090` | `/healthz` + `/metrics` listener. |
| `NOTIFY_LOG_LEVEL` | enum | `info` | `debug` · `info` · `warn` · `error`. |
| `NOTIFY_SHUTDOWN_TIMEOUT` | duration | `30s` | Graceful-shutdown deadline. |
| `NOTIFY_STORE_DRIVER` | enum | `memory` | `memory` · `postgres` · `entdb`. |
| `NOTIFY_POSTGRES_DSN` | string | — | Required when driver=postgres. libpq-style URL. |
| `NOTIFY_POSTGRES_AUTOMIGRATE` | bool | `true` | Apply pending schema migrations on connect. |
| `NOTIFY_ENTDB_ADDRESS` | string | — | Required when driver=entdb. `host:port`. |
| `NOTIFY_ENTDB_TENANT_ID` | string | — | Required when driver=entdb. EntDB tenant id. |
| `NOTIFY_AUTH_JWT_SECRET` | string | — | HS256 verification key. Required unless dev mode. |
| `NOTIFY_AUTH_JWT_ISSUER` | string | — | Pinned `iss` claim, if set. |
| `NOTIFY_AUTH_JWT_AUDIENCE` | string | — | Pinned `aud` claim, if set. |
| `NOTIFY_AUTH_JWT_LEEWAY` | duration | `30s` | Allowed clock skew when validating `exp` / `nbf`. |
| `NOTIFY_INTERNAL_TOKEN` | string | — | Shared secret for `X-Notify-Internal-Token` header. Required unless dev mode. |
| `NOTIFY_AUTH_DEV_MODE` | bool | `false` | Accepts `Bearer dev:<uid>:<tenant>`. Local-dev only. |
| `NOTIFY_ALLOWED_ORIGINS` | csv | — | Comma-separated CORS origins. |
| `NOTIFY_LIVE_CONNECTIONS_ENABLED` | bool | `true` | When false, `StreamEvents` returns Unimplemented. |
| `NOTIFY_LIVE_HEARTBEAT_INTERVAL` | duration | `30s` | Per-connection heartbeat cadence. |
| `NOTIFY_LIVE_RETRY_MAX_ATTEMPTS` | int | `3` | At-least-once retry budget. `0` disables retries. |
| `NOTIFY_LIVE_RETRY_INTERVAL` | duration | `5s` | Interval between retry attempts. |
| `NOTIFY_EMAIL_PROVIDER` | enum | `none` | `none` · `emailservice` (others land in later waves). |
| `NOTIFY_EMAIL_FROM` | string | — | Default From address. Required when a provider is set. |
| `NOTIFY_EMAIL_SERVICE_ADDRESS` | string | — | `host:port` of the elloloop EmailService. Required for `emailservice`. |
| `NOTIFY_EMAIL_SMTP_*` | various | — | SMTP fallback knobs (host, port, username, password). |
| `NOTIFY_SMS_PROVIDER` | enum | — | `twilio` (others land later). |
| `NOTIFY_SMS_ACCOUNT_SID` | string | — | Twilio Account SID. Required when SMS provider is twilio. |
| `NOTIFY_SMS_AUTH_TOKEN` | string | — | Twilio Auth Token. Required when SMS provider is twilio. |
| `NOTIFY_SMS_FROM` | string | — | E.164 sender (e.g. `+15555550000`). |
| `NOTIFY_WHATSAPP_*` | various | — | Same shape as the SMS block. |
| `NOTIFY_WEBPUSH_PROVIDER` | enum | — | `vapid`. |
| `NOTIFY_WEBPUSH_VAPID_PUBLIC` | string | — | VAPID public key (base64url). |
| `NOTIFY_WEBPUSH_VAPID_PRIVATE` | string | — | VAPID private key (base64url). |
| `NOTIFY_WEBPUSH_CONTACT_EMAIL` | string | — | Contact email for the push service. |
| `NOTIFY_MOBILEPUSH_PROVIDER` | enum | — | `fcm` (others land later). |
| `NOTIFY_FCM_CREDENTIALS_JSON` | string | — | Service-account JSON. Required for FCM. |
| `NOTIFY_FCM_PROJECT_ID` | string | — | Firebase project id. Required for FCM. |
| `NOTIFY_APNS_KEY_P8` | string | — | APNs auth key (P8 PEM). |
| `NOTIFY_APNS_KEY_ID` | string | — | APNs key id. |
| `NOTIFY_APNS_TEAM_ID` | string | — | Apple developer team id. |
| `NOTIFY_APNS_TOPIC` | string | — | Bundle id / topic. |
| `NOTIFY_APNS_SANDBOX` | bool | `false` | Use the APNs sandbox endpoint. |

## Docs

Full documentation lives at [elloloop.github.io/notify](https://elloloop.github.io/notify/):

- [Quick Start](https://elloloop.github.io/notify/docs/quickstart) — five-minute "hello, notify"
- [Architecture](https://elloloop.github.io/notify/docs/concepts/architecture)
- [Configuration reference](https://elloloop.github.io/notify/docs/installation/configuration)
- [gRPC / Connect API](https://elloloop.github.io/notify/docs/api-reference/grpc)
- [Send a notification](https://elloloop.github.io/notify/docs/examples/send-notification) — Go / Python / cURL
- [Subscribe over SSE](https://elloloop.github.io/notify/docs/examples/subscribe-sse)

The source is in [`docs-site/`](./docs-site) — an Astro static site
built with the `@refraction-ui/astro` shell. Build it locally with:

```bash
cd docs-site
pnpm install
pnpm run build      # produces dist/
pnpm run preview    # serves on http://127.0.0.1:4321/notify
```

## License

AGPL-3.0 — see [LICENSE](./LICENSE).
