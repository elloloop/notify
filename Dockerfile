# notify service — multi-arch standalone build.
#
# Build:
#   docker build -t notify .
#
# Run:
#   docker run -p 8080:8080 -p 8081:8081 -p 9090:9090 \
#     -e NOTIFY_AUTH_JWT_SECRET=... \
#     -e NOTIFY_INTERNAL_TOKEN=... \
#     -e NOTIFY_STORE_DRIVER=memory \
#     notify

FROM --platform=$BUILDPLATFORM golang:1.26.3-alpine3.23 AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/      ./cmd/
COPY internal/ ./internal/
COPY channels/ ./channels/
COPY store/    ./store/
COPY realtime/ ./realtime/
COPY gen/      ./gen/
COPY *.go      ./

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build \
      -ldflags="-s -w -X main.version=$VERSION -X main.commit=$COMMIT" \
      -o /bin/notifyd \
      ./cmd/notifyd

FROM scratch AS server

COPY --from=builder /bin/notifyd /bin/notifyd
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# 8080  — NotificationClientService (browser / mobile, Connect HTTP/2)
# 8081  — NotificationInternalService (backend producers, gRPC)
# 9090  — /metrics + /healthz
EXPOSE 8080 8081 9090

USER 65532:65532
ENTRYPOINT ["/bin/notifyd"]
