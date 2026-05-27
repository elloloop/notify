package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/golang-jwt/jwt/v5"
)

// claimsKey is a context-key type unique to this package; using a named type
// (not the string literal) prevents accidental collisions with any other
// package that wants to stash something on the request context.
type claimsKey struct{}

// Claims is the small, typed view of the JWT that handlers actually need. The
// raw jwt.MapClaims is intentionally NOT exposed — handlers never reach for
// fields the platform did not promise.
type Claims struct {
	UserID   string
	TenantID string
	Email    string
	// ExpiresAt is the resolved token expiry (UTC). Zero means none.
	ExpiresAt time.Time
}

// ClaimsFromContext returns the Claims attached by the auth interceptor and a
// boolean reporting whether the request was authenticated. Handlers call this
// instead of touching ctx.Value directly so the lookup path is one assertion.
func ClaimsFromContext(ctx context.Context) (Claims, bool) {
	c, ok := ctx.Value(claimsKey{}).(Claims)
	return c, ok
}

// withClaims is the inverse of ClaimsFromContext; kept unexported because only
// the auth middleware should ever stamp claims onto a request.
func withClaims(ctx context.Context, c Claims) context.Context {
	return context.WithValue(ctx, claimsKey{}, c)
}

// AuthValidator turns a raw `Authorization` header value into Claims.
//
// Two implementations live in this package:
//
//   - JWTValidator   — HS256 verification against a shared secret.
//   - DevValidator   — accepts `Bearer dev:<userid>:<tenant>` (and optionally
//     `:<email>`), used only when AuthConfig.DevMode=true. The container
//     refuses to boot with DevMode=false and no JWT secret.
type AuthValidator interface {
	Validate(authzHeader string) (Claims, error)
}

// JWTValidator verifies HS256 tokens against a static secret.
type JWTValidator struct {
	secret   []byte
	issuer   string
	audience string
	leeway   time.Duration
	now      func() time.Time
}

// NewJWTValidator returns a JWTValidator. Empty secret is rejected — callers
// that want to skip JWT verification entirely should use DevValidator instead.
func NewJWTValidator(secret, issuer, audience string, leeway time.Duration) (*JWTValidator, error) {
	if secret == "" {
		return nil, errors.New("auth: jwt secret is empty")
	}
	if leeway < 0 {
		return nil, errors.New("auth: jwt leeway must be >= 0")
	}
	return &JWTValidator{
		secret:   []byte(secret),
		issuer:   issuer,
		audience: audience,
		leeway:   leeway,
		now:      time.Now,
	}, nil
}

// Validate parses, verifies and extracts our Claims from one Authorization
// header value (`Bearer <token>`). All failure modes are mapped to
// connect.Error with CodeUnauthenticated so the surface returns the right
// thing to callers.
func (v *JWTValidator) Validate(authz string) (Claims, error) {
	tok, err := stripBearer(authz)
	if err != nil {
		return Claims{}, unauthenticated(err)
	}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Name}),
		jwt.WithLeeway(v.leeway),
		jwt.WithTimeFunc(v.now),
	)
	if v.issuer != "" {
		parser = jwt.NewParser(
			jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Name}),
			jwt.WithLeeway(v.leeway),
			jwt.WithTimeFunc(v.now),
			jwt.WithIssuer(v.issuer),
		)
	}
	if v.audience != "" {
		parser = jwt.NewParser(
			jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Name}),
			jwt.WithLeeway(v.leeway),
			jwt.WithTimeFunc(v.now),
			jwt.WithIssuer(v.issuer),
			jwt.WithAudience(v.audience),
		)
	}
	claims := jwt.MapClaims{}
	if _, err := parser.ParseWithClaims(tok, claims, func(*jwt.Token) (interface{}, error) {
		return v.secret, nil
	}); err != nil {
		return Claims{}, unauthenticated(err)
	}

	c := Claims{}
	if sub, ok := claims["sub"].(string); ok {
		c.UserID = sub
	}
	if t, ok := claims["tenant"].(string); ok {
		c.TenantID = t
	} else if t, ok := claims["tenant_id"].(string); ok {
		c.TenantID = t
	}
	if e, ok := claims["email"].(string); ok {
		c.Email = e
	}
	switch exp := claims["exp"].(type) {
	case float64:
		c.ExpiresAt = time.Unix(int64(exp), 0)
	case int64:
		c.ExpiresAt = time.Unix(exp, 0)
	}

	if c.UserID == "" {
		return Claims{}, unauthenticated(errors.New("jwt missing sub claim"))
	}
	if c.TenantID == "" {
		return Claims{}, unauthenticated(errors.New("jwt missing tenant claim"))
	}
	return c, nil
}

// DevValidator is the local-dev escape hatch. It accepts a Bearer token of the
// form `dev:<userid>:<tenant>[:<email>]` — anything else is rejected. Used
// only when AuthConfig.DevMode=true and AuthConfig.JWTSecret is empty.
type DevValidator struct{}

// Validate parses the dev token shape. Three or four colon-separated fields,
// the first of which must be the literal `dev`.
func (DevValidator) Validate(authz string) (Claims, error) {
	tok, err := stripBearer(authz)
	if err != nil {
		return Claims{}, unauthenticated(err)
	}
	parts := strings.Split(tok, ":")
	if len(parts) < 3 || len(parts) > 4 || parts[0] != "dev" {
		return Claims{}, unauthenticated(errors.New("dev token must be dev:<userid>:<tenant>[:<email>]"))
	}
	if parts[1] == "" || parts[2] == "" {
		return Claims{}, unauthenticated(errors.New("dev token missing userid or tenant"))
	}
	c := Claims{UserID: parts[1], TenantID: parts[2]}
	if len(parts) == 4 {
		c.Email = parts[3]
	}
	return c, nil
}

// stripBearer accepts `Bearer <token>` (case-insensitive on the scheme,
// permissive on whitespace) and returns the bare token, or an error.
func stripBearer(authz string) (string, error) {
	if authz == "" {
		return "", errors.New("missing Authorization header")
	}
	const prefix = "bearer "
	low := strings.ToLower(authz)
	if !strings.HasPrefix(low, prefix) {
		return "", errors.New("Authorization header must use Bearer scheme")
	}
	tok := strings.TrimSpace(authz[len(prefix):])
	if tok == "" {
		return "", errors.New("Authorization header has empty token")
	}
	return tok, nil
}

// unauthenticated wraps any auth error into a Connect Unauthenticated code so
// handlers / interceptors return the right wire status.
func unauthenticated(err error) error {
	return connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("notify: unauthenticated: %w", err))
}

// NewClientAuthInterceptor returns a Connect interceptor that validates the
// `Authorization` header on every unary AND streaming RPC in the client-facing
// service, attaching Claims to the context on success.
//
// Internal RPCs use a separate header (X-Notify-Internal-Token) and a
// different interceptor; see NewInternalAuthInterceptor.
func NewClientAuthInterceptor(v AuthValidator) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			claims, err := v.Validate(req.Header().Get("Authorization"))
			if err != nil {
				return nil, err
			}
			return next(withClaims(ctx, claims), req)
		}
	}
}

// authenticateStreamReq is the same logic as NewClientAuthInterceptor but
// applied at the start of a streaming handler, because connect-go's
// UnaryInterceptorFunc does not see the streaming handshake. Returning the
// (possibly-augmented) context and an optional error is the smallest seam
// that keeps the validation logic single-sourced.
func authenticateStreamReq(ctx context.Context, v AuthValidator, header string) (context.Context, error) {
	claims, err := v.Validate(header)
	if err != nil {
		return ctx, err
	}
	return withClaims(ctx, claims), nil
}

// NewInternalAuthInterceptor returns an interceptor that compares the
// X-Notify-Internal-Token header against the configured shared secret. An
// empty configured secret means "skip the check" (only allowed in DevMode).
func NewInternalAuthInterceptor(expected string, devMode bool) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if expected == "" {
				if devMode {
					return next(ctx, req)
				}
				return nil, unauthenticated(errors.New("internal token not configured"))
			}
			got := req.Header().Get("X-Notify-Internal-Token")
			if got == "" {
				return nil, unauthenticated(errors.New("missing X-Notify-Internal-Token header"))
			}
			if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
				return nil, unauthenticated(errors.New("internal token mismatch"))
			}
			return next(ctx, req)
		}
	}
}
