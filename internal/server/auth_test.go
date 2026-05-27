package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/golang-jwt/jwt/v5"
)

func mustToken(t *testing.T, secret string, claims jwt.MapClaims, method jwt.SigningMethod) string {
	t.Helper()
	tok := jwt.NewWithClaims(method, claims)
	s, err := tok.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

func makeStandardClaims(now time.Time) jwt.MapClaims {
	return jwt.MapClaims{
		"sub":    "user-1",
		"tenant": "tenant-a",
		"email":  "u@example.com",
		"exp":    now.Add(15 * time.Minute).Unix(),
		"iat":    now.Unix(),
	}
}

func TestStripBearer(t *testing.T) {
	cases := []struct {
		name    string
		header  string
		wantTok string
		wantErr bool
	}{
		{"happy path", "Bearer abc.def.ghi", "abc.def.ghi", false},
		{"lowercase scheme", "bearer abc", "abc", false},
		{"trailing whitespace", "Bearer   abc  ", "abc", false},
		{"empty header", "", "", true},
		{"wrong scheme", "Basic abc", "", true},
		{"empty token after scheme", "Bearer  ", "", true},
		{"only scheme", "Bearer", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tok, err := stripBearer(tc.header)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && tok != tc.wantTok {
				t.Fatalf("tok=%q want %q", tok, tc.wantTok)
			}
		})
	}
}

func TestJWTValidator_HappyPath(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	v, err := NewJWTValidator("sekret", "", "", 30*time.Second)
	if err != nil {
		t.Fatalf("NewJWTValidator: %v", err)
	}
	v.now = func() time.Time { return now }

	tok := mustToken(t, "sekret", makeStandardClaims(now), jwt.SigningMethodHS256)
	c, err := v.Validate("Bearer " + tok)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if c.UserID != "user-1" || c.TenantID != "tenant-a" || c.Email != "u@example.com" {
		t.Fatalf("claims = %+v", c)
	}
	if c.ExpiresAt.IsZero() {
		t.Fatalf("ExpiresAt zero, want populated")
	}
}

func TestJWTValidator_TenantIdAliasing(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	v, _ := NewJWTValidator("sekret", "", "", 30*time.Second)
	v.now = func() time.Time { return now }
	claims := jwt.MapClaims{
		"sub":       "u",
		"tenant_id": "t-alt", // alternative claim name
		"exp":       now.Add(15 * time.Minute).Unix(),
	}
	tok := mustToken(t, "sekret", claims, jwt.SigningMethodHS256)
	c, err := v.Validate("Bearer " + tok)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if c.TenantID != "t-alt" {
		t.Fatalf("TenantID = %q, want t-alt", c.TenantID)
	}
}

func TestJWTValidator_Failures(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name   string
		header func() string
		issuer string
	}{
		{
			name: "expired",
			header: func() string {
				c := makeStandardClaims(now)
				c["exp"] = now.Add(-time.Hour).Unix()
				return "Bearer " + mustToken(t, "sekret", c, jwt.SigningMethodHS256)
			},
		},
		{
			name: "missing sub",
			header: func() string {
				c := jwt.MapClaims{"tenant": "t", "exp": now.Add(time.Hour).Unix()}
				return "Bearer " + mustToken(t, "sekret", c, jwt.SigningMethodHS256)
			},
		},
		{
			name: "missing tenant",
			header: func() string {
				c := jwt.MapClaims{"sub": "u", "exp": now.Add(time.Hour).Unix()}
				return "Bearer " + mustToken(t, "sekret", c, jwt.SigningMethodHS256)
			},
		},
		{
			name: "wrong signature",
			header: func() string {
				return "Bearer " + mustToken(t, "other-secret", makeStandardClaims(now), jwt.SigningMethodHS256)
			},
		},
		{
			name: "malformed bearer",
			header: func() string {
				return "Bearer not-a-jwt"
			},
		},
		{
			name: "no Authorization header",
			header: func() string {
				return ""
			},
		},
		{
			name: "wrong issuer",
			header: func() string {
				c := makeStandardClaims(now)
				c["iss"] = "evil-issuer"
				return "Bearer " + mustToken(t, "sekret", c, jwt.SigningMethodHS256)
			},
			issuer: "trusted",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := NewJWTValidator("sekret", tc.issuer, "", 30*time.Second)
			v.now = func() time.Time { return now }
			_, err := v.Validate(tc.header())
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if connect.CodeOf(err) != connect.CodeUnauthenticated {
				t.Fatalf("code = %v, want unauthenticated", connect.CodeOf(err))
			}
		})
	}
}

func TestJWTValidator_ConstructorValidation(t *testing.T) {
	if _, err := NewJWTValidator("", "", "", 0); err == nil {
		t.Fatal("expected error for empty secret")
	}
	if _, err := NewJWTValidator("s", "", "", -1*time.Second); err == nil {
		t.Fatal("expected error for negative leeway")
	}
}

func TestJWTValidator_IssuerAndAudience(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	v, _ := NewJWTValidator("sekret", "iss-trusted", "aud-app", 30*time.Second)
	v.now = func() time.Time { return now }
	c := makeStandardClaims(now)
	c["iss"] = "iss-trusted"
	c["aud"] = "aud-app"
	tok := mustToken(t, "sekret", c, jwt.SigningMethodHS256)
	if _, err := v.Validate("Bearer " + tok); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Wrong audience should now reject.
	c2 := makeStandardClaims(now)
	c2["iss"] = "iss-trusted"
	c2["aud"] = "different"
	tok2 := mustToken(t, "sekret", c2, jwt.SigningMethodHS256)
	if _, err := v.Validate("Bearer " + tok2); err == nil {
		t.Fatal("expected error for wrong audience")
	}
}

func TestDevValidator(t *testing.T) {
	cases := []struct {
		name    string
		header  string
		wantErr bool
		wantUID string
		wantTID string
		wantEm  string
	}{
		{"happy three parts", "Bearer dev:u1:tenant-a", false, "u1", "tenant-a", ""},
		{"with email", "Bearer dev:u2:tenant-b:e@x.com", false, "u2", "tenant-b", "e@x.com"},
		{"missing scheme", "dev:u:t", true, "", "", ""},
		{"wrong prefix", "Bearer not:u:t", true, "", "", ""},
		{"too few parts", "Bearer dev:u", true, "", "", ""},
		{"too many parts", "Bearer dev:u:t:e:extra", true, "", "", ""},
		{"empty user", "Bearer dev::t", true, "", "", ""},
		{"empty tenant", "Bearer dev:u:", true, "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := DevValidator{}.Validate(tc.header)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if c.UserID != tc.wantUID || c.TenantID != tc.wantTID || c.Email != tc.wantEm {
					t.Fatalf("claims = %+v, want uid=%q tid=%q em=%q", c, tc.wantUID, tc.wantTID, tc.wantEm)
				}
			} else if err != nil && connect.CodeOf(err) != connect.CodeUnauthenticated {
				t.Fatalf("code = %v, want unauthenticated", connect.CodeOf(err))
			}
		})
	}
}

func TestClaimsFromContext(t *testing.T) {
	ctx := context.Background()
	if _, ok := ClaimsFromContext(ctx); ok {
		t.Fatal("expected no claims on empty context")
	}
	want := Claims{UserID: "u1", TenantID: "t1"}
	ctx = withClaims(ctx, want)
	got, ok := ClaimsFromContext(ctx)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestClientAuthInterceptor(t *testing.T) {
	v := DevValidator{}
	interceptor := NewClientAuthInterceptor(v)

	called := false
	next := connect.UnaryFunc(func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		called = true
		c, ok := ClaimsFromContext(ctx)
		if !ok {
			t.Fatal("interceptor did not attach claims")
		}
		if c.UserID != "u1" {
			t.Fatalf("UserID=%q", c.UserID)
		}
		return nil, nil
	})

	req := newFakeUnaryReq(http.Header{"Authorization": {"Bearer dev:u1:t1"}})
	if _, err := interceptor(next)(context.Background(), req); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if !called {
		t.Fatal("next was not called")
	}

	// Failure path: no header.
	called = false
	req2 := newFakeUnaryReq(http.Header{})
	if _, err := interceptor(next)(context.Background(), req2); err == nil {
		t.Fatal("expected error from missing header")
	} else if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("code = %v, want unauthenticated", connect.CodeOf(err))
	}
	if called {
		t.Fatal("next must not run on auth failure")
	}
}

func TestInternalAuthInterceptor(t *testing.T) {
	t.Run("happy path matches token", func(t *testing.T) {
		interceptor := NewInternalAuthInterceptor("secret-token", false)
		called := false
		next := connect.UnaryFunc(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
			called = true
			return nil, nil
		})
		req := newFakeUnaryReq(http.Header{"X-Notify-Internal-Token": {"secret-token"}})
		if _, err := interceptor(next)(context.Background(), req); err != nil {
			t.Fatalf("interceptor: %v", err)
		}
		if !called {
			t.Fatal("next was not called")
		}
	})

	t.Run("mismatch rejected", func(t *testing.T) {
		interceptor := NewInternalAuthInterceptor("secret-token", false)
		next := connect.UnaryFunc(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, errors.New("must not be called")
		})
		req := newFakeUnaryReq(http.Header{"X-Notify-Internal-Token": {"wrong"}})
		_, err := interceptor(next)(context.Background(), req)
		if err == nil {
			t.Fatal("expected error")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Fatalf("code = %v", connect.CodeOf(err))
		}
	})

	t.Run("missing header rejected", func(t *testing.T) {
		interceptor := NewInternalAuthInterceptor("secret-token", false)
		next := connect.UnaryFunc(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, errors.New("must not be called")
		})
		req := newFakeUnaryReq(http.Header{})
		_, err := interceptor(next)(context.Background(), req)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("dev mode lets empty config through", func(t *testing.T) {
		interceptor := NewInternalAuthInterceptor("", true)
		called := false
		next := connect.UnaryFunc(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
			called = true
			return nil, nil
		})
		req := newFakeUnaryReq(http.Header{})
		if _, err := interceptor(next)(context.Background(), req); err != nil {
			t.Fatalf("interceptor: %v", err)
		}
		if !called {
			t.Fatal("next was not called")
		}
	})

	t.Run("non-dev empty config rejects", func(t *testing.T) {
		interceptor := NewInternalAuthInterceptor("", false)
		next := connect.UnaryFunc(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, errors.New("must not be called")
		})
		req := newFakeUnaryReq(http.Header{})
		_, err := interceptor(next)(context.Background(), req)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "not configured") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestConstantTimeEqual(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"", "", true},
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"abc", "ab", false},
		{"a", "ab", false},
	}
	for _, tc := range cases {
		if got := constantTimeEqual(tc.a, tc.b); got != tc.want {
			t.Errorf("equal(%q,%q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
