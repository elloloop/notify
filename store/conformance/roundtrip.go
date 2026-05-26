package conformance

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/elloloop/notify"
)

// adversarialValues exercise a backend's serialization, escaping and encoding.
// A value written to a string field must read back byte-for-byte regardless of
// driver; divergence means the backend mangles, truncates or re-encodes.
var adversarialValues = []struct {
	name  string
	value string
}{
	{"unicode_and_emoji", "Zoë Müller 日本語 café 🔐🛂"},
	{"quotes_backslash_control", "O'Brien \"quoted\" back\\slash\nnewline\ttab"},
	{"sql_injection_shaped", "Robert'); DROP TABLE notifications;--"},
	{"like_wildcards", "100%_off _every_ thing%"},
	{"json_shaped", `{"k":"v","nested":[1,2,{"x":null}]}`},
	{"leading_trailing_space", "   padded value   "},
	{"long_10k", strings.Repeat("Z", 10_000)},
	{"single_char", "x"},
}

// runRoundTripConformance asserts value fidelity for the platform's string and
// int64 fields. Targets three known cross-backend bug classes:
//
//   - structpb float64 coercion: tenant-shard-db marshals payloads through
//     structpb, so int64 values above 2^53 lose precision (float64 mantissa).
//     The Int64_Fidelity_Timestamps subtest probes the boundary explicitly.
//   - silent truncation: a backend that caps a string column drops bytes from
//     a large payload without erroring; LargePayload_Body catches it.
//   - escaping mistakes: SQL/JSON-shaped payloads are common attack inputs;
//     StringFields_OnCreate runs every adversarial value through every field.
func runRoundTripConformance(t *testing.T, d Driver) {
	t.Helper()
	t.Run(d.Name+"/RoundTrip", func(t *testing.T) {
		t.Run("StringFields_OnCreate", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			for i, tc := range adversarialValues {
				n := &notify.Notification{
					NotificationID: fmt.Sprintf("rt-%d", i),
					TenantID:       "acme",
					UserID:         "u1",
					Title:          tc.value,
					Body:           tc.value,
					SubjectRef:     tc.value,
					SubjectType:    tc.value,
					Channel:        notify.ChannelInApp,
					Status:         notify.StatusPending,
					CreatedAtMS:    int64(1000 + i),
				}
				if _, err := s.CreateNotification(ctx, n); err != nil {
					t.Fatalf("%s: create: %v", tc.name, err)
				}
				got, err := s.GetNotification(ctx, "acme", "u1", n.ID)
				if err != nil || got == nil {
					t.Fatalf("%s: get: %v", tc.name, err)
				}
				assertEqualField(t, tc.name, "Title", tc.value, got.Title)
				assertEqualField(t, tc.name, "Body", tc.value, got.Body)
				assertEqualField(t, tc.name, "SubjectRef", tc.value, got.SubjectRef)
				assertEqualField(t, tc.name, "SubjectType", tc.value, got.SubjectType)
			}
		})

		t.Run("Int64_Fidelity_Timestamps", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			cases := []struct {
				name string
				v    int64
			}{
				{"one", 1},
				{"realistic_epoch_ms", 1_700_000_000_000},
				{"two_pow_53_minus_1", (1 << 53) - 1},
				{"two_pow_53", 1 << 53},
				{"two_pow_53_plus_1", (1 << 53) + 1}, // beyond float64 exact-int range
				{"max_int64", math.MaxInt64},
			}
			for i, c := range cases {
				n := &notify.Notification{
					NotificationID: fmt.Sprintf("ts-%d", i),
					TenantID:       "acme",
					UserID:         "u1",
					Title:          "t",
					Channel:        notify.ChannelInApp,
					Status:         notify.StatusPending,
					CreatedAtMS:    c.v,
				}
				if _, err := s.CreateNotification(ctx, n); err != nil {
					t.Fatalf("%s: create: %v", c.name, err)
				}
				if err := s.UpdateStatus(ctx, "acme", n.ID, notify.StatusDelivered, c.v); err != nil {
					t.Fatalf("%s: update: %v", c.name, err)
				}
				got, err := s.GetNotification(ctx, "acme", "u1", n.ID)
				if err != nil || got == nil {
					t.Fatalf("%s: get: %v", c.name, err)
				}
				if got.CreatedAtMS != c.v {
					t.Errorf("%s: CreatedAtMS round-trip = %d, want %d", c.name, got.CreatedAtMS, c.v)
				}
				if got.DeliveredAtMS != c.v {
					t.Errorf("%s: DeliveredAtMS round-trip = %d, want %d", c.name, got.DeliveredAtMS, c.v)
				}
			}
		})

		t.Run("LargePayload_Body", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			for i, size := range []int{64 * 1024, 512 * 1024} {
				val := strings.Repeat("x", size)
				n := &notify.Notification{
					NotificationID: fmt.Sprintf("lp-%d", i),
					TenantID:       "acme",
					UserID:         "u1",
					Title:          "large",
					Body:           val,
					Channel:        notify.ChannelInApp,
					Status:         notify.StatusPending,
					CreatedAtMS:    int64(1000 + i),
				}
				if _, err := s.CreateNotification(ctx, n); err != nil {
					t.Fatalf("size %d: create: %v", size, err)
				}
				got, err := s.GetNotification(ctx, "acme", "u1", n.ID)
				if err != nil || got == nil {
					t.Fatalf("size %d: get: %v", size, err)
				}
				if len(got.Body) != size {
					t.Errorf("size %d: truncated to %d bytes", size, len(got.Body))
				} else if got.Body != val {
					t.Errorf("size %d: corrupted (length matches)", size)
				}
			}
		})
	})
}

func assertEqualField(t *testing.T, caseName, field, want, got string) {
	t.Helper()
	if got != want {
		// Bound the echo so a 10k-char mismatch doesn't flood the log.
		t.Errorf("%s: %s round-trip mismatch:\n  want(len=%d) %.120q\n  got (len=%d) %.120q",
			caseName, field, len(want), want, len(got), got)
	}
}
