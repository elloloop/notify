package conformance

import (
	"context"
	"strings"
	"testing"

	"github.com/elloloop/notify"
)

// runKeyEdgeConformance probes the composite-key boundaries: long keys, keys
// containing the kind of separator bytes a naive backend might use to
// concatenate (TenantID, UserID, NotificationID) into a flat lookup key, and
// case-sensitivity. A driver that joins composite-key parts into one string
// with a chosen separator silently merges keys that contain the separator —
// the suite makes that visible. memory uses a struct map key; postgres uses
// real composite-column indexes; both pass.
func runKeyEdgeConformance(t *testing.T, d Driver) {
	t.Helper()
	t.Run(d.Name+"/KeyEdge", func(t *testing.T) {
		t.Run("NotificationID_LongValue", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			longID := strings.Repeat("a", 256)
			n := mkNotif("acme", "u1", longID, 1000)
			if _, err := s.CreateNotification(ctx, n); err != nil {
				t.Fatalf("create long-id: %v", err)
			}
			// Idempotent re-create must hit the same row.
			n2 := mkNotif("acme", "u1", longID, 2000)
			created, err := s.CreateNotification(ctx, n2)
			if err != nil {
				t.Fatalf("re-create long-id: %v", err)
			}
			if created {
				t.Fatal("re-create returned created=true on the same long key")
			}
			if n2.ID != n.ID {
				t.Fatalf("idempotent id mismatch: %q vs %q", n2.ID, n.ID)
			}
		})

		t.Run("NotificationID_SeparatorBytesDoNotCollide", func(t *testing.T) {
			// a and b would collide if a backend joined (tenant|user|nid) with
			// '|' into a single lookup key — both produce "acme|u1|n1|n2".
			// A correct backend keeps them distinct.
			ctx := context.Background()
			s := d.New(t)
			a := mkNotif("acme", "u1", "n1|n2", 1000)
			b := mkNotif("acme", "u1|n1", "n2", 1001)
			if _, err := s.CreateNotification(ctx, a); err != nil {
				t.Fatalf("create a: %v", err)
			}
			if _, err := s.CreateNotification(ctx, b); err != nil {
				t.Fatalf("create b: %v", err)
			}
			if a.ID == b.ID {
				t.Fatalf("naive '|' separator collision: a and b share id %q", a.ID)
			}
			// And the unicode/symbol id round-trips through Get.
			got, err := s.GetNotification(ctx, "acme", "u1", a.ID)
			if err != nil || got == nil {
				t.Fatalf("get a: %v", err)
			}
			if got.NotificationID != "n1|n2" {
				t.Fatalf("NotificationID round-trip mismatch: %q", got.NotificationID)
			}
		})

		t.Run("DeviceType_CaseSensitive_SeparateRows", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			lower, err := s.UpsertDevice(ctx, &notify.Device{TenantID: "acme", UserID: "u1", DeviceType: "android", Token: "t-lower"})
			if err != nil {
				t.Fatalf("upsert lower: %v", err)
			}
			upper, err := s.UpsertDevice(ctx, &notify.Device{TenantID: "acme", UserID: "u1", DeviceType: "Android", Token: "t-upper"})
			if err != nil {
				t.Fatalf("upsert upper: %v", err)
			}
			if lower.ID == upper.ID {
				t.Fatalf("device upsert treated 'android' and 'Android' as the same key (id=%q)", lower.ID)
			}
			list, _ := s.ListDevices(ctx, "acme", "u1")
			if len(list) != 2 {
				t.Fatalf("case-sensitive upserts produced %d rows, want 2", len(list))
			}
		})
	})
}
