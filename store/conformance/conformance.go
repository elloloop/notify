// Package conformance is a driver-agnostic test suite for notify.Store
// implementations.
//
// Every backend — memory, entdb, postgres — runs the same assertions via
// RunConformance, so the drivers stay behaviourally identical. A failing path
// like TestConformance/postgres/Idempotency points straight at the driver and
// the exact semantic that broke. A new driver runs:
//
//	conformance.RunConformance(t, conformance.Driver{
//	    Name: "mydriver",
//	    New:  func(t *testing.T) notify.Store { return mydriver.New(...) },
//	})
//
// and either passes or fails loudly. memory is the differential reference for
// "correct"; entdb and postgres must match it.
package conformance

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/elloloop/notify"
)

// Driver names the implementation under test and builds a fresh, empty Store
// for each subtest. Drivers that need per-test cleanup should register it with
// t.Cleanup inside New so one subtest never leaks state into the next.
type Driver struct {
	Name string
	New  func(t *testing.T) notify.Store
}

func mkNotif(tenant, user, nid string, createdMS int64) *notify.Notification {
	return &notify.Notification{
		NotificationID: nid,
		TenantID:       tenant,
		UserID:         user,
		Title:          "t-" + nid,
		Channel:        notify.ChannelInApp,
		Status:         notify.StatusPending,
		CreatedAtMS:    createdMS,
	}
}

func ids(ns []*notify.Notification) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.NotificationID
	}
	return out
}

// RunConformance exercises the full notify.Store contract against driver.New,
// under a t.Run group named for the driver.
func RunConformance(t *testing.T, d Driver) {
	t.Helper()
	if d.Name == "" {
		t.Fatal("conformance: Driver.Name is required")
	}
	if d.New == nil {
		t.Fatal("conformance: Driver.New is required")
	}

	t.Run(d.Name, func(t *testing.T) {
		t.Run("CreateGet", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)

			n := mkNotif("acme", "u1", "nid-1", 1000)
			created, err := s.CreateNotification(ctx, n)
			if err != nil {
				t.Fatalf("CreateNotification: %v", err)
			}
			if !created {
				t.Fatal("CreateNotification: created=false on first insert")
			}
			if n.ID == "" {
				t.Fatal("CreateNotification: did not assign ID")
			}

			got, err := s.GetNotification(ctx, "acme", "u1", n.ID)
			if err != nil {
				t.Fatalf("GetNotification: %v", err)
			}
			if got == nil || got.NotificationID != "nid-1" || got.Title != "t-nid-1" {
				t.Fatalf("round-trip mismatch: %#v", got)
			}
			if got.Status != notify.StatusPending {
				t.Fatalf("status = %q, want pending", got.Status)
			}
		})

		t.Run("GetNotFound", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			_, err := s.GetNotification(ctx, "acme", "u1", "no-such")
			if !errors.Is(err, notify.ErrNotFound) {
				t.Fatalf("GetNotification unknown: want ErrNotFound, got %v", err)
			}
		})

		t.Run("Idempotency", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)

			n1 := mkNotif("acme", "u1", "dup", 1000)
			created, err := s.CreateNotification(ctx, n1)
			if err != nil || !created {
				t.Fatalf("first create: created=%v err=%v", created, err)
			}

			n2 := mkNotif("acme", "u1", "dup", 2000)
			n2.Title = "changed"
			created, err = s.CreateNotification(ctx, n2)
			if err != nil {
				t.Fatalf("second create: %v", err)
			}
			if created {
				t.Fatal("second create on same (tenant,user,notificationID): want created=false")
			}
			if n2.ID != n1.ID {
				t.Fatalf("idempotent create id = %q, want existing %q", n2.ID, n1.ID)
			}

			got, err := s.GetNotification(ctx, "acme", "u1", n1.ID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if got.Title != "t-dup" {
				t.Fatalf("idempotent create mutated the stored row: title=%q", got.Title)
			}
		})

		t.Run("StatusTransitions", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)

			n := mkNotif("acme", "u1", "s", 1000)
			if _, err := s.CreateNotification(ctx, n); err != nil {
				t.Fatalf("create: %v", err)
			}

			if err := s.UpdateStatus(ctx, "acme", n.ID, notify.StatusDelivered, 1100); err != nil {
				t.Fatalf("UpdateStatus delivered: %v", err)
			}
			got, _ := s.GetNotification(ctx, "acme", "u1", n.ID)
			if got.Status != notify.StatusDelivered || got.DeliveredAtMS != 1100 {
				t.Fatalf("after delivered: status=%q at=%d", got.Status, got.DeliveredAtMS)
			}

			if err := s.UpdateStatus(ctx, "acme", n.ID, notify.StatusRead, 1200); err != nil {
				t.Fatalf("UpdateStatus read: %v", err)
			}
			got, _ = s.GetNotification(ctx, "acme", "u1", n.ID)
			if got.Status != notify.StatusRead || got.ReadAtMS != 1200 {
				t.Fatalf("after read: status=%q at=%d", got.Status, got.ReadAtMS)
			}

			if err := s.UpdateStatus(ctx, "acme", "no-such", notify.StatusRead, 1); !errors.Is(err, notify.ErrNotFound) {
				t.Fatalf("UpdateStatus unknown: want ErrNotFound, got %v", err)
			}
		})

		t.Run("CursorWalk_ThreePages", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			for i := 0; i < 5; i++ {
				n := mkNotif("acme", "u1", fmt.Sprintf("p%d", i), int64(1000+i*10))
				if _, err := s.CreateNotification(ctx, n); err != nil {
					t.Fatalf("create p%d: %v", i, err)
				}
			}

			// Page 1 (newest two): p4(1040), p3(1030).
			items, cursor, _, err := s.QueryUserNotifications(ctx, notify.Query{TenantID: "acme", UserID: "u1", Limit: 2})
			if err != nil {
				t.Fatalf("page1: %v", err)
			}
			if got := ids(items); len(got) != 2 || got[0] != "p4" || got[1] != "p3" {
				t.Fatalf("page1 order = %v, want [p4 p3]", got)
			}
			if cursor == "" {
				t.Fatal("page1: expected a non-empty cursor")
			}

			// Page 2: p2(1020), p1(1010).
			cur := parseCursor(t, cursor)
			items, cursor, _, err = s.QueryUserNotifications(ctx, notify.Query{TenantID: "acme", UserID: "u1", Limit: 2, CursorMS: &cur})
			if err != nil {
				t.Fatalf("page2: %v", err)
			}
			if got := ids(items); len(got) != 2 || got[0] != "p2" || got[1] != "p1" {
				t.Fatalf("page2 order = %v, want [p2 p1]", got)
			}
			if cursor == "" {
				t.Fatal("page2: expected a non-empty cursor")
			}

			// Page 3 (last): p0(1000), no further cursor.
			cur = parseCursor(t, cursor)
			items, cursor, _, err = s.QueryUserNotifications(ctx, notify.Query{TenantID: "acme", UserID: "u1", Limit: 2, CursorMS: &cur})
			if err != nil {
				t.Fatalf("page3: %v", err)
			}
			if got := ids(items); len(got) != 1 || got[0] != "p0" {
				t.Fatalf("page3 = %v, want [p0]", got)
			}
			if cursor != "" {
				t.Fatalf("page3: expected empty cursor, got %q", cursor)
			}
		})

		t.Run("UnreadFilterAndCount", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			for _, nid := range []string{"a", "b", "c"} {
				n := mkNotif("acme", "u1", nid, 1000)
				if _, err := s.CreateNotification(ctx, n); err != nil {
					t.Fatalf("create %s: %v", nid, err)
				}
				if nid == "a" {
					if err := s.UpdateStatus(ctx, "acme", n.ID, notify.StatusRead, 1100); err != nil {
						t.Fatalf("mark read: %v", err)
					}
				}
			}

			items, _, unread, err := s.QueryUserNotifications(ctx, notify.Query{TenantID: "acme", UserID: "u1", UnreadOnly: true})
			if err != nil {
				t.Fatalf("unreadOnly: %v", err)
			}
			if len(items) != 2 {
				t.Fatalf("unreadOnly returned %d, want 2", len(items))
			}
			if unread != 2 {
				t.Fatalf("unreadCount = %d, want 2", unread)
			}

			items, _, unread, err = s.QueryUserNotifications(ctx, notify.Query{TenantID: "acme", UserID: "u1"})
			if err != nil {
				t.Fatalf("all: %v", err)
			}
			if len(items) != 3 {
				t.Fatalf("all returned %d, want 3", len(items))
			}
			if unread != 2 {
				t.Fatalf("unreadCount (all) = %d, want 2", unread)
			}
		})

		t.Run("DeviceUpsertRotation", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)

			first, err := s.UpsertDevice(ctx, &notify.Device{
				TenantID: "acme", UserID: "u1", DeviceType: "android", Token: "tok-1", CreatedAtMS: 100, LastActiveMS: 100,
			})
			if err != nil {
				t.Fatalf("first upsert: %v", err)
			}
			if first.ID == "" {
				t.Fatal("upsert did not assign device ID")
			}

			rotated, err := s.UpsertDevice(ctx, &notify.Device{
				TenantID: "acme", UserID: "u1", DeviceType: "android", Token: "tok-2", LastActiveMS: 200,
			})
			if err != nil {
				t.Fatalf("rotate upsert: %v", err)
			}
			if rotated.ID != first.ID {
				t.Fatalf("rotation created a new row: %q vs %q", rotated.ID, first.ID)
			}
			if rotated.Token != "tok-2" {
				t.Fatalf("token not rotated: %q", rotated.Token)
			}

			list, err := s.ListDevices(ctx, "acme", "u1")
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			if len(list) != 1 {
				t.Fatalf("after rotation: %d devices, want 1", len(list))
			}

			if _, err := s.UpsertDevice(ctx, &notify.Device{TenantID: "acme", UserID: "u1", DeviceType: "ios", Token: "tok-ios"}); err != nil {
				t.Fatalf("second device: %v", err)
			}
			list, _ = s.ListDevices(ctx, "acme", "u1")
			if len(list) != 2 {
				t.Fatalf("two device types: %d devices, want 2", len(list))
			}
		})

		t.Run("UserIsolation", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)

			a := mkNotif("acme", "u1", "x", 1000)
			b := mkNotif("acme", "u2", "x", 1000)
			c := mkNotif("beta", "u1", "x", 1000)
			for _, n := range []*notify.Notification{a, b, c} {
				if _, err := s.CreateNotification(ctx, n); err != nil {
					t.Fatalf("create: %v", err)
				}
			}

			items, _, _, err := s.QueryUserNotifications(ctx, notify.Query{TenantID: "acme", UserID: "u1"})
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if len(items) != 1 || items[0].ID != a.ID {
				t.Fatalf("isolation leak: %v", ids(items))
			}

			// Cross-user get must miss.
			if _, err := s.GetNotification(ctx, "acme", "u2", a.ID); !errors.Is(err, notify.ErrNotFound) {
				t.Fatalf("cross-user get: want ErrNotFound, got %v", err)
			}
			// Cross-tenant get must miss.
			if _, err := s.GetNotification(ctx, "beta", "u1", a.ID); !errors.Is(err, notify.ErrNotFound) {
				t.Fatalf("cross-tenant get: want ErrNotFound, got %v", err)
			}
		})
	})

	// Extended categories — each opens its own t.Run(d.Name+"/<Category>")
	// group so the test path immediately attributes a failure to the bug class
	// that broke. memory passes every category and is the differential
	// reference; entdb and postgres MUST match it.
	runPaginationConformance(t, d)
	runFreshTenantConformance(t, d)
	runRoundTripConformance(t, d)
	runConcurrencyConformance(t, d)
	runKeyEdgeConformance(t, d)
}

func parseCursor(t *testing.T, cursor string) int64 {
	t.Helper()
	v, err := strconv.ParseInt(cursor, 10, 64)
	if err != nil {
		t.Fatalf("cursor %q is not an int64: %v", cursor, err)
	}
	return v
}
