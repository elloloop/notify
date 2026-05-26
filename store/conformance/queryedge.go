package conformance

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/elloloop/notify"
)

// paginationRows is intentionally above tenant-shard-db's observed per-call
// QueryNodes cap (~100). A driver that issues an un-paginated read truncates
// the result at the cap; the suite walks the cursor so all rows must surface.
const paginationRows = 250

// runPaginationConformance asserts QueryUserNotifications pages through every
// matching row even when the total exceeds the underlying server's
// single-query row cap. tenant-shard-db clamps QueryNodes server-side, so any
// read path that does not honour the cursor silently drops rows.
func runPaginationConformance(t *testing.T, d Driver) {
	t.Helper()
	t.Run(d.Name+"/Pagination", func(t *testing.T) {
		t.Run("QueryUserNotifications_AllPagesReturnEveryRow", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			for i := 0; i < paginationRows; i++ {
				n := mkNotif("acme", "u1", fmt.Sprintf("page-%04d", i), int64(1_000+i))
				if _, err := s.CreateNotification(ctx, n); err != nil {
					t.Fatalf("create %d: %v", i, err)
				}
			}

			seen := make(map[string]struct{}, paginationRows)
			var cursorMS *int64
			for page := 0; page < paginationRows+5; page++ {
				items, cursor, _, err := s.QueryUserNotifications(ctx, notify.Query{
					TenantID: "acme", UserID: "u1",
					Limit:    100,
					CursorMS: cursorMS,
				})
				if err != nil {
					t.Fatalf("page %d: %v", page, err)
				}
				for _, n := range items {
					if _, dup := seen[n.NotificationID]; dup {
						t.Errorf("duplicate row across pages: %q", n.NotificationID)
					}
					seen[n.NotificationID] = struct{}{}
				}
				if cursor == "" {
					break
				}
				v, err := strconv.ParseInt(cursor, 10, 64)
				if err != nil {
					t.Fatalf("cursor parse: %v", err)
				}
				cursorMS = &v
			}
			if len(seen) != paginationRows {
				t.Fatalf("paged read returned %d of %d rows — driver does not page past the server cap", len(seen), paginationRows)
			}
		})

		t.Run("StrictLessThanCutoff", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			for i, ms := range []int64{1000, 2000, 3000} {
				n := mkNotif("acme", "u1", fmt.Sprintf("s%d", i), ms)
				if _, err := s.CreateNotification(ctx, n); err != nil {
					t.Fatalf("create: %v", err)
				}
			}
			cutoff := int64(2000)
			items, _, _, err := s.QueryUserNotifications(ctx, notify.Query{
				TenantID: "acme", UserID: "u1", CursorMS: &cutoff,
			})
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			got := ids(items)
			if len(got) != 1 || got[0] != "s0" {
				t.Fatalf("cursor=2000 must return ONLY rows with CreatedAtMS < 2000; got %v", got)
			}
		})
	})
}

// runFreshTenantConformance asserts every read on a brand-new, never-written
// tenant returns an empty result — never a transport error. On
// tenant-shard-db a fresh tenant has no WAL yet, so a filter QueryNodes can
// return FailedPrecondition "tenant not opened" or (since v1.16.0) a
// sanitized Internal error; the driver MUST translate either into "no rows".
func runFreshTenantConformance(t *testing.T, d Driver) {
	t.Helper()
	t.Run(d.Name+"/FreshTenant", func(t *testing.T) {
		t.Run("QueryUserNotifications_Empty", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			items, cursor, unread, err := s.QueryUserNotifications(ctx, notify.Query{TenantID: "fresh", UserID: "u1"})
			if err != nil {
				t.Errorf("QueryUserNotifications on fresh tenant: want nil err, got %v", err)
			}
			if len(items) != 0 {
				t.Errorf("QueryUserNotifications on fresh tenant: want 0 rows, got %d", len(items))
			}
			if cursor != "" {
				t.Errorf("QueryUserNotifications on fresh tenant: want empty cursor, got %q", cursor)
			}
			if unread != 0 {
				t.Errorf("QueryUserNotifications on fresh tenant: want unread=0, got %d", unread)
			}
		})

		t.Run("GetNotification_NotFound", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			_, err := s.GetNotification(ctx, "fresh", "u1", "no-such")
			if !errors.Is(err, notify.ErrNotFound) {
				t.Errorf("GetNotification on fresh tenant: want ErrNotFound, got %v", err)
			}
		})

		t.Run("ListDevices_Empty", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			devs, err := s.ListDevices(ctx, "fresh", "u1")
			if err != nil {
				t.Errorf("ListDevices on fresh tenant: want nil err, got %v", err)
			}
			if len(devs) != 0 {
				t.Errorf("ListDevices on fresh tenant: want 0 devices, got %d", len(devs))
			}
		})
	})
}
