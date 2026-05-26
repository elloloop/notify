package conformance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/elloloop/notify"
)

// runConcurrencyConformance asserts the Store is the serialization point for
// the races a multi-replica notification service is exposed to:
//
//   - parallel inserts of distinct rows do not get lost,
//   - parallel inserts of the same idempotency key produce exactly one row
//     with exactly one created=true winner,
//   - parallel device upserts on the same composite key produce exactly one
//     row — the composite-uniqueness canary. tenant-shard-db has no native
//     composite unique constraint, so this exposes any missing service-layer
//     guard (cf. identity's ConcurrentDuplicate_OAuthIdentity_SingleRow).
//   - parallel status transitions on one row finish in a valid terminal state
//     without error,
//   - read-your-writes: a goroutine's own CreateNotification is visible to its
//     own QueryUserNotifications immediately after.
//
// All subtests use the (start-channel, N-goroutines, collect-results) pattern
// so the race window is forced open before any work happens. t.Errorf is used
// inside goroutines because t.Fatalf is only safe on the test goroutine.
func runConcurrencyConformance(t *testing.T, d Driver) {
	t.Helper()
	t.Run(d.Name+"/Concurrency", func(t *testing.T) {
		t.Run("ConcurrentCreate_DistinctKeys_NoLostWrites", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			const N = 16

			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < N; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					n := mkNotif("acme", "u1", fmt.Sprintf("d-%d", i), int64(1000+i))
					if _, err := s.CreateNotification(ctx, n); err != nil {
						t.Errorf("goroutine %d create: %v", i, err)
					}
				}(i)
			}
			close(start)
			wg.Wait()

			items, _, _, err := s.QueryUserNotifications(ctx, notify.Query{TenantID: "acme", UserID: "u1", Limit: notify.MaxPageLimit})
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if len(items) != N {
				t.Fatalf("concurrent distinct creates: %d of %d rows survived", len(items), N)
			}
		})

		t.Run("ConcurrentCreate_SameKey_SingleWinner", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			const N = 16
			type outcome struct {
				created bool
				id      string
				err     error
			}
			results := make(chan outcome, N)
			start := make(chan struct{})
			for i := 0; i < N; i++ {
				go func() {
					<-start
					n := mkNotif("acme", "u1", "race", 1000)
					created, err := s.CreateNotification(ctx, n)
					results <- outcome{created, n.ID, err}
				}()
			}
			close(start)

			winners, losers := 0, 0
			var canonicalID string
			for i := 0; i < N; i++ {
				o := <-results
				if o.err != nil {
					t.Errorf("goroutine err: %v", o.err)
					continue
				}
				if o.id == "" {
					t.Errorf("goroutine got empty ID")
				}
				if canonicalID == "" {
					canonicalID = o.id
				}
				if o.id != canonicalID {
					t.Errorf("idempotent create returned %q, want canonical %q", o.id, canonicalID)
				}
				if o.created {
					winners++
				} else {
					losers++
				}
			}
			if winners != 1 {
				t.Errorf("race winners = %d, want 1 (losers=%d)", winners, losers)
			}
			if losers != N-1 {
				t.Errorf("race losers = %d, want %d", losers, N-1)
			}

			items, _, _, _ := s.QueryUserNotifications(ctx, notify.Query{TenantID: "acme", UserID: "u1"})
			if len(items) != 1 {
				t.Errorf("same-key race left %d rows, want 1", len(items))
			}
		})

		t.Run("ConcurrentUpsertDevice_SameKey_SingleRow", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			const N = 16

			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < N; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					dev := &notify.Device{
						TenantID: "acme", UserID: "u1", DeviceType: "android",
						Token: fmt.Sprintf("tok-%d", i), CreatedAtMS: 100, LastActiveMS: int64(100 + i),
					}
					if _, err := s.UpsertDevice(ctx, dev); err != nil {
						t.Errorf("goroutine %d upsert: %v", i, err)
					}
				}(i)
			}
			close(start)
			wg.Wait()

			list, err := s.ListDevices(ctx, "acme", "u1")
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			if len(list) != 1 {
				t.Fatalf("same-key concurrent upserts left %d rows, want 1 — composite-uniqueness guard missing", len(list))
			}
		})

		t.Run("ConcurrentUpdateStatus_NoError", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			n := mkNotif("acme", "u1", "upd", 1000)
			if _, err := s.CreateNotification(ctx, n); err != nil {
				t.Fatalf("create: %v", err)
			}

			const N = 16
			errs := make(chan error, N)
			start := make(chan struct{})
			targets := []notify.DeliveryStatus{notify.StatusDelivered, notify.StatusAcked, notify.StatusRead}
			for i := 0; i < N; i++ {
				go func(i int) {
					<-start
					target := targets[i%len(targets)]
					errs <- s.UpdateStatus(ctx, "acme", n.ID, target, int64(2000+i))
				}(i)
			}
			close(start)
			for i := 0; i < N; i++ {
				if err := <-errs; err != nil && !errors.Is(err, notify.ErrNotFound) {
					t.Errorf("UpdateStatus race err: %v", err)
				}
			}

			got, err := s.GetNotification(ctx, "acme", "u1", n.ID)
			if err != nil || got == nil {
				t.Fatalf("get post-race: %v %#v", err, got)
			}
			switch got.Status {
			case notify.StatusDelivered, notify.StatusAcked, notify.StatusRead:
				// any of the targets is acceptable; none must error.
			default:
				t.Errorf("post-race status = %q, want one of delivered/acked/read", got.Status)
			}
		})

		t.Run("ConcurrentReadYourWrites_QueryAfterCreate", func(t *testing.T) {
			ctx := context.Background()
			s := d.New(t)
			const N = 32

			var wg sync.WaitGroup
			for i := 0; i < N; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					n := mkNotif("acme", "u1", fmt.Sprintf("ryw-%d", i), int64(1000+i))
					if _, err := s.CreateNotification(ctx, n); err != nil {
						t.Errorf("create: %v", err)
						return
					}
					// The writer's own row must be visible to its own immediate query.
					items, _, _, err := s.QueryUserNotifications(ctx, notify.Query{TenantID: "acme", UserID: "u1", Limit: notify.MaxPageLimit})
					if err != nil {
						t.Errorf("query: %v", err)
						return
					}
					found := false
					for _, it := range items {
						if it.ID == n.ID {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("read-your-writes violated: row %q not visible to its own writer", n.NotificationID)
					}
				}(i)
			}
			wg.Wait()
		})
	})
}
