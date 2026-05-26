// Package memory is an in-process notify.Store for tests and local development.
// It is the differential reference the conformance suite checks the entdb and
// postgres drivers against: "correct" is whatever memory does.
package memory

import (
	"context"
	"sort"
	"strconv"
	"sync"

	"github.com/google/uuid"

	"github.com/elloloop/notify"
)

// notifKey and devKey use struct values (not concatenated strings) as map keys
// so a NotificationID/DeviceType that contains a chosen separator byte cannot
// collide with a different composition that hashes to the same flat string.
// This keeps memory honest as the "correct" reference for the conformance
// suite's KeyEdge probes.
type notifKey struct{ Tenant, User, NotificationID string }
type devKey struct{ Tenant, User, DeviceType string }

// Store is a goroutine-safe in-memory notify.Store.
type Store struct {
	mu            sync.Mutex
	notifications map[string]*notify.Notification
	notifByKey    map[notifKey]string
	devices       map[string]*notify.Device
	deviceByKey   map[devKey]string
}

// New returns an empty Store.
func New() *Store {
	return &Store{
		notifications: make(map[string]*notify.Notification),
		notifByKey:    make(map[notifKey]string),
		devices:       make(map[string]*notify.Device),
		deviceByKey:   make(map[devKey]string),
	}
}

var _ notify.Store = (*Store)(nil)

func cloneNotification(n *notify.Notification) *notify.Notification {
	c := *n
	return &c
}

func cloneDevice(d *notify.Device) *notify.Device {
	c := *d
	return &c
}

// CreateNotification implements notify.Store.
func (s *Store) CreateNotification(_ context.Context, n *notify.Notification) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := notifKey{n.TenantID, n.UserID, n.NotificationID}
	if existingID, ok := s.notifByKey[key]; ok {
		n.ID = existingID
		return false, nil
	}
	if n.ID == "" {
		n.ID = uuid.NewString()
	}
	s.notifications[n.ID] = cloneNotification(n)
	s.notifByKey[key] = n.ID
	return true, nil
}

// GetNotification implements notify.Store.
func (s *Store) GetNotification(_ context.Context, tenantID, userID, id string) (*notify.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.notifications[id]
	if !ok || n.TenantID != tenantID || n.UserID != userID {
		return nil, notify.ErrNotFound
	}
	return cloneNotification(n), nil
}

// UpdateStatus implements notify.Store.
func (s *Store) UpdateStatus(_ context.Context, tenantID, id string, status notify.DeliveryStatus, atMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.notifications[id]
	if !ok || n.TenantID != tenantID {
		return notify.ErrNotFound
	}
	n.Status = status
	switch status {
	case notify.StatusDelivered:
		n.DeliveredAtMS = atMS
	case notify.StatusAcked:
		n.AckAtMS = atMS
	case notify.StatusRead:
		n.ReadAtMS = atMS
	}
	return nil
}

// QueryUserNotifications implements notify.Store.
func (s *Store) QueryUserNotifications(_ context.Context, q notify.Query) ([]*notify.Notification, string, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	limit := notify.ClampLimit(q.Limit)

	var matched []*notify.Notification
	unread := 0
	for _, n := range s.notifications {
		if n.TenantID != q.TenantID || n.UserID != q.UserID {
			continue
		}
		if n.Status != notify.StatusRead {
			unread++
		}
		if q.UnreadOnly && n.Status == notify.StatusRead {
			continue
		}
		if q.CursorMS != nil && n.CreatedAtMS >= *q.CursorMS {
			continue
		}
		matched = append(matched, cloneNotification(n))
	}

	// Newest-first; tiebreak on id for deterministic ordering.
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].CreatedAtMS != matched[j].CreatedAtMS {
			return matched[i].CreatedAtMS > matched[j].CreatedAtMS
		}
		return matched[i].ID > matched[j].ID
	})

	nextCursor := ""
	if len(matched) > limit {
		matched = matched[:limit]
		nextCursor = strconv.FormatInt(matched[len(matched)-1].CreatedAtMS, 10)
	}
	return matched, nextCursor, unread, nil
}

// UpsertDevice implements notify.Store.
func (s *Store) UpsertDevice(_ context.Context, d *notify.Device) (*notify.Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := devKey{d.TenantID, d.UserID, d.DeviceType}
	if id, ok := s.deviceByKey[key]; ok {
		existing := s.devices[id]
		existing.Token = d.Token
		existing.LastActiveMS = d.LastActiveMS
		return cloneDevice(existing), nil
	}
	if d.ID == "" {
		d.ID = uuid.NewString()
	}
	s.devices[d.ID] = cloneDevice(d)
	s.deviceByKey[key] = d.ID
	return cloneDevice(s.devices[d.ID]), nil
}

// ListDevices implements notify.Store.
func (s *Store) ListDevices(_ context.Context, tenantID, userID string) ([]*notify.Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []*notify.Device
	for _, d := range s.devices {
		if d.TenantID == tenantID && d.UserID == userID {
			out = append(out, cloneDevice(d))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeviceType < out[j].DeviceType })
	return out, nil
}
