package postgres

import (
	"errors"
	"strings"
	"time"
)

// Config controls how the Postgres-backed notify.Store connects to its
// database.
//
// DSN is the libpq-style connection string, e.g.
//
//	postgres://user:pass@host:5432/dbname?sslmode=disable
//
// MaxConns caps the underlying pgxpool. A zero value means use DefaultMaxConns.
//
// ConnTimeout is the per-acquire timeout used when opening a new connection
// in the pool. It does NOT bound total query time — callers still pass a
// context with the appropriate deadline.
//
// AutoMigrate controls whether New() applies pending schema migrations on
// first connect. In CI / dev / test we want true (matches identity); in
// strict production deploys teams may flip it to false and run the SQL
// migrations from a deploy pipeline instead.
type Config struct {
	DSN         string
	MaxConns    int32
	ConnTimeout time.Duration
	AutoMigrate bool
}

// DefaultMaxConns is used when Config.MaxConns is zero.
const DefaultMaxConns int32 = 25

// DefaultConnTimeout is used when Config.ConnTimeout is zero.
const DefaultConnTimeout = 5 * time.Second

func (c *Config) applyDefaults() {
	if c.MaxConns == 0 {
		c.MaxConns = DefaultMaxConns
	}
	if c.ConnTimeout == 0 {
		c.ConnTimeout = DefaultConnTimeout
	}
}

func (c *Config) validate() error {
	if c == nil {
		return errors.New("postgres: nil config")
	}
	if strings.TrimSpace(c.DSN) == "" {
		return errors.New("postgres: DSN is required")
	}
	return nil
}
