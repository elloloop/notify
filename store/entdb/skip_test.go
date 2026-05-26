//go:build !realentdb

package entdb_test

import "testing"

// TestConformanceSkip is the placeholder that runs on the default (no
// build-tag) test path so `go test ./store/entdb/...` produces a single
// SKIP line instead of an empty package report. The real conformance
// suite lives in realentdb_conformance_test.go behind the `realentdb`
// build tag — invoke `go test -tags=realentdb ./store/entdb/...` with
// NOTIFY_ENTDB_ADDRESS pointing at a live tenant-shard-db service to run
// it.
func TestConformanceSkip(t *testing.T) {
	t.Skip("conformance suite gated behind -tags=realentdb + NOTIFY_ENTDB_ADDRESS")
}
