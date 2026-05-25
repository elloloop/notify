package memory_test

import (
	"testing"

	"github.com/elloloop/notify"
	"github.com/elloloop/notify/store/conformance"
	"github.com/elloloop/notify/store/memory"
)

// TestConformance runs the shared, driver-agnostic suite against the in-memory
// store. The entdb and postgres drivers run the identical suite, so all three
// backends are guaranteed to behave the same.
func TestConformance(t *testing.T) {
	conformance.RunConformance(t, conformance.Driver{
		Name: "memory",
		New:  func(t *testing.T) notify.Store { return memory.New() },
	})
}
