package plane

import (
	"testing"
)

// The caller must be able to tell "never spoke" from "spoke a while ago": Degrade names
// the two differently, and only a heartbeat makes a node count as live.
func TestLivenessRegistry_ReportedAtOnlyForNodesThatSpoke(t *testing.T) {
	reg := NewLivenessRegistry()
	if _, ok := reg.ReportedAt(7); ok {
		t.Error("ReportedAt must be absent for a node that never sent a heartbeat")
	}
	reg.Record(7)
	if _, ok := reg.ReportedAt(7); !ok {
		t.Error("ReportedAt must be set for a node that sent one")
	}
	if _, ok := reg.ReportedAt(8); ok {
		t.Error("another node's heartbeat says nothing about this one")
	}
}

// A leadership change must discard the predecessor's soft state rather than inherit it.
func TestLivenessRegistry_ResetDropsTheRecord(t *testing.T) {
	reg := NewLivenessRegistry()
	reg.Record(1)

	reg.Reset()

	if _, ok := reg.ReportedAt(1); ok {
		t.Error("a new term must not judge from heartbeats it never received")
	}
}
