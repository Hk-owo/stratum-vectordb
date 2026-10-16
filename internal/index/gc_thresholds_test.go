package index

import "testing"

// TestGCThresholdsStaySeparated pins the 2026-10 decision that the collection bar
// sits ABOVE the append-reuse bar.
//
// At equal values §8.6(c)'s rebuild fires the moment §8.6(d) starts caring about a
// version, so the collection path never observes a candidate — which is exactly how
// the feature spent its first months collecting nothing (docs/known-gaps.md §G,
// docs/stress-test-report.md §C.5). The two constants are easy to "tidy up" back
// into equality, so the RELATION is asserted rather than the numbers.
func TestGCThresholdsStaySeparated(t *testing.T) {
	if DefaultGCRatioThreshold <= DefaultAppendMaxDeadRatio {
		t.Fatalf("DefaultGCRatioThreshold (%v) must stay above DefaultAppendMaxDeadRatio (%v): "+
			"at equal or lower values the append-reuse rebuild wins the race and §8.6(d) never sees a candidate",
			DefaultGCRatioThreshold, DefaultAppendMaxDeadRatio)
	}
}
