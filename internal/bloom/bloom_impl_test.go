package bloom

import (
	"fmt"
	"testing"
)

// TestBitsAndBloomsFilter follows the T1-4 case table in
// Stratum_测试顺序.md exactly, run against the real
// bits-and-blooms/bloom-backed implementation. Written before
// BitsAndBloomsFilter exists (TDD): this file does not compile until
// bloom_impl.go is added.
func TestBitsAndBloomsFilter(t *testing.T) {
	t.Run("add then test hits", func(t *testing.T) {
		f := NewBitsAndBloomsFilter(1000, 0.01)
		f.Add("key1")
		if !f.Test("key1") {
			t.Fatalf("Test(key1) = false, want true after Add")
		}
	})

	t.Run("untested key not added returns false (probabilistic, expected in 99%% of cases)", func(t *testing.T) {
		f := NewBitsAndBloomsFilter(1000, 0.01)
		f.Add("key1")
		if f.Test("not_added") {
			t.Fatalf("Test(not_added) = true; this CAN happen probabilistically but should be rare for an empty-ish filter")
		}
	})

	t.Run("false positive rate stays within configured bound at scale", func(t *testing.T) {
		const n = 100_000
		const targetFPRate = 0.01

		f := NewBitsAndBloomsFilter(n, targetFPRate)
		for i := 0; i < n; i++ {
			f.Add(fmt.Sprintf("inserted-%d", i))
		}

		falsePositives := 0
		for i := 0; i < n; i++ {
			if f.Test(fmt.Sprintf("not-inserted-%d", i)) {
				falsePositives++
			}
		}

		gotRate := float64(falsePositives) / float64(n)
		// Allow some margin above the target rate: this is a randomized
		// structure, and bits-and-blooms' sizing is an approximation, not
		// an exact guarantee. 2x the target is generous enough to avoid
		// test flakiness while still catching a badly misconfigured
		// filter (e.g. one that ignores the fp parameter entirely).
		const maxAcceptableRate = targetFPRate * 2
		if gotRate > maxAcceptableRate {
			t.Fatalf("false positive rate = %.4f (%d/%d), want <= %.4f", gotRate, falsePositives, n, maxAcceptableRate)
		}
		t.Logf("false positive rate = %.4f (%d/%d), target %.4f", gotRate, falsePositives, n, targetFPRate)
	})

	t.Run("serialize then deserialize into new instance preserves membership", func(t *testing.T) {
		f := NewBitsAndBloomsFilter(1000, 0.01)
		f.Add("key1")
		f.Add("key2")
		data, err := f.Serialize()
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}

		f2 := NewBitsAndBloomsFilter(1000, 0.01)
		if err := f2.Deserialize(data); err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if !f2.Test("key1") {
			t.Fatalf("Test(key1) on restored filter = false, want true")
		}
		if !f2.Test("key2") {
			t.Fatalf("Test(key2) on restored filter = false, want true")
		}
	})

	t.Run("reset clears membership", func(t *testing.T) {
		f := NewBitsAndBloomsFilter(1000, 0.01)
		f.Add("key1")
		f.Reset()
		if f.Test("key1") {
			t.Fatalf("Test(key1) after Reset = true, want false")
		}
	})
}

// TestBitsAndBloomsFilter_EdgeCases covers cases beyond the T1-4 table.
func TestBitsAndBloomsFilter_EdgeCases(t *testing.T) {
	t.Run("serialize empty filter round-trips", func(t *testing.T) {
		f := NewBitsAndBloomsFilter(1000, 0.01)
		data, err := f.Serialize()
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}
		f2 := NewBitsAndBloomsFilter(1000, 0.01)
		if err := f2.Deserialize(data); err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if f2.Test("anything") {
			t.Fatalf("Test(anything) on empty restored filter = true, want false")
		}
	})

	t.Run("deserialize invalid data returns an error, does not panic", func(t *testing.T) {
		f := NewBitsAndBloomsFilter(1000, 0.01)
		err := f.Deserialize([]byte{0x01, 0x02, 0x03}) // too short / malformed
		if err == nil {
			t.Fatalf("Deserialize(garbage) = nil error, want an error")
		}
	})

	t.Run("reset then reuse behaves like a fresh filter", func(t *testing.T) {
		f := NewBitsAndBloomsFilter(1000, 0.01)
		f.Add("key1")
		f.Reset()
		f.Add("key2")
		if !f.Test("key2") {
			t.Fatalf("Test(key2) after Reset+Add = false, want true")
		}
		if f.Test("key1") {
			t.Fatalf("Test(key1) after Reset = true, want false (Reset should fully clear state)")
		}
	})
}
