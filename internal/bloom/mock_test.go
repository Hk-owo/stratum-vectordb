package bloom

import "testing"

// TestMockBloomFilter sanity-checks the in-memory mock's behavior, mirroring
// the functional (non-false-positive-rate) case shapes from T1-4 in
// Stratum_测试顺序.md. False-positive-rate testing is out of scope for this
// mock by design — see the MockBloomFilter doc comment — and belongs to the
// real bits-and-blooms-backed implementation's own T1-4 suite in Phase 1.
func TestMockBloomFilter(t *testing.T) {
	t.Run("add then test hits", func(t *testing.T) {
		b := NewMockBloomFilter()
		b.Add("key1")
		if !b.Test("key1") {
			t.Fatalf("Test(key1) = false, want true after Add")
		}
	})

	t.Run("untested key not added", func(t *testing.T) {
		b := NewMockBloomFilter()
		if b.Test("not_added") {
			t.Fatalf("Test(not_added) = true, want false")
		}
	})

	t.Run("serialize then deserialize into new instance preserves membership", func(t *testing.T) {
		b := NewMockBloomFilter()
		b.Add("key1")
		b.Add("key2")
		data, err := b.Serialize()
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}

		b2 := NewMockBloomFilter()
		if err := b2.Deserialize(data); err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if !b2.Test("key1") {
			t.Fatalf("Test(key1) on restored filter = false, want true")
		}
		if !b2.Test("key2") {
			t.Fatalf("Test(key2) on restored filter = false, want true")
		}
		if b2.Test("key3") {
			t.Fatalf("Test(key3) on restored filter = true, want false")
		}
	})

	t.Run("reset clears membership", func(t *testing.T) {
		b := NewMockBloomFilter()
		b.Add("key1")
		b.Reset()
		if b.Test("key1") {
			t.Fatalf("Test(key1) after Reset = true, want false")
		}
	})

	t.Run("serialize empty filter round-trips", func(t *testing.T) {
		b := NewMockBloomFilter()
		data, err := b.Serialize()
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}
		b2 := NewMockBloomFilter()
		if err := b2.Deserialize(data); err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if b2.Test("anything") {
			t.Fatalf("Test(anything) on empty restored filter = true, want false")
		}
	})
}
