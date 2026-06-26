package bloom

import (
	"bytes"
	"fmt"
	"sync"

	bitsbloom "github.com/bits-and-blooms/bloom/v3"
)

// BitsAndBloomsFilter is the real BloomFilter implementation, backed by
// github.com/bits-and-blooms/bloom. Used for both chunk-existence
// filtering (one instance per knowledge base) and version-document
// filtering (one instance per version) — see the bloom package doc
// comment for how the two usages share this same type.
//
// expectedItems and falsePositiveRate are fixed at construction time (via
// bloom.NewWithEstimates), matching the bloom_filter.false_positive_rate
// configuration value; Deserialize replaces the filter's internal bit set
// entirely, so the original sizing parameters only matter for filters
// built via Add, not ones restored via Deserialize.
type BitsAndBloomsFilter struct {
	mu            sync.Mutex
	expectedItems uint
	fpRate        float64
	filter        *bitsbloom.BloomFilter
}

// NewBitsAndBloomsFilter constructs a filter sized for expectedItems
// elements at the given target false-positive rate (e.g. 0.01, matching
// bloom_filter.false_positive_rate in the node config).
func NewBitsAndBloomsFilter(expectedItems uint, falsePositiveRate float64) *BitsAndBloomsFilter {
	return &BitsAndBloomsFilter{
		expectedItems: expectedItems,
		fpRate:        falsePositiveRate,
		filter:        bitsbloom.NewWithEstimates(expectedItems, falsePositiveRate),
	}
}

func (f *BitsAndBloomsFilter) Add(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.filter.AddString(key)
}

func (f *BitsAndBloomsFilter) Test(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.filter.TestString(key)
}

// Serialize encodes the filter's current bit set using
// bits-and-blooms/bloom's own binary format (MarshalBinary), which is
// stable and round-trips through Deserialize on any
// BitsAndBloomsFilter instance regardless of its original construction
// parameters.
func (f *BitsAndBloomsFilter) Serialize() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := f.filter.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("bloom: serialize: %w", err)
	}
	return data, nil
}

// Deserialize replaces the filter's internal bit set with the one encoded
// in data, as previously produced by Serialize.
func (f *BitsAndBloomsFilter) Deserialize(data []byte) error {
	restored := &bitsbloom.BloomFilter{}
	if _, err := restored.ReadFrom(bytes.NewReader(data)); err != nil {
		return fmt.Errorf("bloom: deserialize: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.filter = restored
	return nil
}

// Reset clears the filter back to empty, preserving its original sizing
// parameters (expectedItems / falsePositiveRate) rather than leaving it in
// whatever state a prior Deserialize may have put it in.
func (f *BitsAndBloomsFilter) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.filter = bitsbloom.NewWithEstimates(f.expectedItems, f.fpRate)
}

var _ BloomFilter = (*BitsAndBloomsFilter)(nil)
