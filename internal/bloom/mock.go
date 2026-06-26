package bloom

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

// MockBloomFilter is an in-memory, exact-set-based stand-in for
// BloomFilter, for use in unit tests of modules that depend on
// BloomFilter (WriteCoordinator, QueryService, etc.).
//
// Unlike the real bits-and-blooms-backed implementation built in Phase 1
// (1-A-4), this mock has no false positives: Test only ever returns true
// for keys that were Added. This is intentional — Phase 0 mocks exist to
// let other modules exercise their own logic in isolation, and false-
// positive behavior is something only the real implementation's own T1-4
// tests should be asserting on. Modules that specifically need to test
// their false-positive-confirmation fallback path should not rely on this
// mock to produce false positives; they should drive that case directly
// (e.g. by asserting the fallback path is invoked when Test is true but
// the authoritative store says no).
type MockBloomFilter struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

// NewMockBloomFilter constructs an empty MockBloomFilter.
func NewMockBloomFilter() *MockBloomFilter {
	return &MockBloomFilter{keys: make(map[string]struct{})}
}

func (b *MockBloomFilter) Add(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.keys[key] = struct{}{}
}

func (b *MockBloomFilter) Test(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.keys[key]
	return ok
}

// Serialize encodes the key set as a simple length-prefixed list. This
// format is private to MockBloomFilter and is not expected to be
// compatible with the real implementation's on-disk format — it only
// needs to round-trip through MockBloomFilter.Deserialize.
func (b *MockBloomFilter) Serialize() ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(b.keys))); err != nil {
		return nil, fmt.Errorf("bloom: serialize count: %w", err)
	}
	for key := range b.keys {
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(key))); err != nil {
			return nil, fmt.Errorf("bloom: serialize key length: %w", err)
		}
		buf.WriteString(key)
	}
	return buf.Bytes(), nil
}

func (b *MockBloomFilter) Deserialize(data []byte) error {
	r := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return fmt.Errorf("bloom: deserialize count: %w", err)
	}
	keys := make(map[string]struct{}, count)
	for i := uint32(0); i < count; i++ {
		var klen uint32
		if err := binary.Read(r, binary.LittleEndian, &klen); err != nil {
			return fmt.Errorf("bloom: deserialize key length: %w", err)
		}
		kbuf := make([]byte, klen)
		if _, err := r.Read(kbuf); err != nil {
			return fmt.Errorf("bloom: deserialize key bytes: %w", err)
		}
		keys[string(kbuf)] = struct{}{}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.keys = keys
	return nil
}

func (b *MockBloomFilter) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.keys = make(map[string]struct{})
}

var _ BloomFilter = (*MockBloomFilter)(nil)
