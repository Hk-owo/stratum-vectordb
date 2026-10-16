package index

import (
	"sync/atomic"
	"testing"
	"time"
)

// Concurrent reads of ONE version share a single pass over disk.
//
// §5.8: N leaves pulling the same artifact at once is the ordinary shape of a
// cold replica catching up. Before this merge the source read the pair once per
// caller, so N whole artifacts were resident at the same time (v14 §10.3 recorded
// that peak as unmeasured).
func TestIndexManager_ConcurrentReadsOfOneArtifactReadItOnce(t *testing.T) {
	im := NewIndexManager(IndexManagerConfig{IndexDataDir: t.TempDir(), LRUCapacity: 4})

	var reads int32
	started := make(chan struct{})
	gate := make(chan struct{})
	im.readFile = func(string) ([]byte, error) {
		if atomic.AddInt32(&reads, 1) == 1 {
			close(started)
		}
		<-gate
		return []byte("artifact"), nil
	}

	const callers = 8
	type result struct {
		index, sidecar []byte
		err            error
	}
	done := make(chan result, callers)
	call := func() {
		i, s, err := im.ReadIndexFiles("kb-1", 1)
		done <- result{i, s, err}
	}

	go call()
	<-started // the first read is in flight, so its entry is published
	for i := 1; i < callers; i++ {
		go call()
	}
	// The waiters cannot finish before the gate opens, so this only has to be
	// long enough for them to reach the map and join the in-flight read.
	time.Sleep(100 * time.Millisecond)
	close(gate)

	for i := 0; i < callers; i++ {
		r := <-done
		if r.err != nil {
			t.Fatalf("ReadIndexFiles: %v", r.err)
		}
		if string(r.index) != "artifact" || string(r.sidecar) != "artifact" {
			t.Fatalf("index=%q sidecar=%q, want the artifact for every caller", r.index, r.sidecar)
		}
	}
	if got := atomic.LoadInt32(&reads); got != 2 {
		t.Fatalf("disk reads = %d, want 2 (one index + one sidecar, shared by all %d callers)", got, callers)
	}
}

// The merge is NOT a cache: a later read must hit disk again, or a receiver could
// be handed the artifact a rebuild just replaced.
func TestIndexManager_SequentialReadsAreNotCached(t *testing.T) {
	im := NewIndexManager(IndexManagerConfig{IndexDataDir: t.TempDir(), LRUCapacity: 4})

	var reads int32
	im.readFile = func(string) ([]byte, error) {
		atomic.AddInt32(&reads, 1)
		return []byte("artifact"), nil
	}

	for i := 0; i < 2; i++ {
		if _, _, err := im.ReadIndexFiles("kb-1", 1); err != nil {
			t.Fatalf("ReadIndexFiles: %v", err)
		}
	}
	if got := atomic.LoadInt32(&reads); got != 4 {
		t.Fatalf("disk reads = %d, want 4 (two calls x index+sidecar); the merge must not cache", got)
	}
}
