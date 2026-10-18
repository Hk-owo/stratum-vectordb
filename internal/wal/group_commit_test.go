package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Group commit is about one number: how many fsyncs N concurrent writers cost.
// These tests pin the two properties that matter — the fsyncs really are shared, and
// a shared fsync that fails cannot leave anybody believing their record is durable.

// openTestWAL opens a fresh log in a temporary directory.
func openTestWAL(t *testing.T) *FileWAL {
	t.Helper()
	w, err := NewFileWAL(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	return w
}

// waitFor polls cond for up to ~5s. Used to wait for other goroutines to reach a
// point the test cannot observe directly (here: "the writers have appended, and are
// now queued on the fsync").
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFileWAL_GroupsConcurrentFsyncs is the reason group commit exists: several
// knowledge bases written at once share fsyncs instead of paying one each.
//
// The schedule is arranged so the sharing is unavoidable rather than lucky. Writer
// A leads a round and parks inside its fsync; while it is parked, N more writers
// append. Their bytes land after A's round read how far the log had got, so A
// cannot cover them — they have to share the NEXT round between themselves. The
// expectation is therefore two fsyncs for N+1 writers (it was N+1 before).
//
// 收益的量化: per-write fsync would cost writers+1 rounds on this same schedule.
func TestFileWAL_GroupsConcurrentFsyncs(t *testing.T) {
	w := openTestWAL(t)
	defer w.Close()

	ctx := context.Background()

	var (
		mu       sync.Mutex
		syncs    int
		inFlight = make(chan struct{})
		release  = make(chan struct{})
	)
	w.syncFile = func(f *os.File) error {
		mu.Lock()
		syncs++
		first := syncs == 1
		mu.Unlock()
		if first {
			close(inFlight)
			<-release
		}
		return f.Sync()
	}

	// Writer A leads the first round and stays inside it.
	aDone := make(chan error, 1)
	go func() { aDone <- w.WriteCommit(ctx, 1) }()
	<-inFlight

	// While A is parked, the others append. Each COMMIT payload is 8 bytes, so the
	// log's end offset tells us exactly when all of them have been written — which is
	// the moment the test can let A's fsync finish.
	const writers = 8
	recordLen := int64(recordHeaderLen + 8 + recordCRCLen)
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- w.WriteCommit(ctx, int64(100+i))
		}(i)
	}
	waitFor(t, "the writers to append", func() bool {
		return w.appended.Load() >= int64(writers+1)*recordLen
	})
	close(release)

	if err := <-aDone; err != nil {
		t.Fatalf("writer A: %v", err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent writer: %v", err)
		}
	}

	mu.Lock()
	got := syncs
	mu.Unlock()
	if got > 2 {
		t.Fatalf("%d writers + 1 cost %d fsyncs; group commit should need at most 2 "+
			"(one round was already in flight when the others appended)", writers, got)
	}

	// The sharing must not have cost the guarantee: every record is in the log, and a
	// fresh open reads them all back.
	w2, err := NewFileWAL(w.path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	for _, versionID := range []int64{1, 100, 101, 102, 103, 104, 105, 106, 107} {
		if !w2.committedVersions[versionID] {
			t.Fatalf("version %d is missing from the reopened log", versionID)
		}
	}
}

// TestFileWAL_FailedSyncRoundFailsClosed pins what a shared fsync that fails means.
//
// With one writer per fsync, a failure could be answered by rolling that writer's
// record back. With a round covering several, there is no way to tell which of them
// reached the device — so the log has to stop accepting writes, and no caller may be
// told its record is durable. That includes the caller's own retry: an idempotent
// repeat must fail too, or the failure would look like success on the second attempt.
func TestFileWAL_FailedSyncRoundFailsClosed(t *testing.T) {
	w := openTestWAL(t)
	defer w.Close()

	ctx := context.Background()
	syncErr := errors.New("simulated device failure")
	w.syncFile = func(*os.File) error { return syncErr }

	if err := w.WriteCommit(ctx, 7); !errors.Is(err, syncErr) {
		t.Fatalf("WriteCommit on a failing fsync = %v, want the fsync error", err)
	}
	if err := w.WriteCommit(ctx, 7); err == nil {
		t.Fatal("the idempotent repeat of a failed write reported success")
	}
	if err := w.WriteCommit(ctx, 8); err == nil {
		t.Fatal("a log whose fsync failed accepted another record")
	}
	if err := w.WriteCursor(ctx, "kb-1", 1); err == nil {
		t.Fatal("a log whose fsync failed accepted a cursor record")
	}
}

// TestFileWAL_WritersShareFsyncsAcrossACompaction checks the two mechanisms against
// each other. Compaction replaces the file under the writers' feet, and what a
// writer is waiting for is a *record* being durable — not a byte offset in a file
// that no longer exists. Run with -race: this is also the race this design has to
// survive.
func TestFileWAL_WritersShareFsyncsAcrossACompaction(t *testing.T) {
	w := openTestWAL(t)
	defer w.Close()

	ctx := context.Background()
	// A slow fsync is what makes the writers overlap with the compaction instead of
	// finishing first.
	w.syncFile = func(f *os.File) error {
		time.Sleep(time.Millisecond)
		return f.Sync()
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := w.WriteCommit(ctx, int64(i+1)); err != nil {
				t.Errorf("WriteCommit(%d): %v", i+1, err)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Nothing is reclaimable here (keepThrough is empty), so this is a pure
		// rewrite — which is exactly the file swap the test needs.
		for i := 0; i < 10; i++ {
			if err := w.Compact(ctx, map[string]int64{}); err != nil {
				t.Errorf("Compact: %v", err)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("writers and compaction deadlocked")
	}

	// Every COMMIT survived the swaps.
	w2, err := NewFileWAL(w.path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	for i := 1; i <= 16; i++ {
		if !w2.committedVersions[int64(i)] {
			t.Fatalf("version %d did not survive the concurrent compactions", i)
		}
	}
}
