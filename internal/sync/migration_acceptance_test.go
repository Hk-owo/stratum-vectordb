package sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"stratum/internal/docstore"
	"sync"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
)

func TestMigrationInstallAfterRealTargetFence(t *testing.T) {
	f, h, g, _ := stagedInstallFixture(t)
	if err := g.Fence(context.Background(), "kb", 1, "move"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.InstallKBMigration(context.Background(), f.request); err != nil {
		t.Fatal("target's required persistent fence prevents installation:", err)
	}
}

func TestMigrationInstallRejectsDifferentCommittedPayload(t *testing.T) {
	f, h, _, _ := stagedInstallFixture(t)
	path := migrationStagePath(f.root, "kb", "move")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(data)
	var output bytes.Buffer
	digest := sha256.New()
	for {
		frame, err := readMigrationFrame(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e := frame.Entry; e != nil && e.EntryType == pb.SyncEntryType_SYNC_ENTRY_TYPE_DOC_STORE && len(e.Payload) > 1 {
			e.Payload = append([]byte{tagContent}, []byte("foreign-body")...)
		}
		if frame.Complete {
			frame.PayloadSha256 = hex.EncodeToString(digest.Sum(nil))
		}
		encoded, err := encodeMigrationFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		if !frame.Complete {
			hashMigrationFrame(digest, encoded)
		}
		if err := writeMigrationFrame(&output, encoded); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(path, output.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := h.InstallKBMigration(context.Background(), f.request); err == nil {
		t.Fatal("installed self-consistent payload different from Raft STAGED hash")
	}
	if body, err := f.target.docStore.ReadAt(context.Background(), "kb", "doc", f.versions[0]); err == nil {
		t.Fatalf("rejected payload touched ordinary store: %q", body)
	}
}

func TestMigrationInstallValidatesEntireFileBeforeMutation(t *testing.T) {
	f, h, _, _ := stagedInstallFixture(t)
	path := migrationStagePath(f.root, "kb", "move")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-1], 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := h.InstallKBMigration(context.Background(), f.request); err == nil {
		t.Fatal("accepted corrupt footer")
	}
	if body, err := f.target.docStore.ReadAt(context.Background(), "kb", "doc", f.versions[0]); err == nil {
		t.Fatalf("corrupt artifact mutated ordinary data before validation: %q", body)
	}
}

func TestMigrationServeCommittedEmptyActiveNeedsNoIndex(t *testing.T) {
	f, _, _ := installedFixture(t)
	index := newFakeIndex()
	index.never = true
	h, _ := newServeHandler(t, f, f.root, 3, newFakeCursor(), index)
	h.migration.serveWait = 20 * time.Millisecond
	receipt, err := h.PrepareKBMigrationServing(context.Background(), f.request)
	if err != nil || !receipt.GetIndexReady() || len(index.builds) != 0 {
		t.Fatal("committed empty active must serve without an index artifact", receipt, err, index.builds)
	}
}

type slowMigrationDocStore struct {
	docstore.DocStore
	delay sync.Once
}

func (s *slowMigrationDocStore) Write(ctx context.Context, kb, doc string, version int64, value []byte) error {
	s.delay.Do(func() { time.Sleep(installCheckpoint + 50*time.Millisecond) })
	return s.DocStore.Write(ctx, kb, doc, version, value)
}

func TestMigrationInstalledRetryMayCrossCheckpoint(t *testing.T) {
	f, h, _ := installedFixture(t)
	follower := h.receiver.follower
	follower.docStore = &slowMigrationDocStore{DocStore: follower.docStore}
	if _, err := h.InstallKBMigration(context.Background(), f.request); err != nil {
		t.Fatal("installed retry crossing checkpoint refused", err)
	}
}

type cancellingMigrationIndex struct {
	*fakeIndex
	cancelTask func()
}

func (i *cancellingMigrationIndex) IndexExists(ctx context.Context, kb string, v int64) (bool, error) {
	i.cancelTask()
	return true, nil
}
func TestMigrationServeDoesNotReturnLateEvidence(t *testing.T) {
	f, _, _ := installedFixture(t, 1)
	index := &cancellingMigrationIndex{fakeIndex: newFakeIndex(), cancelTask: func() {
		if err := f.metadata.ProposeCancelKBMigration(context.Background(), "move"); err != nil {
			t.Fatal(err)
		}
	}}
	h, _ := newServeHandler(t, f, f.root, 3, newFakeCursor(), index)
	if _, err := h.PrepareKBMigrationServing(context.Background(), f.request); err == nil {
		t.Fatal("cancelled task returned serving evidence")
	}
}

type delayedMigrationIndex struct {
	*fakeIndex
	readyAt time.Time
}

func (i *delayedMigrationIndex) IndexExists(context.Context, string, int64) (bool, error) {
	return time.Now().After(i.readyAt), nil
}

func TestMigrationServeIndexWaitMayCrossCheckpoint(t *testing.T) {
	f, _, _ := installedFixture(t, 1)
	i := &delayedMigrationIndex{fakeIndex: newFakeIndex(), readyAt: time.Now().Add(serveCheckpoint + 100*time.Millisecond)}
	i.never = true
	h, _ := newServeHandler(t, f, f.root, 3, newFakeCursor(), i)
	h.migration.serveWait = 8 * time.Second
	if receipt, err := h.PrepareKBMigrationServing(context.Background(), f.request); err != nil || !receipt.GetIndexReady() {
		t.Fatal("installed serving task rejected at periodic checkpoint", receipt, err)
	}
}

func TestMigrationInstallRechecksCancellationAfterGateWait(t *testing.T) {
	f, h, g, _ := stagedInstallFixture(t)
	ctx := context.Background()
	_, release, err := g.EnterImport(ctx, "kb", 1, "move")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() { _, err := h.InstallKBMigration(ctx, f.request); done <- err }()
	select {
	case err := <-done:
		t.Fatal("installation bypassed occupied import gate", err)
	case <-time.After(30 * time.Millisecond):
	}
	if err := f.metadata.ProposeCancelKBMigration(ctx, "move"); err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled waiter installed data")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter did not exit")
	}
	if body, err := f.target.docStore.ReadAt(ctx, "kb", "doc", f.versions[0]); err == nil {
		t.Fatal("cancelled waiter mutated data", string(body))
	}
}
