package coordinator

import (
	"context"
	"errors"
	"testing"

	"stratum/internal/types"
)

// reclaimStepLog records the order the delete flow's steps ran in, so a case can
// pin that the chunk reclaim happens while the version's document list still
// names the candidates.
type reclaimStepLog struct{ steps []string }

func (l *reclaimStepLog) add(step string) { l.steps = append(l.steps, step) }

// stubVersionChunkReclaimer is a VersionChunkReclaimer double: it logs its call
// and answers with a fixed error.
type stubVersionChunkReclaimer struct {
	log   *reclaimStepLog
	err   error
	calls []int64
}

func (s *stubVersionChunkReclaimer) ReclaimVersionChunks(_ context.Context, _ string, versionID int64) error {
	s.log.add("reclaim-chunks")
	s.calls = append(s.calls, versionID)
	return s.err
}

var _ VersionChunkReclaimer = (*stubVersionChunkReclaimer)(nil)

// loggedVersionDocList logs DeleteByVersion, the step that destroys the
// candidate set.
type loggedVersionDocList struct {
	*testVersionDocList
	log     *reclaimStepLog
	deleted []int64
}

func (v *loggedVersionDocList) DeleteByVersion(ctx context.Context, kbID string, versionID int64) error {
	v.log.add("versiondoc-delete")
	v.deleted = append(v.deleted, versionID)
	return v.testVersionDocList.DeleteByVersion(ctx, kbID, versionID)
}

// TestLocalVersionDropper_ReclaimsChunksBeforeDroppingTheDocList: the reclaim
// reads the version's document set to find its chunks, so it must run before the
// step that removes that set — otherwise the delete flow itself would hide the
// candidates from the only path that reclaims them.
func TestLocalVersionDropper_ReclaimsChunksBeforeDroppingTheDocList(t *testing.T) {
	ctx := context.Background()
	log := &reclaimStepLog{}
	vd := &loggedVersionDocList{testVersionDocList: newTestVersionDocList(), log: log}
	reclaimer := &stubVersionChunkReclaimer{log: log}

	dropper := LocalVersionDropper{
		IndexManager:   newTestIndexManager(),
		DocStore:       newTestDocStore(),
		VersionDocList: vd,
		Metadata: &gcTestRaftNode{
			kbs:      []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
			versions: map[string][]types.VersionMeta{"kb-1": {{VersionID: 9, KBID: "kb-1"}}},
		},
		ChunkReclaimer: reclaimer,
	}
	if err := dropper.DropVersion(ctx, "kb-1", 7); err != nil {
		t.Fatalf("DropVersion: %v", err)
	}
	if len(reclaimer.calls) != 1 || reclaimer.calls[0] != 7 {
		t.Errorf("reclaim calls = %v, want [7]", reclaimer.calls)
	}
	if len(log.steps) < 2 || log.steps[0] != "reclaim-chunks" || log.steps[1] != "versiondoc-delete" {
		t.Errorf("step order = %v, want the chunk reclaim BEFORE the document list is dropped", log.steps)
	}
}

// TestLocalVersionDropper_KeepsTheDocListWhenTheReclaimFails: a failed reclaim
// must leave the candidate set in place, or the retry (the delete flow's backoff,
// or the tombstone's PENDING entry) would run against an empty version.
func TestLocalVersionDropper_KeepsTheDocListWhenTheReclaimFails(t *testing.T) {
	ctx := context.Background()
	log := &reclaimStepLog{}
	vd := &loggedVersionDocList{testVersionDocList: newTestVersionDocList(), log: log}

	dropper := LocalVersionDropper{
		IndexManager:   newTestIndexManager(),
		DocStore:       newTestDocStore(),
		VersionDocList: vd,
		Metadata: &gcTestRaftNode{
			kbs:      []types.KnowledgeBaseMeta{{KBID: "kb-1"}},
			versions: map[string][]types.VersionMeta{"kb-1": {{VersionID: 9, KBID: "kb-1"}}},
		},
		ChunkReclaimer: &stubVersionChunkReclaimer{log: log, err: errors.New("vecstore: unavailable")},
	}
	if err := dropper.DropVersion(ctx, "kb-1", 7); err == nil {
		t.Fatal("a failed chunk reclaim must be reported, not treated as a finished deletion")
	}
	if len(vd.deleted) != 0 {
		t.Errorf("the document list was dropped despite the failed reclaim: deleted=%v", vd.deleted)
	}
}

// TestDropVersionStorage_ReclaimsChunksFirst pins the same two properties on the
// receiving node's path (PushHandler.DeleteVersionData →
// WriteCoordinatorImpl.DropVersionStorage), which is where a split deployment's
// chunk data actually is.
func TestDropVersionStorage_ReclaimsChunksFirst(t *testing.T) {
	ctx := context.Background()

	t.Run("order", func(t *testing.T) {
		log := &reclaimStepLog{}
		vd := &loggedVersionDocList{testVersionDocList: newTestVersionDocList(), log: log}
		w := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
			VersionDocList: vd,
			ChunkReclaimer: &stubVersionChunkReclaimer{log: log},
		})
		if err := w.DropVersionStorage(ctx, "kb-1", 7); err != nil {
			t.Fatalf("DropVersionStorage: %v", err)
		}
		if len(log.steps) < 2 || log.steps[0] != "reclaim-chunks" || log.steps[1] != "versiondoc-delete" {
			t.Errorf("step order = %v, want the chunk reclaim BEFORE the document list is dropped", log.steps)
		}
	})

	t.Run("failure keeps the candidate set", func(t *testing.T) {
		log := &reclaimStepLog{}
		vd := &loggedVersionDocList{testVersionDocList: newTestVersionDocList(), log: log}
		w := NewWriteCoordinatorImpl(WriteCoordinatorConfig{
			VersionDocList: vd,
			ChunkReclaimer: &stubVersionChunkReclaimer{log: log, err: errors.New("vecstore: unavailable")},
		})
		if err := w.DropVersionStorage(ctx, "kb-1", 7); err == nil {
			t.Fatal("DropVersionStorage must report a failed chunk reclaim")
		}
		if len(vd.deleted) != 0 {
			t.Errorf("the document list was dropped despite the failed reclaim: deleted=%v", vd.deleted)
		}
	})
}

// TestLocalVersionDropper_EndToEnd_ReclaimsTheChunksOfTheDeletedVersion runs the
// real write path and then the real dropper: a version written with content has
// its chunk vector and mapping reclaimed when the VERSION is deleted (not the
// document), which is the hand-off the event-driven reclaim exists for — no
// periodic sweep involved.
func TestLocalVersionDropper_EndToEnd_ReclaimsTheChunksOfTheDeletedVersion(t *testing.T) {
	ctx := context.Background()
	const (
		kbID    = "kb-e2e-version-chunks"
		content = "a document body that becomes exactly one chunk"
	)

	h := newGCMutexHarness(t)
	cs := newGCChunkStore()
	coord, _, rn := h.newNode(t, kbID, cs)

	v1, err := coord.Execute(ctx, kbID, 0, []types.DocChange{
		{Op: types.ChangeOpAdd, DocID: "doc-1", Content: content},
	}, "")
	if err != nil {
		t.Fatalf("write v1: %v", err)
	}
	chunkIDs, err := h.cdm.ListChunkIDs(ctx, kbID)
	if err != nil {
		t.Fatalf("ListChunkIDs: %v", err)
	}
	if len(chunkIDs) != 1 {
		t.Fatalf("want exactly 1 chunk for %q, got %d (%v)", content, len(chunkIDs), chunkIDs)
	}
	chunkID := chunkIDs[0]
	if ok, err := cs.Exists(ctx, kbID, chunkID); err != nil || !ok {
		t.Fatalf("precondition: chunk should exist; exists=%v err=%v", ok, err)
	}

	gc := NewChunkGarbageCollectorImpl(ChunkGarbageCollectorConfig{
		SweepIntervalSec: 1,
		Locks:            h.locks,
		RaftNode:         rn,
		ChunkDocMapper:   h.cdm,
		DocStore:         h.ds,
		ChunkStore:       cs,
		VersionDocList:   h.vdl,
	})
	dropper := LocalVersionDropper{
		IndexManager:   h.index,
		DocStore:       h.ds,
		VersionDocList: h.vdl,
		Metadata:       rn,
		ChunkReclaimer: gc,
	}

	// The delete flow marks the version first, then reclaims it.
	if _, err := rn.ProposeMarkVersionDeleting(ctx, kbID, v1, types.VersionDeleteSingle); err != nil {
		t.Fatalf("mark v%d deleting: %v", v1, err)
	}
	if err := dropper.DropVersion(ctx, kbID, v1); err != nil {
		t.Fatalf("DropVersion: %v", err)
	}

	if ok, err := cs.Exists(ctx, kbID, chunkID); err != nil || ok {
		t.Errorf("the deleted version's chunk vector survived; exists=%v err=%v", ok, err)
	}
	if docs, err := h.cdm.ListDocIDs(ctx, kbID, chunkID); err != nil || len(docs) != 0 {
		t.Errorf("its mapping survived; docs=%v err=%v", docs, err)
	}
}
