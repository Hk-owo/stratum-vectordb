package sync

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "stratum/api/proto/stratum"
)

// regKey identifies one (§8.5) announcement: a version of a knowledge base.
type regKey struct {
	kbID      string
	versionID int64
}

// recordingSourceRegistry stands in for plane.DataSourceRegistry — the §8.5
// announcement sink — and records what the receive side stored. It is a local
// fake on purpose: this file is about the *wiring* (does the writer's address
// travel from the broadcaster through proto into the table?), while the table's
// own behaviour is covered in internal/plane. It also keeps this package's
// tests out of an import cycle: plane imports sync.
type recordingSourceRegistry struct {
	mu        sync.Mutex
	entries   map[regKey]string
	forgotten []regKey
}

func newRecordingSourceRegistry() *recordingSourceRegistry {
	return &recordingSourceRegistry{entries: map[regKey]string{}}
}

func (r *recordingSourceRegistry) Register(kbID string, versionID int64, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[regKey{kbID, versionID}] = addr
}

func (r *recordingSourceRegistry) ForgetVersion(kbID string, versionID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := regKey{kbID, versionID}
	delete(r.entries, key)
	r.forgotten = append(r.forgotten, key)
}

func (r *recordingSourceRegistry) lookup(kbID string, versionID int64) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	addr, ok := r.entries[regKey{kbID, versionID}]
	return addr, ok
}

func (r *recordingSourceRegistry) registrations() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// recordingWatcher stands in for the §7.3 takeover hook.
type recordingWatcher struct {
	mu        sync.Mutex
	confirmed []regKey
}

func (w *recordingWatcher) WatchVersionWrite(kbID string, versionID int64) {}

func (w *recordingWatcher) ConfirmVersionWrite(kbID string, versionID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.confirmed = append(w.confirmed, regKey{kbID, versionID})
}

func (w *recordingWatcher) confirmedVersion(kbID string, versionID int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, c := range w.confirmed {
		if c == (regKey{kbID, versionID}) {
			return true
		}
	}
	return false
}

// recordingDropper stands in for the §10.6 cleanup target.
type recordingDropper struct {
	mu      sync.Mutex
	dropped []regKey
}

func (d *recordingDropper) DropVersionStorage(_ context.Context, kbID string, versionID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dropped = append(d.dropped, regKey{kbID, versionID})
	return nil
}

// TestConfirmVersionWrite_CarriesWriterAddressIntoTheAnnouncementTable is the
// wiring test for §8.5's announcement channel, end to end over real gRPC: the
// broadcaster puts the writer's address on the wire, and the receiving
// PushHandler records it, so a later puller learns where the version's data
// lives without asking the leader. The §7.3 takeover stand-down must still
// happen on the same message — the announcement rides the confirmation rather
// than replacing it.
func TestConfirmVersionWrite_CarriesWriterAddressIntoTheAnnouncementTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reg := newRecordingSourceRegistry()
	watcher := &recordingWatcher{}
	_, _, addr := startPushServer(t, 7,
		WithDataSourceRegistry(reg),
		WithVersionWriteWatcher(watcher),
	)

	broadcaster := NewConfirmBroadcaster(PresenceCheckerConfig{})
	if err := broadcaster.ConfirmVersionWrite(ctx, addr, "kb-1", 42, "writer:7001", false); err != nil {
		t.Fatalf("ConfirmVersionWrite: %v", err)
	}

	got, ok := reg.lookup("kb-1", 42)
	if !ok {
		t.Fatal("the receiving node must record the writer's announcement (§8.5)")
	}
	if got != "writer:7001" {
		t.Fatalf("recorded source = %q, want the writer's own address %q", got, "writer:7001")
	}
	if !watcher.confirmedVersion("kb-1", 42) {
		t.Error("the same message must still stand down the takeover timer (§7.3)")
	}
}

// A node that is not configured to announce itself sends an empty address
// (main.go's SelfDataSyncAddr is empty when unset). Recording that would turn
// "nobody said anything" into "the source is nowhere", so the receive side must
// treat it as no announcement.
func TestConfirmVersionWrite_EmptyWriterAddressIsNotRecorded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reg := newRecordingSourceRegistry()
	_, _, addr := startPushServer(t, 7, WithDataSourceRegistry(reg))

	broadcaster := NewConfirmBroadcaster(PresenceCheckerConfig{})
	if err := broadcaster.ConfirmVersionWrite(ctx, addr, "kb-1", 42, "", false); err != nil {
		t.Fatalf("ConfirmVersionWrite: %v", err)
	}

	if n := reg.registrations(); n != 0 {
		t.Fatalf("an empty writer address must not be recorded, table has %d entries", n)
	}
}

// Reclaiming a version's data must withdraw its announcement: otherwise a later
// puller is pointed at a node that no longer holds the version, and the pull
// fails against a source that looks authoritative.
func TestDeleteVersionData_WithdrawsTheAnnouncement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reg := newRecordingSourceRegistry()
	dropper := &recordingDropper{}
	_, _, addr := startPushServer(t, 7,
		WithDataSourceRegistry(reg),
		WithVersionDataDropper(dropper),
	)

	broadcaster := NewConfirmBroadcaster(PresenceCheckerConfig{})
	// empty=false: this test is about the §8.5 announcement travelling through
	// proto into the table, not about the empty-version flag.
	if err := broadcaster.ConfirmVersionWrite(ctx, addr, "kb-1", 42, "writer:7001", false); err != nil {
		t.Fatalf("ConfirmVersionWrite: %v", err)
	}
	if _, ok := reg.lookup("kb-1", 42); !ok {
		t.Fatal("precondition: the announcement must be recorded before it is withdrawn")
	}

	cleaner := NewVersionDataCleaner(PresenceCheckerConfig{})
	if err := cleaner.DeleteVersionData(ctx, addr, "kb-1", 42, "test reclaim"); err != nil {
		t.Fatalf("DeleteVersionData: %v", err)
	}

	if _, ok := reg.lookup("kb-1", 42); ok {
		t.Error("after its data is reclaimed, the version's announcement must be withdrawn")
	}
	if len(dropper.dropped) != 1 {
		t.Errorf("dropper calls = %v, want exactly one (the data must actually be reclaimed)", dropper.dropped)
	}
}

// TestPushHandler_ConfirmVersionWriteMovesTheCursorForAnEmptyVersion pins the
// second half of the A4 fix: a version with no document changes is never fanned
// out, so the §8.5 announcement is the only cue a non-coordinating replica gets
// that it exists. The coordinator sends the fact (empty_version) instead of
// leaving the replica to discover it — discovering it would mean fetching, and
// there is nothing to fetch. Without this the replica answers "version 0" to the
// station's freshness check (§9.3(2)) for a version it in fact holds.
func TestPushHandler_ConfirmVersionWriteMovesTheCursorForAnEmptyVersion(t *testing.T) {
	adv := &recordingAdvancer{}
	h := NewPushHandler(nil, 7, WithLocalVersionAdvancer(adv))

	if _, err := h.ConfirmVersionWrite(context.Background(), &pb.ConfirmVersionWriteRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       9,
		SourceAddr:      "writer:7000",
		EmptyVersion:    true,
	}); err != nil {
		t.Fatalf("ConfirmVersionWrite: %v", err)
	}

	got := adv.got()
	if len(got) != 1 || got[0] != "kb-1/9" {
		t.Fatalf("cursor marks = %v, want exactly kb-1/9", got)
	}
}

// TestPushHandler_ConfirmVersionWriteLeavesTheCursorForAVersionWithData: the flag
// is not a blanket "mark it held". A version WITH documents arrives through the
// fan-out, and marking it here would claim a version whose records this node may
// never have received — exactly the staleness lie the cursor exists to prevent.
func TestPushHandler_ConfirmVersionWriteLeavesTheCursorForAVersionWithData(t *testing.T) {
	adv := &recordingAdvancer{}
	h := NewPushHandler(nil, 7, WithLocalVersionAdvancer(adv))

	if _, err := h.ConfirmVersionWrite(context.Background(), &pb.ConfirmVersionWriteRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       9,
		SourceAddr:      "writer:7000",
		EmptyVersion:    false,
	}); err != nil {
		t.Fatalf("ConfirmVersionWrite: %v", err)
	}

	if got := adv.got(); len(got) != 0 {
		t.Fatalf("cursor marks = %v, want none — a version with records is the fan-out's business", got)
	}
}

// TestPushHandler_ConfirmVersionWriteWithoutAdvancerIsSilent: the hook is
// optional, and a node that wired none must still answer the confirmation —
// the coordinator is counting acknowledgements.
func TestPushHandler_ConfirmVersionWriteWithoutAdvancerIsSilent(t *testing.T) {
	h := NewPushHandler(nil, 7)

	resp, err := h.ConfirmVersionWrite(context.Background(), &pb.ConfirmVersionWriteRequest{
		KnowledgeBaseId: "kb-1",
		VersionId:       9,
		EmptyVersion:    true,
	})
	if err != nil {
		t.Fatalf("ConfirmVersionWrite without an advancer: %v", err)
	}
	if resp.GetNodeId() != 7 {
		t.Fatalf("response node = %d, want 7", resp.GetNodeId())
	}
}
