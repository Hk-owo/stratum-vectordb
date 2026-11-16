package sync

import (
	"context"
	"errors"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

// localVersionServer answers the two presence RPCs, so the client wrappers can be
// checked without a cluster: what matters is that a dial or RPC failure is
// returned as an error and never as "this peer does not hold the version".
type localVersionServer struct {
	pb.UnimplementedDataSyncServiceServer
	version  int64
	present  bool
	err      error
	presence int
	queries  int
}

func (s *localVersionServer) LocalVersion(context.Context, *pb.LocalVersionRequest) (*pb.LocalVersionResponse, error) {
	s.queries++
	if s.err != nil {
		return nil, s.err
	}
	return &pb.LocalVersionResponse{Version: s.version}, nil
}

func (s *localVersionServer) VersionPresence(context.Context, *pb.VersionPresenceRequest) (*pb.VersionPresenceResponse, error) {
	s.presence++
	if s.err != nil {
		return nil, s.err
	}
	return &pb.VersionPresenceResponse{Present: s.present}, nil
}

func TestLocalVersionQuerier_AsksThePeer(t *testing.T) {
	ctx := context.Background()
	server := &localVersionServer{version: 12}
	querier := NewLocalVersionQuerier(PresenceCheckerConfig{Dial: migrationBufDial(t, server)})
	got, err := querier.LocalVersionOf(ctx, "s1", "kb")
	if err != nil || got != 12 {
		t.Fatalf("LocalVersionOf = %d, %v; want 12", got, err)
	}
	if server.queries != 1 {
		t.Errorf("queries = %d, want 1", server.queries)
	}

	// An unreachable peer is an error, not version 0: the caller uses this answer
	// to pick a backfill source, and 0 would look like "nothing to fetch".
	dialErr := errors.New("no route")
	blind := NewLocalVersionQuerier(PresenceCheckerConfig{Dial: func(context.Context, string) (*grpc.ClientConn, error) { return nil, dialErr }})
	if _, err := blind.LocalVersionOf(ctx, "s1", "kb"); !errors.Is(err, dialErr) {
		t.Errorf("dial failure = %v, want %v", err, dialErr)
	}
	failing := NewLocalVersionQuerier(PresenceCheckerConfig{Dial: migrationBufDial(t, &localVersionServer{err: errors.New("rpc refused")})})
	if _, err := failing.LocalVersionOf(ctx, "s1", "kb"); err == nil {
		t.Error("an RPC failure was reported as a version number")
	}
}

func TestPresenceChecker_HasVersion(t *testing.T) {
	ctx := context.Background()
	server := &localVersionServer{present: true}
	checker := NewPresenceChecker(PresenceCheckerConfig{Dial: migrationBufDial(t, server)})
	present, err := checker.HasVersion(ctx, "s1", "kb", 3)
	if err != nil || !present {
		t.Fatalf("HasVersion = %v, %v; want true", present, err)
	}

	absent := NewPresenceChecker(PresenceCheckerConfig{Dial: migrationBufDial(t, &localVersionServer{})})
	if present, err := absent.HasVersion(ctx, "s1", "kb", 3); err != nil || present {
		t.Errorf("absent version = %v, %v; want false, nil", present, err)
	}

	dialErr := errors.New("no route")
	blind := NewPresenceChecker(PresenceCheckerConfig{Dial: func(context.Context, string) (*grpc.ClientConn, error) { return nil, dialErr }})
	if _, err := blind.HasVersion(ctx, "s1", "kb", 3); !errors.Is(err, dialErr) {
		t.Errorf("dial failure = %v, want %v", err, dialErr)
	}
	// "Unreachable" must stay distinguishable from "does not hold it": the caller
	// treats those two very differently.
	unreachable := NewPresenceChecker(PresenceCheckerConfig{Dial: migrationBufDial(t, &localVersionServer{err: errors.New("rpc refused")})})
	if present, err := unreachable.HasVersion(ctx, "s1", "kb", 3); err == nil || present {
		t.Errorf("RPC failure = %v, %v; want an error and false", present, err)
	}
}

// The inbound fence RPC is the only way a node takes itself out of service, so it
// is the last place that may accept a task the directory does not describe.
func TestFenceKBReplica_RefusesTasksTheDirectoryDoesNotDescribe(t *testing.T) {
	ctx := context.Background()
	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := func(t *testing.T) storage.Directory {
		t.Helper()
		d := storage.NewDirectory()
		if err := d.RegisterNodes(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}}); err != nil {
			t.Fatal(err)
		}
		p := storage.Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"}
		d.Placements["kb"] = p
		d.Migrations = map[string]storage.Migration{
			"move": {Request: storage.MigrationRequest{OperationID: "move", KBID: "kb", ExpectedEpoch: 1, FromID: 2, ToID: 3}, Old: p.Clone(), Phase: storage.MigrationFencing},
		}
		return d
	}
	request := func() *pb.FenceKBReplicaRequest {
		return &pb.FenceKBReplicaRequest{KnowledgeBaseId: "kb", OperationId: "move", ExpectedEpoch: 1}
	}

	if _, err := (&PushHandler{}).FenceKBReplica(ctx, &pb.FenceKBReplicaRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty request = %v, want InvalidArgument", err)
	}
	if _, err := (&PushHandler{}).FenceKBReplica(ctx, request()); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("unwired handler = %v, want FailedPrecondition", err)
	}

	unreadable := directoryReader{err: serrors.ErrNoLeaderYet}
	if _, err := (&PushHandler{directory: unreadable, executionGate: gate, nodeID: 2}).FenceKBReplica(ctx, request()); err == nil {
		t.Error("an unreadable directory was treated as a valid task")
	}

	cases := []struct {
		name string
		dir  func(t *testing.T) storage.Directory
		node int64
		want codes.Code
	}{
		{
			name: "unknown operation",
			dir: func(t *testing.T) storage.Directory {
				d := base(t)
				d.Migrations = map[string]storage.Migration{}
				return d
			},
			node: 2,
			want: codes.NotFound,
		},
		{
			name: "another knowledge base",
			dir: func(t *testing.T) storage.Directory {
				d := base(t)
				m := d.Migrations["move"]
				m.Request.KBID = "other"
				d.Migrations["move"] = m
				return d
			},
			node: 2,
			want: codes.FailedPrecondition,
		},
		{
			name: "task already staged",
			dir: func(t *testing.T) storage.Directory {
				d := base(t)
				m := d.Migrations["move"]
				m.Phase = storage.MigrationStaged
				d.Migrations["move"] = m
				return d
			},
			node: 2,
			want: codes.FailedPrecondition,
		},
		{
			name: "stale placement epoch",
			dir: func(t *testing.T) storage.Directory {
				d := base(t)
				p := d.Placements["kb"]
				p.Epoch = 4
				d.Placements["kb"] = p
				return d
			},
			node: 2,
			want: codes.Aborted,
		},
		{
			name: "node outside the fence set",
			dir:  base,
			node: 9,
			want: codes.FailedPrecondition,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &PushHandler{directory: directoryReader{directory: tc.dir(t)}, executionGate: gate, nodeID: tc.node}
			_, err := h.FenceKBReplica(ctx, request())
			if status.Code(err) != tc.want {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}

	member := &PushHandler{directory: directoryReader{directory: base(t)}, executionGate: gate, nodeID: 2}
	resp, err := member.FenceKBReplica(ctx, request())
	if err != nil {
		t.Fatalf("fence: %v", err)
	}
	if resp.GetNodeId() != 2 || resp.GetOperationId() != "move" || resp.GetFencedEpoch() != 1 {
		t.Errorf("response = %+v", resp)
	}
	// The fence is now durable: an ordinary write at that epoch is refused.
	if _, _, err := gate.Enter(ctx, "kb", 1); !errors.Is(err, serrors.ErrKBWritePaused) {
		t.Errorf("write after fence = %v, want ErrKBWritePaused", err)
	}

	// A node that has no gate cannot fence at all.
	if _, err := (&PushHandler{directory: directoryReader{directory: base(t)}, nodeID: 2}).FenceKBReplica(ctx, request()); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("handler without a gate = %v, want FailedPrecondition", err)
	}
}

// directoryReader answers StorageDirectory with a fixed snapshot, optionally
// failing, so the handlers can be exercised without a Raft node.
type directoryReader struct {
	directory storage.Directory
	err       error
}

func (r directoryReader) StorageDirectory(context.Context) (storage.Directory, error) {
	return r.directory.Clone(), r.err
}

func (r directoryReader) GetKBPlacement(context.Context, string) (storage.Snapshot, error) {
	return storage.Snapshot{}, r.err
}

// The index-fetch gate is a capacity limit, so n <= 0 has to mean "unlimited" and
// not "a channel of capacity zero", which would block every fetch forever.
func TestNewFetchSem(t *testing.T) {
	if newFetchSem(0) != nil || newFetchSem(-1) != nil {
		t.Error("a non-positive bound must be unlimited")
	}
	sem := newFetchSem(2)
	if cap(sem) != 2 {
		t.Errorf("capacity = %d, want 2", cap(sem))
	}
	h := NewNodeHandler(nil, nil)
	WithMaxConcurrentIndexFetch(3)(h)
	if cap(h.fetchSem) != 3 {
		t.Errorf("wired capacity = %d, want 3", cap(h.fetchSem))
	}
	WithMaxConcurrentIndexFetch(0)(h)
	if h.fetchSem != nil {
		t.Error("a zero bound left a gate in place")
	}
}

func TestWithMigrationCleanup_WiresTheDropper(t *testing.T) {
	h := NewNodeHandler(nil, nil)
	dropper := &kbDropper{}
	WithMigrationCleanup(dropper)(h)
	if h.cleanup == nil {
		t.Error("cleanup hook was not wired")
	}
}

// kbDropper is the migration-cleanup hook the handler calls when a migrated KB's
// old copy has to go.
type kbDropper struct{ dropped []string }

func (d *kbDropper) DropKnowledgeBase(_ context.Context, kbID string) error {
	d.dropped = append(d.dropped, kbID)
	return nil
}

// The target's import entrance is the only way a non-member writes here, so a
// handler without a gate, or a malformed request, is refused before the gate is
// consulted.
func TestEnterMigrationImport_RequiresAGateAndARequest(t *testing.T) {
	ctx := context.Background()
	if _, _, err := (*PushHandler)(nil).enterMigrationImport(ctx, "kb", 1, "move"); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("nil handler = %v", err)
	}
	if _, _, err := NewPushHandler(nil, 3).enterMigrationImport(ctx, "kb", 1, "move"); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("no gate = %v", err)
	}

	gate, err := storage.NewExecutionGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler := NewPushHandler(nil, 3)
	handler.executionGate = gate
	if _, _, err := handler.enterMigrationImport(ctx, "", 1, "move"); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Errorf("no knowledge base = %v", err)
	}
	if _, _, err := handler.enterMigrationImport(ctx, "kb", 0, "move"); !errors.Is(err, serrors.ErrInvalidArgument) {
		t.Errorf("no epoch = %v", err)
	}
	// Without a durable fence for this exact task the import is refused.
	if _, _, err := handler.enterMigrationImport(ctx, "kb", 1, "move"); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("unfenced KB = %v, want ErrMigrationConflict", err)
	}
	if err := gate.Fence(ctx, "kb", 1, "move"); err != nil {
		t.Fatal(err)
	}
	if _, release, err := handler.enterMigrationImport(ctx, "kb", 1, "move"); err != nil {
		t.Errorf("fenced task = %v", err)
	} else {
		release()
	}
}

// The index fetcher's default dial is the ordinary insecure one; the injected form
// exists so tests never touch a port.
func TestIndexFetcher_DialIsInjectable(t *testing.T) {
	sentinel := errors.New("no route")
	fetcher := NewIndexFetcher()
	fetcher.Dial = func(context.Context, string) (*grpc.ClientConn, error) { return nil, sentinel }
	if _, _, err := fetcher.FetchIndex(context.Background(), "s1", "kb", 1); !errors.Is(err, sentinel) {
		t.Errorf("injected dial = %v, want %v", err, sentinel)
	}
}

// A staging directory the process cannot read cannot be prepared: sweeping an
// unknown layout is how a live artifact gets deleted by mistake.
func TestPrepareDirectory_RefusesAnUnreadableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits do not bind root")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if err := (&migrationStager{dir: dir}).prepareDirectory(); err == nil {
		t.Error("an unreadable staging directory was prepared")
	}
}
