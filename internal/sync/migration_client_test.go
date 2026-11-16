package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"google.golang.org/grpc"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

// fakeSyncServer answers every migration RPC the fence client can send, with
// whatever the test configured. Each call is counted so a test can assert that a
// refusal happened before the network was touched.
type fakeSyncServer struct {
	pb.UnimplementedDataSyncServiceServer
	fenceResponse   *pb.FenceKBReplicaResponse
	stageResponse   *pb.MigrationStageReceipt
	installResponse *pb.MigrationInstallReceipt
	serveResponse   *pb.MigrationServeReceipt
	err             error
	calls           int
}

func (s *fakeSyncServer) FenceKBReplica(context.Context, *pb.FenceKBReplicaRequest) (*pb.FenceKBReplicaResponse, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.fenceResponse, nil
}

func (s *fakeSyncServer) StageKBMigration(context.Context, *pb.MigrationTransferRequest) (*pb.MigrationStageReceipt, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.stageResponse, nil
}

func (s *fakeSyncServer) InstallKBMigration(context.Context, *pb.MigrationTransferRequest) (*pb.MigrationInstallReceipt, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.installResponse, nil
}

func (s *fakeSyncServer) PrepareKBMigrationServing(context.Context, *pb.MigrationTransferRequest) (*pb.MigrationServeReceipt, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.serveResponse, nil
}

func (s *fakeSyncServer) CleanupStorage(context.Context, *pb.CleanupStorageRequest) (*pb.CleanupStorageResponse, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return &pb.CleanupStorageResponse{}, nil
}

func (s *fakeSyncServer) ActivateKBMigration(context.Context, *pb.MigrationTransferRequest) (*pb.MigrationServeReceipt, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return &pb.MigrationServeReceipt{}, nil
}

func fenceClient(t *testing.T, server *fakeSyncServer) *FenceClient {
	t.Helper()
	client := &FenceClient{Dial: migrationBufDial(t, server)}
	t.Cleanup(client.Close)
	return client
}

func movingTask() storage.Migration {
	return storage.Migration{
		Request: storage.MigrationRequest{OperationID: "move", KBID: "kb", ExpectedEpoch: 1, FromID: 2, ToID: 3},
		Old:     storage.Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"},
		Stage:   &storage.MigrationStageReceipt{SourceID: 1, TargetID: 3, ManifestHash: sha("stage-manifest"), PayloadHash: sha("payload")},
		Install: &storage.MigrationInstallReceipt{SourceID: 1, TargetID: 3, ManifestHash: sha("stage-manifest"), StageHash: sha("payload")},
		Phase:   storage.MigrationStaged,
	}
}

// sha is a real SHA-256 in hex: the client refuses receipts whose hashes are not
// valid digests, so a placeholder would test the wrong branch.
func sha(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}

func targetNode() storage.Node { return storage.Node{ID: 3, Addr: "s3", State: storage.NodeActive} }

// A target answers with a receipt; the client's job is to refuse one that does not
// describe the task it was asked about, because the control layer would otherwise
// record evidence for the wrong bytes.
func TestFenceClient_InstallAndServeCheckTheReceipt(t *testing.T) {
	ctx := context.Background()
	task := movingTask()

	t.Run("install without a stage receipt", func(t *testing.T) {
		server := &fakeSyncServer{}
		client := fenceClient(t, server)
		_, err := client.Install(ctx, targetNode(), storage.Migration{})
		if !errors.Is(err, serrors.ErrMigrationConflict) {
			t.Fatalf("err = %v, want ErrMigrationConflict", err)
		}
		if server.calls != 0 {
			t.Error("a task without a stage receipt still reached the network")
		}
	})

	t.Run("install receipt mismatch", func(t *testing.T) {
		cases := map[string]*pb.MigrationInstallReceipt{
			"another source":    {SourceNodeId: 2, TargetNodeId: 3, ManifestSha256: sha("stage-manifest"), StageSha256: sha("payload")},
			"another target":    {SourceNodeId: 1, TargetNodeId: 9, ManifestSha256: sha("stage-manifest"), StageSha256: sha("payload")},
			"another manifest":  {SourceNodeId: 1, TargetNodeId: 3, ManifestSha256: sha("other"), StageSha256: sha("payload")},
			"another artifact":  {SourceNodeId: 1, TargetNodeId: 3, ManifestSha256: sha("stage-manifest"), StageSha256: sha("other")},
			"hash not a sha256": {SourceNodeId: 1, TargetNodeId: 3, ManifestSha256: "manifest", StageSha256: sha("payload")},
		}
		for name, response := range cases {
			client := fenceClient(t, &fakeSyncServer{installResponse: response})
			if _, err := client.Install(ctx, targetNode(), task); !errors.Is(err, serrors.ErrMigrationConflict) {
				t.Errorf("%s: err = %v, want ErrMigrationConflict", name, err)
			}
		}
	})

	t.Run("install succeeds", func(t *testing.T) {
		want := storage.MigrationInstallReceipt{SourceID: 1, TargetID: 3, ManifestHash: sha("stage-manifest"), StageHash: sha("payload"), Versions: 1}
		client := fenceClient(t, &fakeSyncServer{installResponse: &pb.MigrationInstallReceipt{SourceNodeId: 1, TargetNodeId: 3, ManifestSha256: sha("stage-manifest"), StageSha256: sha("payload"), VersionCount: 1}})
		got, err := client.Install(ctx, targetNode(), task)
		if err != nil {
			t.Fatalf("install: %v", err)
		}
		if got != want {
			t.Errorf("receipt = %+v, want %+v", got, want)
		}
	})

	t.Run("transport failure", func(t *testing.T) {
		client := fenceClient(t, &fakeSyncServer{err: serrors.ErrStorageUnavailable})
		if _, err := client.Install(ctx, targetNode(), task); !errors.Is(err, serrors.ErrStorageUnavailable) && err == nil {
			t.Errorf("err = %v, want the transport failure", err)
		}
	})

	t.Run("serve needs both receipts", func(t *testing.T) {
		client := fenceClient(t, &fakeSyncServer{})
		if _, err := client.PrepareServing(ctx, targetNode(), storage.Migration{}); !errors.Is(err, serrors.ErrMigrationConflict) {
			t.Errorf("no receipts = %v, want ErrMigrationConflict", err)
		}
		withoutInstall := task
		withoutInstall.Install = nil
		if _, err := client.PrepareServing(ctx, targetNode(), withoutInstall); !errors.Is(err, serrors.ErrMigrationConflict) {
			t.Errorf("no install receipt = %v, want ErrMigrationConflict", err)
		}
	})

	t.Run("serve receipt mismatch", func(t *testing.T) {
		cases := map[string]*pb.MigrationServeReceipt{
			"another target":    {TargetNodeId: 9, ManifestSha256: sha("stage-manifest"), IndexReady: true},
			"hash not a sha256": {TargetNodeId: 3, ManifestSha256: "manifest", IndexReady: true},
			"index not ready":   {TargetNodeId: 3, ManifestSha256: sha("stage-manifest")},
		}
		for name, response := range cases {
			client := fenceClient(t, &fakeSyncServer{serveResponse: response})
			if _, err := client.PrepareServing(ctx, targetNode(), task); !errors.Is(err, serrors.ErrMigrationConflict) {
				t.Errorf("%s: err = %v, want ErrMigrationConflict", name, err)
			}
		}
	})

	t.Run("serve succeeds", func(t *testing.T) {
		client := fenceClient(t, &fakeSyncServer{serveResponse: &pb.MigrationServeReceipt{TargetNodeId: 3, ManifestSha256: sha("stage-manifest"), ActiveVersion: 7, IndexReady: true}})
		got, err := client.PrepareServing(ctx, targetNode(), task)
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
		if got.ActiveVersion != 7 || !got.IndexReady {
			t.Errorf("receipt = %+v", got)
		}
	})

	t.Run("activation needs the serving evidence", func(t *testing.T) {
		server := &fakeSyncServer{}
		client := fenceClient(t, server)
		if err := client.Activate(ctx, targetNode(), storage.Migration{}); !errors.Is(err, serrors.ErrMigrationConflict) {
			t.Errorf("no evidence = %v, want ErrMigrationConflict", err)
		}
		withoutServe := task
		withoutServe.Serve = nil
		if err := client.Activate(ctx, targetNode(), withoutServe); !errors.Is(err, serrors.ErrMigrationConflict) {
			t.Errorf("no serve receipt = %v, want ErrMigrationConflict", err)
		}
		if server.calls != 0 {
			t.Error("an incomplete task still reached the network")
		}
	})

	t.Run("activation succeeds", func(t *testing.T) {
		ready := task
		ready.Phase = storage.MigrationInstalled
		ready.Serve = &storage.MigrationServeReceipt{TargetID: 3, ManifestHash: sha("stage-manifest"), IndexReady: true}
		if err := fenceClient(t, &fakeSyncServer{}).Activate(ctx, targetNode(), ready); err != nil {
			t.Errorf("activate: %v", err)
		}
	})
}

// The client must also survive a peer that answers with the wrong identity, and a
// dial that never connects: both are transport-level refusals, not conflicts.
func TestFenceClient_FenceAndCleanup(t *testing.T) {
	ctx := context.Background()
	task := movingTask()

	mismatched := map[string]*pb.FenceKBReplicaResponse{
		"another node":      {NodeId: 9, OperationId: "move", FencedEpoch: 1},
		"another operation": {NodeId: 2, OperationId: "other", FencedEpoch: 1},
		"another epoch":     {NodeId: 2, OperationId: "move", FencedEpoch: 2},
	}
	for name, response := range mismatched {
		client := fenceClient(t, &fakeSyncServer{fenceResponse: response})
		if err := client.Fence(ctx, storage.Node{ID: 2, Addr: "s2"}, task); !errors.Is(err, serrors.ErrMigrationConflict) {
			t.Errorf("%s: err = %v, want ErrMigrationConflict", name, err)
		}
	}
	if err := fenceClient(t, &fakeSyncServer{fenceResponse: &pb.FenceKBReplicaResponse{NodeId: 2, OperationId: "move", FencedEpoch: 1}}).Fence(ctx, storage.Node{ID: 2, Addr: "s2"}, task); err != nil {
		t.Errorf("fence: %v", err)
	}

	if err := fenceClient(t, &fakeSyncServer{}).Cleanup(ctx, targetNode(), storage.Cleanup{ID: "c"}); err != nil {
		t.Errorf("cleanup: %v", err)
	}

	dialErr := errors.New("no route")
	failing := &FenceClient{Dial: func(context.Context, string) (*grpc.ClientConn, error) { return nil, dialErr }}
	t.Cleanup(failing.Close)
	if err := failing.Fence(ctx, targetNode(), task); !errors.Is(err, dialErr) {
		t.Errorf("fence dial failure = %v, want %v", err, dialErr)
	}
	if err := failing.Cleanup(ctx, targetNode(), storage.Cleanup{ID: "c"}); !errors.Is(err, dialErr) {
		t.Errorf("cleanup dial failure = %v, want %v", err, dialErr)
	}
	if _, err := failing.Install(ctx, targetNode(), task); !errors.Is(err, dialErr) {
		t.Errorf("install dial failure = %v, want %v", err, dialErr)
	}
	if _, err := failing.PrepareServing(ctx, targetNode(), task); !errors.Is(err, dialErr) {
		t.Errorf("serve dial failure = %v, want %v", err, dialErr)
	}
	ready := task
	ready.Serve = &storage.MigrationServeReceipt{TargetID: 3, ManifestHash: sha("stage-manifest"), IndexReady: true}
	if err := failing.Activate(ctx, targetNode(), ready); !errors.Is(err, dialErr) {
		t.Errorf("activate dial failure = %v, want %v", err, dialErr)
	}
}
