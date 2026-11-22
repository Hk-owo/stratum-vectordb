package sync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/fsutil"
	"stratum/internal/pebbleutil"
	"stratum/internal/storage"
)

// migrationViewMetadata answers the transfer view with whatever the test built,
// so every refusal in migrationViewFor can be reached without staging a whole
// cluster. Nothing else is ever called on it.
type migrationViewMetadata struct {
	migrationFixtureMetadata
	view storage.MigrationTransferView
	err  error
}

func (m migrationViewMetadata) GetKBMigrationTransfer(context.Context, string) (storage.MigrationTransferView, error) {
	return m.view, m.err
}

// transferView is one admissible view: KB "kb" on {1,2} at epoch 1 with migration
// "move" taking it to node 3, every member already fenced.
func transferView(t *testing.T, mutate func(d *storage.Directory, manifest *storage.MigrationManifest)) storage.MigrationTransferView {
	t.Helper()
	d := storage.NewDirectory()
	if err := d.RegisterNodes(0, []storage.Node{{ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}, {ID: 3, Addr: "s3"}}); err != nil {
		t.Fatal(err)
	}
	p := storage.Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"}
	d.Placements["kb"] = p
	d.ActiveVersions = map[string]int64{"kb": 5}
	d.Migrations = map[string]storage.Migration{
		"move": {
			Request:   storage.MigrationRequest{OperationID: "move", KBID: "kb", ExpectedEpoch: 1, FromID: 2, ToID: 3},
			Old:       p.Clone(),
			FencedIDs: []int64{1, 2, 3},
			Phase:     storage.MigrationFenced,
		},
	}
	manifest := storage.MigrationManifest{KBID: "kb", ActiveVersion: 5}
	if mutate != nil {
		mutate(&d, &manifest)
	}
	return storage.MigrationTransferView{Directory: d, Manifest: manifest}
}

func viewHandler(t *testing.T, nodeID int64, metadata MigrationMetadata) *NodeHandler {
	t.Helper()
	return NewNodeHandler(nil, NewPushHandler(nil, nodeID), WithMigrationStaging(metadata, t.TempDir()))
}

func testTransferRequest() *pb.MigrationTransferRequest {
	return &pb.MigrationTransferRequest{KnowledgeBaseId: "kb", OperationId: "move", ExpectedEpoch: 1, SourceNodeId: 1}
}

// No transfer RPC may run without a stager, a metadata reader, and a receiver:
// any of the three missing means the answer would be "here is the data" for a
// placement nobody verified.
func TestMigrationViewFor_RequiresTheStagingWiring(t *testing.T) {
	ctx := context.Background()
	view := transferView(t, nil)

	bare := NewNodeHandler(nil, NewPushHandler(nil, 1))
	if _, _, _, err := bare.migrationViewFor(ctx, testTransferRequest(), false, false); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("no stager = %v", err)
	}

	noMetadata := NewNodeHandler(nil, NewPushHandler(nil, 1))
	noMetadata.migration = &migrationStager{}
	if _, _, _, err := noMetadata.migrationViewFor(ctx, testTransferRequest(), false, false); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("no metadata = %v", err)
	}

	noReceiver := NewNodeHandler(nil, nil, WithMigrationStaging(migrationViewMetadata{view: view}, t.TempDir()))
	if _, _, _, err := noReceiver.migrationViewFor(ctx, testTransferRequest(), false, false); !errors.Is(err, serrors.ErrPlacementChangeUnsafe) {
		t.Errorf("no receiver = %v", err)
	}

	h := viewHandler(t, 1, migrationViewMetadata{view: view})
	badRequests := map[string]func(*pb.MigrationTransferRequest) *pb.MigrationTransferRequest{
		"requestless call": func(*pb.MigrationTransferRequest) *pb.MigrationTransferRequest { return nil },
		"no knowledge base": func(r *pb.MigrationTransferRequest) *pb.MigrationTransferRequest {
			r.KnowledgeBaseId = ""
			return r
		},
		"no operation": func(r *pb.MigrationTransferRequest) *pb.MigrationTransferRequest {
			r.OperationId = ""
			return r
		},
		"epoch zero": func(r *pb.MigrationTransferRequest) *pb.MigrationTransferRequest {
			r.ExpectedEpoch = 0
			return r
		},
		"no source node": func(r *pb.MigrationTransferRequest) *pb.MigrationTransferRequest {
			r.SourceNodeId = 0
			return r
		},
	}
	for name, mutate := range badRequests {
		if _, _, _, err := h.migrationViewFor(ctx, mutate(testTransferRequest()), false, false); !errors.Is(err, serrors.ErrInvalidArgument) {
			t.Errorf("%s: err = %v, want ErrInvalidArgument", name, err)
		}
	}

	// A metadata read failure is surfaced unchanged: the caller has to be able to
	// tell "the cluster says no" from "I could not reach the cluster".
	failing := viewHandler(t, 1, migrationViewMetadata{err: serrors.ErrNoLeaderYet})
	if _, _, _, err := failing.migrationViewFor(ctx, testTransferRequest(), false, false); !errors.Is(err, serrors.ErrNoLeaderYet) {
		t.Errorf("metadata failure = %v, want it surfaced", err)
	}
}

// Everything migrationViewFor refuses, one case per reason. A transfer that slipped
// past any of these would hand a peer data for a placement it is not part of, or
// for a version the cluster has already moved past.
func TestMigrationViewFor_RefusesEverythingOutsideTheCommittedTask(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		mutate func(d *storage.Directory, manifest *storage.MigrationManifest)
		req    func(r *pb.MigrationTransferRequest) *pb.MigrationTransferRequest
		node   int64
		target bool
		allow  bool
		want   error
	}{
		{
			name: "operation is unknown",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				d.Migrations = map[string]storage.Migration{}
			},
			want: serrors.ErrMigrationNotFound,
		},
		{
			name: "knowledge base moved on",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				m := d.Migrations["move"]
				m.Request.KBID = "other"
				d.Migrations["move"] = m
			},
			want: serrors.ErrPlacementStale,
		},
		{
			name: "epoch moved on",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				m := d.Migrations["move"]
				m.Request.ExpectedEpoch = 2
				d.Migrations["move"] = m
			},
			want: serrors.ErrPlacementStale,
		},
		{
			name: "task is still draining",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				m := d.Migrations["move"]
				m.Phase = storage.MigrationDraining
				d.Migrations["move"] = m
			},
			want: serrors.ErrMigrationConflict,
		},
		{
			name: "installed task outside the install window",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				m := d.Migrations["move"]
				m.Phase = storage.MigrationInstalled
				d.Migrations["move"] = m
			},
			want: serrors.ErrMigrationConflict,
		},
		{
			// The task carries a fault flag but the directory has no matching
			// block, so there is no committed proof that the member may be skipped.
			name: "fault mode whose revocation proof is not committed",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				m := d.Migrations["move"]
				m.Request.Fault = true
				m.FailedBlock = storage.ServiceBlock{OperationID: "block", Revision: 1}
				d.Migrations["move"] = m
			},
			want: serrors.ErrPlacementStale,
		},
		{
			name: "placement lost a member",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				d.Placements["kb"] = storage.Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1}, RequiredReplicas: 1, OperationID: "create/kb"}
			},
			want: serrors.ErrPlacementStale,
		},
		{
			name:   "placement is gone",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) { delete(d.Placements, "kb") },
			want:   serrors.ErrPlacementUnknown,
		},
		{
			name: "source was never a member",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				p := storage.Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{2, 3}, RequiredReplicas: 2, OperationID: "create/kb"}
				d.Placements["kb"] = p
				m := d.Migrations["move"]
				m.Old = p.Clone()
				d.Migrations["move"] = m
			},
			want: serrors.ErrNotKBReplica,
		},
		{
			name: "source never acknowledged its fence",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				m := d.Migrations["move"]
				m.FencedIDs = []int64{2, 3}
				d.Migrations["move"] = m
			},
			want: serrors.ErrNotKBReplica,
		},
		{
			name: "the failed member asks for its own data",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				block := storage.ServiceBlock{OperationID: "block", Revision: 1, Revoked: true}
				m := d.Migrations["move"]
				m.Request = storage.MigrationRequest{OperationID: "move", KBID: "kb", ExpectedEpoch: 1, FromID: 1, ToID: 3, Fault: true}
				m.FailedBlock = block
				m.FencedIDs = []int64{1, 2, 3}
				d.Migrations["move"] = m
				d.ServiceBlocks = map[int64]storage.ServiceBlock{1: block}
			},
			want: serrors.ErrNotKBReplica,
		},
		{
			name: "the fence set is incomplete",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				m := d.Migrations["move"]
				m.FencedIDs = []int64{1, 2}
				d.Migrations["move"] = m
			},
			want: serrors.ErrMigrationBusy,
		},
		{
			name:   "wrong node asking as the target",
			node:   1,
			target: true,
			want:   serrors.ErrNotKBReplica,
		},
		{
			name:   "wrong node asking as the source",
			node:   3,
			target: false,
			want:   serrors.ErrNotKBReplica,
		},
		{
			name:   "manifest belongs to another KB",
			mutate: func(_ *storage.Directory, manifest *storage.MigrationManifest) { manifest.KBID = "other" },
			want:   serrors.ErrMigrationConflict,
		},
		{
			name:   "manifest describes another active version",
			mutate: func(_ *storage.Directory, manifest *storage.MigrationManifest) { manifest.ActiveVersion = 4 },
			want:   serrors.ErrMigrationConflict,
		},
		{
			// Installation answers a repeated request from the committed task, so
			// INSTALLED is admissible only inside that window.
			name: "installed task inside the install window",
			mutate: func(d *storage.Directory, _ *storage.MigrationManifest) {
				m := d.Migrations["move"]
				m.Phase = storage.MigrationInstalled
				d.Migrations["move"] = m
			},
			node:   3,
			target: true,
			allow:  true,
		},
		{
			name:   "target asks for its own copy",
			node:   3,
			target: true,
		},
		{
			name: "source exports to the target",
		},
	}
	for _, tc := range cases {
		view := transferView(t, tc.mutate)
		node := tc.node
		if node == 0 {
			if tc.target {
				node = 3
			} else {
				node = 1
			}
		}
		h := viewHandler(t, node, migrationViewMetadata{view: view})
		req := testTransferRequest()
		if tc.req != nil {
			req = tc.req(req)
		}
		_, _, manifest, err := h.migrationViewFor(ctx, req, tc.target, tc.allow)
		if tc.want == nil {
			if err != nil {
				t.Errorf("%s: err = %v, want the view", tc.name, err)
				continue
			}
			if manifest.KBID != "kb" || manifest.ActiveVersion != 5 {
				t.Errorf("%s: manifest = %+v", tc.name, manifest)
			}
			continue
		}
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
}

func TestMigrationStageHelpers(t *testing.T) {
	root := t.TempDir()
	a := migrationStagePath(root, "kb", "move")
	if b := migrationStagePath(root, "kb", "move"); a != b {
		t.Errorf("stage path is not stable: %q vs %q", a, b)
	}
	if filepath.Dir(a) != root || filepath.Ext(a) != ".stage" {
		t.Errorf("stage path = %q, want a .stage file under %q", a, root)
	}
	if a == migrationStagePath(root, "kb", "other") || a == migrationStagePath(root, "other", "move") {
		t.Error("stage path ignores the KB or the operation")
	}

	if err := fsutil.SyncDir(root); err != nil {
		t.Errorf("sync of a real directory = %v", err)
	}
	if err := fsutil.SyncDir(filepath.Join(root, "missing")); err == nil {
		t.Error("sync of a missing directory reported success")
	}

	// The stager prefers an injected hook, which is how a failing fsync becomes a
	// test instead of a coincidence.
	stager := &migrationStager{}
	if err := stager.syncDirectory(root); err != nil {
		t.Errorf("default syncDirectory = %v", err)
	}
	injected := errors.New("fsync refused")
	stager.syncDir = func(string) error { return injected }
	if err := stager.syncDirectory(root); !errors.Is(err, injected) {
		t.Errorf("injected syncDirectory = %v, want %v", err, injected)
	}
}

// The frame on disk is the only thing the installer trusts, so a truncated or
// oversized frame has to fail closed rather than be interpreted.
func TestMigrationFrameCodec(t *testing.T) {
	var buf bytes.Buffer
	want := &pb.MigrationDataFrame{OperationId: "move", KnowledgeBaseId: "kb", PlacementEpoch: 1}
	if err := writeMigrationFrame(&buf, mustMarshal(t, want)); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := readMigrationFrame(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("round trip = %v, want %v", got, want)
	}

	dataCases := []struct {
		name string
		body []byte
		want error
	}{
		{name: "zero length", body: frameLength(0), want: serrors.ErrMigrationConflict},
		{name: "oversized", body: frameLength(maxMigrationFrame + 1), want: serrors.ErrMigrationConflict},
		{name: "truncated body", body: append(frameLength(10), 'a', 'b'), want: io.ErrUnexpectedEOF},
		{name: "truncated length", body: []byte{0, 0}, want: io.ErrUnexpectedEOF},
	}
	for _, tc := range dataCases {
		if _, err := readMigrationFrame(bytes.NewReader(tc.body)); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
	// 0xFF 0xFF 0xFF is not a valid protobuf field header, so the payload cannot
	// be decoded into a frame at all.
	if _, err := readMigrationFrame(bytes.NewReader(append(frameLength(3), 0xFF, 0xFF, 0xFF))); err == nil {
		t.Error("an unmarshalable frame was accepted")
	}

	if err := writeMigrationFrame(failingWriter{}, []byte{1}); err == nil {
		t.Error("a failed length write was reported as success")
	}
	if err := writeMigrationFrame(&onceWriter{}, []byte{1}); err == nil {
		t.Error("a failed payload write was reported as success")
	}
}

func TestValidateMigrationArtifact(t *testing.T) {
	ctx := context.Background()
	req := testTransferRequest()
	manifest := storage.MigrationManifest{KBID: "kb", ActiveVersion: 5}
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.stage")
	if err := os.WriteFile(empty, nil, 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := validateMigrationStage(ctx, filepath.Join(dir, "missing.stage"), req, manifest, 3); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("missing artifact = %v, want os.ErrNotExist", err)
	}
	if _, err := validateMigrationStage(ctx, dir, req, manifest, 3); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("directory as artifact = %v, want a data error", err)
	}
	if _, err := validateMigrationStage(ctx, empty, req, manifest, 3); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("empty artifact = %v, want an incomplete-artifact error", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := validateMigrationStage(cancelled, empty, req, manifest, 3); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled validation = %v, want context.Canceled", err)
	}

	f, err := os.CreateTemp(dir, "progress-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	stop := errors.New("sweep interrupted")
	if _, err := validateMigrationFile(ctx, f, req, manifest, 3, func() error { return stop }); !errors.Is(err, stop) {
		t.Errorf("progress failure = %v, want %v", err, stop)
	}
	if _, err := validateMigrationFile(ctx, f, req, manifest, 3, func() error { return nil }); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("short file = %v, want an incomplete-artifact error", err)
	}

	// The receipt travels as proto in both directions; a lost field is a target
	// acknowledging an artifact it never verified.
	receipt := storage.MigrationStageReceipt{SourceID: 1, TargetID: 3, ManifestHash: "manifest", PayloadHash: "payload", Entries: 7, Bytes: 99}
	if got := stageReceiptFromProto(stageReceiptToProto(receipt)); got != receipt {
		t.Errorf("receipt round trip = %+v, want %+v", got, receipt)
	}
}

// The export path parses keys it did not write, so a malformed one has to abort
// the transfer instead of being streamed as if it were a document.
func TestMigrationExportRejectsMalformedStoreKeys(t *testing.T) {
	ctx := context.Background()
	prefix := pebbleutil.EncodeString("kb")
	malformed := map[string][]byte{
		"short key":           append(append([]byte{}, prefix...), 0, 0),
		"zero length":         append(append(append([]byte{}, prefix...), 0, 0, 0, 0), make([]byte, 8)...),
		"length past the end": append(append(append([]byte{}, prefix...), 0, 0, 0, 4), append([]byte("ab"), make([]byte, 8)...)...),
	}
	for name, key := range malformed {
		f := newLeaderFixture(t)
		if err := f.handler.docstoreDB.Set(key, []byte("value"), nil); err != nil {
			t.Fatal(err)
		}
		if err := f.handler.exportMigrationMVCC(ctx, "kb", func(*pb.SyncEntry) error { return nil }); !errors.Is(err, serrors.ErrMigrationConflict) {
			t.Errorf("%s: err = %v, want a data error", name, err)
		}
	}

	f := newLeaderFixture(t)
	if err := f.docStore.Write(ctx, "kb", "doc", 1, []byte("body")); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := f.handler.exportMigrationMVCC(cancelled, "kb", func(*pb.SyncEntry) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled export = %v, want context.Canceled", err)
	}

	// A consumer that gave up ends the export: the source must not keep walking a
	// store nobody is reading.
	stop := errors.New("stream closed")
	if err := f.handler.exportMigrationMVCC(ctx, "kb", func(*pb.SyncEntry) error { return stop }); !errors.Is(err, stop) {
		t.Errorf("send failure = %v, want %v", err, stop)
	}
}

// Reverse mappings are the ordinary export path; the forward scan exists to catch
// a lost reverse row, so both the malformed-key and the missing-row cases refuse.
func TestMigrationForwardRelationScan(t *testing.T) {
	ctx := context.Background()
	prefix := append([]byte{'F'}, pebbleutil.EncodeString("kb")...)
	encode := func(parts ...string) []byte {
		out := append([]byte{}, prefix...)
		for _, p := range parts {
			out = append(out, pebbleutil.EncodeString(p)...)
		}
		return out
	}

	empty := newLeaderFixture(t)
	if err := empty.handler.verifyMigrationForwardRelations(ctx, "kb", nil, nil); err != nil {
		t.Fatalf("empty scan: %v", err)
	}

	malformed := map[string][]byte{
		"short relation":      append(append([]byte{}, prefix...), 0, 0, 0),
		"zero length":         append(append([]byte{}, prefix...), 0, 0, 0, 0),
		"length past the end": append(append([]byte{}, prefix...), 0, 0, 0, 9),
		"trailing bytes":      append(encode("chunk", "doc"), 'x'),
		"second malformed":    append(append(append([]byte{}, prefix...), pebbleutil.EncodeString("chunk")...), 0, 0, 0, 0),
	}
	for name, key := range malformed {
		f := newLeaderFixture(t)
		if err := f.handler.chunkdocDB.Set(key, nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := f.handler.verifyMigrationForwardRelations(ctx, "kb", map[string]bool{"doc": true}, nil); !errors.Is(err, serrors.ErrMigrationConflict) {
			t.Errorf("%s: err = %v, want a data error", name, err)
		}
	}

	// A retained document whose forward row has no reverse row is exactly the
	// silent-chunk-loss case this scan exists for.
	missing := newLeaderFixture(t)
	if err := missing.handler.chunkdocDB.Set(encode("chunk", "doc"), nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := missing.handler.verifyMigrationForwardRelations(ctx, "kb", map[string]bool{"doc": true}, nil); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("missing reverse relation = %v, want a data error", err)
	}
	if err := missing.handler.verifyMigrationForwardRelations(ctx, "kb", map[string]bool{"doc": true}, map[migrationPair]bool{{doc: "doc", chunk: "chunk"}: true}); err != nil {
		t.Errorf("matched relation = %v, want success", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := missing.handler.verifyMigrationForwardRelations(cancelled, "kb", nil, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled scan = %v, want context.Canceled", err)
	}
}

// An export with no exporter configured must refuse before it reads anything: the
// transfer view alone is not permission to stream.
func TestExportKBMigration_WithoutAnExporter(t *testing.T) {
	fixture := newMigrationTransferFixture(t)
	h := NewNodeHandler(fixture.source.handler, NewPushHandler(nil, 1), WithMigrationStaging(fixture.metadata, t.TempDir()))
	h.exporter = nil
	err := h.ExportKBMigration(fixture.request, &captureMigrationStream{ctx: context.Background()})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("err = %v, want FailedPrecondition", err)
	}
}

func mustMarshal(t *testing.T, frame *pb.MigrationDataFrame) []byte {
	t.Helper()
	b, err := proto.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func frameLength(n uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], n)
	return b[:]
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write refused") }

type onceWriter struct{ wrote bool }

func (w *onceWriter) Write(p []byte) (int, error) {
	if w.wrote {
		return 0, errors.New("payload refused")
	}
	w.wrote = true
	return len(p), nil
}

// The installation record is the target's own claim about an artifact it wrote. It
// has to be rejected when it does not identify that artefact, and an unreadable one
// must not be mistaken for "not installed yet".
func TestInstallRecord_ValidatesWhatItReads(t *testing.T) {
	dir := t.TempDir()
	good := migrationInstallRecord{
		Receipt:     storage.MigrationInstallReceipt{SourceID: 1, TargetID: 3, ManifestHash: "manifest", StageHash: "stage"},
		OperationID: "move",
		KBID:        "kb",
		Epoch:       1,
	}
	path := migrationInstallPath(dir, "kb", "move")
	if !strings.HasSuffix(path, installRecordSuffix) || filepath.Dir(path) != dir {
		t.Errorf("install path = %q, want %q under %q", path, installRecordSuffix, dir)
	}
	if _, err := readInstallRecord(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("missing record = %v, want os.ErrNotExist", err)
	}

	if err := os.WriteFile(path, []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readInstallRecord(path); !errors.Is(err, serrors.ErrMigrationConflict) {
		t.Errorf("corrupt record = %v, want a data error", err)
	}

	for name, record := range map[string]migrationInstallRecord{
		"no operation":      {Receipt: good.Receipt, KBID: "kb", Epoch: 1},
		"no knowledge base": {Receipt: good.Receipt, OperationID: "move", Epoch: 1},
		"no epoch":          {Receipt: good.Receipt, OperationID: "move", KBID: "kb"},
		"no target":         {Receipt: storage.MigrationInstallReceipt{SourceID: 1}, OperationID: "move", KBID: "kb", Epoch: 1},
	} {
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readInstallRecord(path); !errors.Is(err, serrors.ErrMigrationConflict) {
			t.Errorf("%s: err = %v, want a data error", name, err)
		}
	}

	data, err := json.Marshal(good)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := readInstallRecord(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.OperationID != "move" || got.KBID != "kb" || got.Epoch != 1 || got.Receipt != good.Receipt {
		t.Errorf("record = %+v, want the one that was written", got)
	}
}

// Staging refuses to reuse an artifact it cannot validate: a leftover file from
// another attempt is a conflict, never something to overwrite silently.
func TestStageKBMigration_RefusesAnUnusableExistingArtifact(t *testing.T) {
	ctx := context.Background()
	fixture := newMigrationTransferFixture(t)

	// A directory where the artifact should be is not an artifact.
	path := migrationStagePath(fixture.targetHandler.migration.dir, "kb", "move")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.targetHandler.StageKBMigration(ctx, fixture.request); err == nil {
		t.Error("a directory as staging artifact was accepted")
	}

	// A corrupt artifact is a conflict rather than a silent re-transfer, because the
	// bytes on disk may be the only copy of a partially verified transfer.
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not a frame stream"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := fixture.targetHandler.StageKBMigration(ctx, fixture.request)
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("corrupt artifact = %v, want FailedPrecondition", err)
	}

	// A dial failure is reported as a transfer failure, and nothing is published.
	fixture = newMigrationTransferFixture(t)
	dialErr := errors.New("no route")
	fixture.targetHandler.migration.dial = func(context.Context, string) (*grpc.ClientConn, error) { return nil, dialErr }
	if _, err := fixture.targetHandler.StageKBMigration(ctx, fixture.request); !errors.Is(err, dialErr) {
		t.Errorf("dial failure = %v, want %v", err, dialErr)
	}

	// A source that answers with an error ends the transfer instead of producing a
	// short artifact.
	fixture = newMigrationTransferFixture(t)
	fixture.targetHandler.migration.dial = migrationBufDial(t, interruptedMigrationExporter{tail: serrors.ErrStorageUnavailable})
	if _, err := fixture.targetHandler.StageKBMigration(ctx, fixture.request); err == nil {
		t.Error("a failed source was accepted as a complete transfer")
	}

	// A source that ends without a completion frame is refused: a truncated stream
	// must never look like a finished artifact.
	fixture = newMigrationTransferFixture(t)
	fixture.targetHandler.migration.dial = migrationBufDial(t, interruptedMigrationExporter{})
	if _, err := fixture.targetHandler.StageKBMigration(ctx, fixture.request); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("truncated stream = %v, want FailedPrecondition", err)
	}
}

// A confirmation from a node the placement does not name is refused: the address is
// what a later puller will dial, so it has to belong to a replica of that KB.
func TestConfirmVersionWrite_ChecksTheSourceAddress(t *testing.T) {
	ctx := context.Background()
	snapshot := storage.Snapshot{
		ActiveVersion: 1,
		Placement:     storage.Placement{KBID: "kb", Epoch: 1, ReplicaIDs: []int64{1, 2}, RequiredReplicas: 2, OperationID: "create/kb"},
		Nodes:         []storage.Node{{ID: 1, Addr: "s1", State: storage.NodeActive}, {ID: 2, Addr: "s2", State: storage.NodeActive}},
	}
	placements := func(context.Context, string) (storage.Snapshot, error) { return snapshot, nil }
	handler := NewPushHandler(nil, 1, WithStoragePlacement(placements))
	watcher := &confirmWatcher{}
	handler.watcher = watcher

	resp, err := handler.ConfirmVersionWrite(ctx, &pb.ConfirmVersionWriteRequest{KnowledgeBaseId: "kb", VersionId: 4, SourceAddr: "s2"})
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if resp.GetNodeId() != 1 {
		t.Errorf("response = %+v", resp)
	}
	if len(watcher.confirmed) != 1 || watcher.confirmed[0] != "kb/4" {
		t.Errorf("confirmed = %v, want the announcement recorded", watcher.confirmed)
	}

	// An address outside the placement is another node's business.
	if _, err := handler.ConfirmVersionWrite(ctx, &pb.ConfirmVersionWriteRequest{KnowledgeBaseId: "kb", VersionId: 4, SourceAddr: "elsewhere"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("foreign address = %v, want FailedPrecondition", err)
	}
	// A KB this node is not a replica of is refused before anything is recorded.
	outsider := NewPushHandler(nil, 9, WithStoragePlacement(placements))
	if _, err := outsider.ConfirmVersionWrite(ctx, &pb.ConfirmVersionWriteRequest{KnowledgeBaseId: "kb", VersionId: 4}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("non-replica = %v, want FailedPrecondition", err)
	}
}

// confirmWatcher records the takeover stand-down announcements.
type confirmWatcher struct{ confirmed []string }

func (w *confirmWatcher) ConfirmVersionWrite(kbID string, versionID int64) {
	w.confirmed = append(w.confirmed, fmt.Sprintf("%s/%d", kbID, versionID))
}

func (w *confirmWatcher) WatchVersionWrite(string, int64) {}
