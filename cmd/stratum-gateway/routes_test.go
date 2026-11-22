package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "stratum/api/proto/stratum"
)

type fakeRoutesTier struct {
	pb.UnimplementedKnowledgeBaseServiceServer
	pb.UnimplementedStorageAdminServiceServer

	deletedKB     string
	rollbackReq   *pb.RollbackVersionRequest
	deleteVerReq  *pb.DeleteVersionRequest
	dirReq        *pb.GetStorageDirectoryRequest
	manageNodeReq *pb.ManageStorageNodeRequest
	drainNodeReq  *pb.DrainStorageNodeRequest
	migrateReq    *pb.MigrateStorageKBRequest
	cancelMigReq  *pb.CancelStorageMigrationRequest
	dirErr        error
	rpcErr        error
}

func (f *fakeRoutesTier) DeleteKnowledgeBase(_ context.Context, req *pb.DeleteKnowledgeBaseRequest) (*pb.DeleteKnowledgeBaseResponse, error) {
	f.deletedKB = req.GetKnowledgeBaseId()
	if f.rpcErr != nil {
		return nil, f.rpcErr
	}
	return &pb.DeleteKnowledgeBaseResponse{Success: true}, nil
}

func (f *fakeRoutesTier) RollbackVersion(_ context.Context, req *pb.RollbackVersionRequest) (*pb.RollbackVersionResponse, error) {
	f.rollbackReq = req
	if f.rpcErr != nil {
		return nil, f.rpcErr
	}
	return &pb.RollbackVersionResponse{Success: true}, nil
}

func (f *fakeRoutesTier) DeleteVersion(_ context.Context, req *pb.DeleteVersionRequest) (*pb.DeleteVersionResponse, error) {
	f.deleteVerReq = req
	if f.rpcErr != nil {
		return nil, f.rpcErr
	}
	return &pb.DeleteVersionResponse{Success: true}, nil
}

func (f *fakeRoutesTier) GetDirectory(_ context.Context, req *pb.GetStorageDirectoryRequest) (*pb.GetStorageDirectoryResponse, error) {
	f.dirReq = req
	if f.dirErr != nil {
		return nil, f.dirErr
	}
	return &pb.GetStorageDirectoryResponse{
		Revision: 3,
		Nodes:    []*pb.StorageNodeEntry{{NodeId: 1, Address: "127.0.0.1:7100", ManagementState: "active"}},
	}, nil
}

func (f *fakeRoutesTier) ManageNode(_ context.Context, req *pb.ManageStorageNodeRequest) (*pb.StorageAdminResult, error) {
	f.manageNodeReq = req
	if f.rpcErr != nil {
		return nil, f.rpcErr
	}
	return &pb.StorageAdminResult{}, nil
}

func (f *fakeRoutesTier) DrainNode(_ context.Context, req *pb.DrainStorageNodeRequest) (*pb.StorageAdminResult, error) {
	f.drainNodeReq = req
	if f.rpcErr != nil {
		return nil, f.rpcErr
	}
	return &pb.StorageAdminResult{}, nil
}

func (f *fakeRoutesTier) MigrateKB(_ context.Context, req *pb.MigrateStorageKBRequest) (*pb.StorageAdminResult, error) {
	f.migrateReq = req
	if f.rpcErr != nil {
		return nil, f.rpcErr
	}
	return &pb.StorageAdminResult{}, nil
}

func (f *fakeRoutesTier) CancelMigration(_ context.Context, req *pb.CancelStorageMigrationRequest) (*pb.StorageAdminResult, error) {
	f.cancelMigReq = req
	if f.rpcErr != nil {
		return nil, f.rpcErr
	}
	return &pb.StorageAdminResult{}, nil
}

func newRoutesTestServer(t *testing.T, tier *fakeRoutesTier) *httptest.Server {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterKnowledgeBaseServiceServer(srv, tier)
	pb.RegisterStorageAdminServiceServer(srv, tier)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	gw := &gateway{
		kb:           pb.NewKnowledgeBaseServiceClient(conn),
		query:        pb.NewQueryServiceClient(conn),
		admin:        pb.NewAdminServiceClient(conn),
		storageAdmin: pb.NewStorageAdminServiceClient(conn),
	}
	mux := http.NewServeMux()
	gw.registerRoutes(mux)
	hs := httptest.NewServer(mux)
	t.Cleanup(hs.Close)
	return hs
}

func TestGatewayRestfulAliasesAndStorageAdmin(t *testing.T) {
	tier := &fakeRoutesTier{}
	hs := newRoutesTestServer(t, tier)

	// DELETE /api/knowledge-bases/{id}
	req, _ := http.NewRequest(http.MethodDelete, hs.URL+"/api/knowledge-bases/kb-99", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || tier.deletedKB != "kb-99" {
		t.Fatalf("DELETE kb: status=%d deletedKB=%q", resp.StatusCode, tier.deletedKB)
	}

	// POST /api/knowledge-bases/{id}/activate
	resp, err = http.Post(hs.URL+"/api/knowledge-bases/kb-99/activate", "application/json", strings.NewReader(`{"target_version_id":"12"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || tier.rollbackReq.GetTargetVersionId() != 12 {
		t.Fatalf("activate: status=%d req=%v", resp.StatusCode, tier.rollbackReq)
	}

	// DELETE /api/knowledge-bases/{id}/versions/{vid}?mode=SINGLE
	req, _ = http.NewRequest(http.MethodDelete, hs.URL+"/api/knowledge-bases/kb-99/versions/5?mode=SINGLE", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || tier.deleteVerReq.GetVersionId() != 5 || tier.deleteVerReq.GetMode() != pb.VersionDeleteMode_VERSION_DELETE_MODE_SINGLE {
		t.Fatalf("delete version: status=%d req=%v", resp.StatusCode, tier.deleteVerReq)
	}

	// GET /api/storage/directory
	resp, err = http.Get(hs.URL + "/api/storage/directory?knowledge_base_id=kb-99&node_id=2")
	if err != nil {
		t.Fatal(err)
	}
	var dirBody map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&dirBody)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || tier.dirReq.GetKnowledgeBaseId() != "kb-99" || tier.dirReq.GetNodeId() != 2 {
		t.Fatalf("storage directory: status=%d req=%v body=%v", resp.StatusCode, tier.dirReq, dirBody)
	}

	// POST /api/storage/nodes/{node_id}/manage
	resp, err = http.Post(hs.URL+"/api/storage/nodes/3/manage", "application/json", strings.NewReader(`{"action":"drain","expected_revision":"3"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || tier.manageNodeReq.GetNodeId() != 3 || tier.manageNodeReq.GetAction() != "drain" {
		t.Fatalf("manage node: status=%d req=%v", resp.StatusCode, tier.manageNodeReq)
	}

	// POST /api/storage/nodes/{node_id}/drain
	resp, err = http.Post(hs.URL+"/api/storage/nodes/3/drain", "application/json", strings.NewReader(`{"operation_id":"op-1","expected_revision":"3","target_node_ids":["4"]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || tier.drainNodeReq.GetNodeId() != 3 || tier.drainNodeReq.GetOperationId() != "op-1" {
		t.Fatalf("drain node: status=%d req=%v", resp.StatusCode, tier.drainNodeReq)
	}

	// POST /api/storage/migrations & cancel
	resp, err = http.Post(hs.URL+"/api/storage/migrations", "application/json", strings.NewReader(`{"operation_id":"mig-1","knowledge_base_id":"kb-1","expected_epoch":"2","from_node_id":"1","to_node_id":"2"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || tier.migrateReq.GetOperationId() != "mig-1" {
		t.Fatalf("migrate kb: status=%d req=%v", resp.StatusCode, tier.migrateReq)
	}

	resp, err = http.Post(hs.URL+"/api/storage/migrations/mig-1/cancel", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || tier.cancelMigReq.GetOperationId() != "mig-1" {
		t.Fatalf("cancel migration: status=%d req=%v", resp.StatusCode, tier.cancelMigReq)
	}
}

func TestGatewayAuthErrorStatusMapping(t *testing.T) {
	tier := &fakeRoutesTier{dirErr: status.Error(codes.PermissionDenied, "storage_admin required")}
	hs := newRoutesTestServer(t, tier)

	resp, err := http.Get(hs.URL + "/api/storage/directory")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("PermissionDenied status = %d, want 403", resp.StatusCode)
	}

	tier.dirErr = status.Error(codes.Unauthenticated, "missing token")
	resp, err = http.Get(hs.URL + "/api/storage/directory")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Unauthenticated status = %d, want 401", resp.StatusCode)
	}
}

func TestGatewayRoutesValidationAndUpstreamErrors(t *testing.T) {
	tier := &fakeRoutesTier{}
	hs := newRoutesTestServer(t, tier)

	// Full enum name in ?mode=VERSION_DELETE_MODE_SUBTREE
	req, _ := http.NewRequest(http.MethodDelete, hs.URL+"/api/knowledge-bases/kb-1/versions/9?mode=VERSION_DELETE_MODE_SUBTREE", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || tier.deleteVerReq.GetMode() != pb.VersionDeleteMode_VERSION_DELETE_MODE_SUBTREE {
		t.Fatalf("full enum delete version: status=%d req=%v", resp.StatusCode, tier.deleteVerReq)
	}

	// Bad inputs -> 400 BadRequest
	badCases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodDelete, "/api/knowledge-bases/kb-1/versions/not-int", ""},
		{http.MethodDelete, "/api/knowledge-bases/kb-1/versions/5?mode=BOGUS", ""},
		{http.MethodGet, "/api/storage/directory?node_id=bad", ""},
		{http.MethodPost, "/api/storage/nodes/bad/manage", `{}`},
		{http.MethodPost, "/api/storage/nodes/3/manage", `{bad json`},
		{http.MethodPost, "/api/storage/nodes/bad/drain", `{}`},
		{http.MethodPost, "/api/storage/nodes/3/drain", `{bad json`},
		{http.MethodPost, "/api/storage/migrations/mig-1/cancel", `{bad json`},
	}
	for _, tc := range badCases {
		var r *http.Request
		if tc.body != "" {
			r, _ = http.NewRequest(tc.method, hs.URL+tc.path, strings.NewReader(tc.body))
		} else {
			r, _ = http.NewRequest(tc.method, hs.URL+tc.path, nil)
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s %s: status=%d, want 400", tc.method, tc.path, resp.StatusCode)
		}
	}

	// Upstream gRPC errors -> 412 FailedPrecondition
	tier.rpcErr = status.Error(codes.FailedPrecondition, "precondition failed")
	errCases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodDelete, "/api/knowledge-bases/kb-1", ""},
		{http.MethodDelete, "/api/knowledge-bases/kb-1/versions/5", ""},
		{http.MethodPost, "/api/storage/nodes/3/manage", `{"action":"drain"}`},
		{http.MethodPost, "/api/storage/nodes/3/drain", `{"operation_id":"op-1"}`},
		{http.MethodPost, "/api/storage/migrations/mig-1/cancel", `{}`},
	}
	for _, tc := range errCases {
		var r *http.Request
		if tc.body != "" {
			r, _ = http.NewRequest(tc.method, hs.URL+tc.path, strings.NewReader(tc.body))
		} else {
			r, _ = http.NewRequest(tc.method, hs.URL+tc.path, nil)
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusPreconditionFailed {
			t.Fatalf("%s %s: status=%d, want 412", tc.method, tc.path, resp.StatusCode)
		}
	}
}
