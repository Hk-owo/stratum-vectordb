package router

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	pb "stratum/api/proto/stratum"
	"testing"
)

func TestStorageAdminRequiresClusterCredentialAndControlRouting(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(CredentialMetadataKey, "Bearer test"))
	s := NewStorageAdminServer(&Router{auth: NewAuthenticator(func(string) (Principal, bool) { return Principal{AllKBs: true}, true })})
	if err := s.authorize(ctx); status.Code(err) != codes.PermissionDenied {
		t.Fatal("tenant can administer cluster", err)
	}
	s.r.auth = NewAuthenticator(func(string) (Principal, bool) { return Principal{AllKBs: true, StorageAdmin: true}, true })
	if err := s.authorize(ctx); err != nil {
		t.Fatal(err)
	}
	for name := range pb.StorageAdminService_ServiceDesc.Methods {
		method := "/stratum.StorageAdminService/" + pb.StorageAdminService_ServiceDesc.Methods[name].MethodName
		if !isWriteMethod(method) || isStorageMethod(method) {
			t.Fatal("storage management must route to control leader", method)
		}
	}
}
