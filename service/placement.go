package service

import (
	"context"
	"google.golang.org/grpc"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
	"strings"
)

// UnaryStoragePlacement fences the storage-facing KB APIs before any local
// index work. Control metadata and cluster-wide admin methods stay independent.
func UnaryStoragePlacement(resolver storage.Resolver, nodeID int64) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		kb, ok := req.(interface{ GetKnowledgeBaseId() string })
		storageAPI := strings.HasPrefix(info.FullMethod, "/stratum.QueryService/") || strings.HasPrefix(info.FullMethod, "/stratum.AdminService/")
		if resolver != nil && storageAPI && ok && kb.GetKnowledgeBaseId() != "" {
			snap, err := storage.Resolve(ctx, resolver, kb.GetKnowledgeBaseId())
			if err != nil {
				return nil, serrors.ToGRPCStatus(err)
			}
			if !snap.Placement.Contains(nodeID) {
				return nil, serrors.ToGRPCStatus(serrors.ErrNotKBReplica)
			}
			ctx = storage.Bind(ctx, snap)
		}
		return next(ctx, req)
	}
}

type KBReplicaResolver func(context.Context, string) ([]string, error)

func (s *KnowledgeBaseServiceImpl) SetKBReplicaResolver(resolve KBReplicaResolver) {
	s.kbReplicas = resolve
}
func (s *AdminServiceImpl) SetKBReplicaResolver(resolve KBReplicaResolver) { s.kbReplicas = resolve }
