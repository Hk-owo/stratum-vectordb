package service

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	serrors "stratum/internal/errors"
	"stratum/internal/storage"
)

func leaseProtected(method string) bool {
	if strings.HasPrefix(method, "/stratum.QueryService/") {
		return true
	}
	if strings.HasPrefix(method, "/stratum.AdminService/") {
		switch method {
		case "/stratum.AdminService/HealthCheck", "/stratum.AdminService/GetSystemStatus", "/stratum.AdminService/GetClusterStatus", "/stratum.AdminService/ListFailedVersions", "/stratum.AdminService/ForceRetryVersion", "/stratum.AdminService/ForceAbandonVersion":
			return false
		default:
			return true
		}
	}
	if strings.HasPrefix(method, "/stratum.DataSyncService/") {
		switch method {
		case "/stratum.DataSyncService/Heartbeat", "/stratum.DataSyncService/GetChainTails", "/stratum.DataSyncService/GetDataHolders", "/stratum.DataSyncService/FenceKBReplica", "/stratum.DataSyncService/CleanupStorage":
			return false
		default:
			return true
		}
	}
	return false
}
func UnaryStorageServiceLease(g *storage.ServiceLease) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if g == nil || !leaseProtected(info.FullMethod) {
			return next(ctx, req)
		}
		c, release, err := g.Enter(ctx)
		if err != nil {
			return nil, serrors.ToGRPCStatus(err)
		}
		defer release()
		result, err := next(c, req)
		if gateErr := g.CheckContext(c); gateErr != nil {
			return nil, serrors.ToGRPCStatus(gateErr)
		}
		return result, err
	}
}

type leasedStream struct {
	grpc.ServerStream
	ctx  context.Context
	gate *storage.ServiceLease
}

func (s *leasedStream) Context() context.Context { return s.ctx }
func (s *leasedStream) SendMsg(m any) error {
	if err := s.gate.CheckContext(s.ctx); err != nil {
		return serrors.ToGRPCStatus(err)
	}
	return s.ServerStream.SendMsg(m)
}
func (s *leasedStream) RecvMsg(m any) error {
	if err := s.gate.CheckContext(s.ctx); err != nil {
		return serrors.ToGRPCStatus(err)
	}
	err := s.ServerStream.RecvMsg(m)
	if e := s.gate.CheckContext(s.ctx); e != nil {
		return serrors.ToGRPCStatus(e)
	}
	return err
}
func StreamStorageServiceLease(g *storage.ServiceLease) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if g == nil || !leaseProtected(info.FullMethod) {
			return next(srv, ss)
		}
		ctx, release, err := g.Enter(ss.Context())
		if err != nil {
			return serrors.ToGRPCStatus(err)
		}
		defer release()
		err = next(srv, &leasedStream{ServerStream: ss, ctx: ctx, gate: g})
		if e := g.CheckContext(ctx); e != nil {
			return serrors.ToGRPCStatus(e)
		}
		return err
	}
}
