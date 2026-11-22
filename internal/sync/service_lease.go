package sync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/rpcpool"
	"stratum/internal/storage"
)

// ServiceRenewer is separate from the advisory heartbeat: authorization requires
// a committed control operation, and failure cannot be mistaken for liveness.
type ServiceRenewer struct {
	NodeID        int64
	Address       string
	Gate          *storage.ServiceLease
	ResolveLeader LeaderAddrResolver
	Directory     func(context.Context) (storage.Directory, error)
	Pool          *rpcpool.Pool
	Dial          func(context.Context, string) (*grpc.ClientConn, error)
	Logger        *zap.Logger
	now           func() time.Time
	conn          *grpc.ClientConn
	addr          string
}

func (r *ServiceRenewer) close() {
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
		r.addr = ""
	}
}
func (r *ServiceRenewer) Run(ctx context.Context) {
	defer r.close()
	defer r.Gate.Pause()
	ticker := time.NewTicker(storage.ServiceLeaseDuration / 3)
	defer ticker.Stop()
	for {
		if err := r.ReportOnce(ctx); err != nil && ctx.Err() == nil && r.Logger != nil {
			r.Logger.Debug("storage service renewal pending", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (r *ServiceRenewer) ReportOnce(ctx context.Context) error {
	if r.Gate == nil || r.ResolveLeader == nil || r.Directory == nil {
		return serrors.ErrInvalidArgument
	}
	now := r.now
	if now == nil {
		now = time.Now
	}
	// Count resolver, dialing, commit and reconciliation against the SAME grant.
	start := now()
	c, cancel := context.WithTimeout(ctx, storage.ServiceLeaseDuration/3)
	defer cancel()
	addr, ok, err := r.ResolveLeader(c)
	if err != nil {
		return err
	}
	if !ok || addr == "" {
		return serrors.ErrNoLeaderYet
	}
	var rpcConn *grpc.ClientConn
	if r.Dial == nil && r.Pool != nil {
		rpcConn, err = r.Pool.GetContext(c, addr)
		if err != nil {
			return err
		}
	} else {
		if r.conn == nil || addr != r.addr {
			r.close()
			if r.Dial != nil {
				r.conn, err = r.Dial(c, addr)
			} else {
				r.conn, err = rpcpool.DefaultDialer().DialContext(c, addr)
			}
			if err != nil {
				return err
			}
			r.addr = addr
		}
		rpcConn = r.conn
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	requestID := hex.EncodeToString(nonce[:])
	resp, err := pb.NewInternalServiceClient(rpcConn).RenewStorageService(c, &pb.RenewStorageServiceRequest{NodeId: r.NodeID, Address: r.Address, RequestId: requestID})
	if err != nil {
		if serrors.ReasonOf(err) == "storage_service_paused" {
			r.Gate.Pause()
		}
		return err
	}
	duration := time.Duration(resp.GetDurationMs()) * time.Millisecond
	if resp.GetRequestId() != requestID || resp.GetDurationMs() <= 0 || resp.GetDurationMs() > storage.ServiceLeaseDuration.Milliseconds() || resp.GetDirectoryRevision() == 0 {
		return fmt.Errorf("%w: invalid service grant", serrors.ErrStorageServicePaused)
	}
	if r.Gate.NeedsReconcile(resp.GetDirectoryRevision()) {
		r.Gate.Pause()
		dir, err := r.Directory(c)
		if err != nil {
			return err
		}
		return r.Gate.Install(start, duration, resp.GetDirectoryRevision(), dir)
	}
	// Expiry can occur after NeedsReconcile. Never reopen admission with Renew.
	return r.Gate.Renew(start, duration, resp.GetDirectoryRevision())
}
