package sync

import (
	"context"
	"fmt"
	"slices"
	stdsync "sync"

	"google.golang.org/grpc"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/rpcpool"
	"stratum/internal/storage"
)

func (h *PushHandler) FenceKBReplica(ctx context.Context, req *pb.FenceKBReplicaRequest) (*pb.FenceKBReplicaResponse, error) {
	if req.GetKnowledgeBaseId() == "" || req.GetOperationId() == "" || req.GetExpectedEpoch() == 0 {
		return nil, serrors.ToGRPCStatus(serrors.ErrInvalidArgument)
	}
	if h.executionGate == nil || h.directory == nil {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementChangeUnsafe)
	}
	dir, err := h.directory.StorageDirectory(ctx)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	m, ok := dir.Migrations[req.GetOperationId()]
	if !ok {
		return nil, serrors.ToGRPCStatus(serrors.ErrMigrationNotFound)
	}
	if m.Request.KBID != req.GetKnowledgeBaseId() || (m.Phase != storage.MigrationFencing && m.Phase != storage.MigrationFenced) {
		return nil, serrors.ToGRPCStatus(serrors.ErrMigrationConflict)
	}
	p, err := dir.Lookup(m.Request.KBID)
	if err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if req.GetExpectedEpoch() != m.Request.ExpectedEpoch || p.Epoch != req.GetExpectedEpoch() {
		return nil, serrors.ToGRPCStatus(serrors.ErrPlacementStale)
	}
	if err := dir.ValidateFailedBlock(m); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	if !slices.Contains(m.RequiredFenceIDs(), h.nodeID) {
		return nil, serrors.ToGRPCStatus(serrors.ErrNotKBReplica)
	}
	if err := h.executionGate.Fence(ctx, m.Request.KBID, req.GetExpectedEpoch(), req.GetOperationId()); err != nil {
		return nil, serrors.ToGRPCStatus(err)
	}
	return &pb.FenceKBReplicaResponse{NodeId: h.nodeID, OperationId: req.GetOperationId(), FencedEpoch: req.GetExpectedEpoch()}, nil
}

type FenceClient struct {
	mu    stdsync.Mutex
	conns map[string]*grpc.ClientConn
	Pool  *rpcpool.Pool
	Dial  func(context.Context, string) (*grpc.ClientConn, error)
}

func (c *FenceClient) connection(ctx context.Context, node storage.Node) (*grpc.ClientConn, error) {
	if c.Dial == nil && c.Pool != nil {
		return c.Pool.GetContext(ctx, node.Addr)
	}
	c.mu.Lock()
	if c.conns == nil {
		c.conns = make(map[string]*grpc.ClientConn)
	}
	conn := c.conns[node.Addr]
	if conn == nil {
		var err error
		if c.Dial != nil {
			conn, err = c.Dial(ctx, node.Addr)
		} else {
			conn, err = rpcpool.DefaultDialer().DialContext(ctx, node.Addr)
		}
		if err != nil {
			c.mu.Unlock()
			return nil, err
		}
		c.conns[node.Addr] = conn
	}
	c.mu.Unlock()
	return conn, nil
}

func (c *FenceClient) Fence(ctx context.Context, node storage.Node, m storage.Migration) error {
	conn, err := c.connection(ctx, node)
	if err != nil {
		return err
	}
	resp, err := pb.NewDataSyncServiceClient(conn).FenceKBReplica(ctx, &pb.FenceKBReplicaRequest{KnowledgeBaseId: m.Request.KBID, OperationId: m.Request.OperationID, ExpectedEpoch: m.Request.ExpectedEpoch})
	if err != nil {
		return err
	}
	if resp.GetNodeId() != node.ID || resp.GetOperationId() != m.Request.OperationID || resp.GetFencedEpoch() != m.Request.ExpectedEpoch {
		return fmt.Errorf("%w: invalid fence acknowledgement from %s", serrors.ErrMigrationConflict, node.Addr)
	}
	return nil
}

func (c *FenceClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, addr)
	}
}

func (c *FenceClient) Stage(ctx context.Context, target, source storage.Node, m storage.Migration) (storage.MigrationStageReceipt, error) {
	var zero storage.MigrationStageReceipt
	conn, err := c.connection(ctx, target)
	if err != nil {
		return zero, err
	}
	response, err := pb.NewDataSyncServiceClient(conn).StageKBMigration(ctx, &pb.MigrationTransferRequest{KnowledgeBaseId: m.Request.KBID, OperationId: m.Request.OperationID, ExpectedEpoch: m.Request.ExpectedEpoch, SourceNodeId: source.ID})
	if err != nil {
		return zero, err
	}
	receipt := stageReceiptFromProto(response)
	if receipt.SourceID != source.ID || receipt.TargetID != target.ID || !storage.ValidSHA256(receipt.ManifestHash) || !storage.ValidSHA256(receipt.PayloadHash) || receipt.Bytes == 0 {
		return zero, serrors.ErrMigrationConflict
	}
	return receipt, nil
}

// Install drives the target's own installation of a verified artifact. The
// source named here is the one the control layer recorded on the stage receipt,
// never a fresh choice.
func (c *FenceClient) Install(ctx context.Context, target storage.Node, m storage.Migration) (storage.MigrationInstallReceipt, error) {
	var zero storage.MigrationInstallReceipt
	if m.Stage == nil || m.Stage.SourceID <= 0 {
		return zero, serrors.ErrMigrationConflict
	}
	conn, err := c.connection(ctx, target)
	if err != nil {
		return zero, err
	}
	response, err := pb.NewDataSyncServiceClient(conn).InstallKBMigration(ctx, &pb.MigrationTransferRequest{KnowledgeBaseId: m.Request.KBID, OperationId: m.Request.OperationID, ExpectedEpoch: m.Request.ExpectedEpoch, SourceNodeId: m.Stage.SourceID})
	if err != nil {
		return zero, err
	}
	receipt := installReceiptFromProto(response)
	if receipt.SourceID != m.Stage.SourceID || receipt.TargetID != target.ID || receipt.ManifestHash != m.Stage.ManifestHash || receipt.StageHash != m.Stage.PayloadHash || !storage.ValidSHA256(receipt.ManifestHash) {
		return zero, serrors.ErrMigrationConflict
	}
	return receipt, nil
}

// PrepareServing drives the target's local serving steps (document filters, data
// cursor, active-version index). The evidence it returns is local only: no READY
// report reaches the control layer for a node that is not a member yet.
func (c *FenceClient) PrepareServing(ctx context.Context, target storage.Node, m storage.Migration) (storage.MigrationServeReceipt, error) {
	var zero storage.MigrationServeReceipt
	if m.Stage == nil || m.Install == nil {
		return zero, serrors.ErrMigrationConflict
	}
	conn, err := c.connection(ctx, target)
	if err != nil {
		return zero, err
	}
	response, err := pb.NewDataSyncServiceClient(conn).PrepareKBMigrationServing(ctx, &pb.MigrationTransferRequest{KnowledgeBaseId: m.Request.KBID, OperationId: m.Request.OperationID, ExpectedEpoch: m.Request.ExpectedEpoch, SourceNodeId: m.Stage.SourceID})
	if err != nil {
		return zero, err
	}
	receipt := serveReceiptFromProto(response)
	if receipt.TargetID != target.ID || !storage.ValidSHA256(receipt.ManifestHash) || !receipt.IndexReady {
		return zero, serrors.ErrMigrationConflict
	}
	return receipt, nil
}

func (c *FenceClient) Activate(ctx context.Context, target storage.Node, m storage.Migration) error {
	if m.Stage == nil || m.Serve == nil {
		return serrors.ErrMigrationConflict
	}
	conn, err := c.connection(ctx, target)
	if err != nil {
		return err
	}
	_, err = pb.NewDataSyncServiceClient(conn).ActivateKBMigration(ctx, &pb.MigrationTransferRequest{KnowledgeBaseId: m.Request.KBID, OperationId: m.Request.OperationID, ExpectedEpoch: m.Old.Epoch, SourceNodeId: m.Stage.SourceID})
	return err
}
func (c *FenceClient) Cleanup(ctx context.Context, node storage.Node, task storage.Cleanup) error {
	conn, err := c.connection(ctx, node)
	if err != nil {
		return err
	}
	_, err = pb.NewDataSyncServiceClient(conn).CleanupStorage(ctx, &pb.CleanupStorageRequest{CleanupId: task.ID})
	return err
}
