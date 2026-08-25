package chunkstore

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	vecstorepb "stratum/api/proto/vecstore"
)

// VecstoreChunkStore is the real ChunkStore implementation: a gRPC client
// wrapper around the C++ vecstore process's ChunkStorageService, per
// Stratum_接口设计v9.md "ChunkStore". The key passed to vecstore is
// kbID encoded with a length prefix followed by chunkID — see encodeKey —
// so that DeleteByKB's prefix delete cannot accidentally match a
// different knowledge base whose ID happens to be a string prefix of
// another (e.g. "kb" vs "kb-extended").
type VecstoreChunkStore struct {
	conn   *grpc.ClientConn
	client vecstorepb.ChunkStorageServiceClient
}

// NewVecstoreChunkStore dials addr (the vecstore.grpc_addr config value)
// and returns a ready-to-use ChunkStore. Dialing is non-blocking by
// default (grpc-go lazily connects on first RPC and transparently
// reconnects on transient failures with its own internal backoff), which
// is why this constructor does not itself implement an app-level retry
// loop — connection-level retry/backoff is handled by the gRPC channel
// itself; RPC-level retry policy belongs to callers like WriteCoordinator
// (configured via write_coordinator.max_retries), not to this thin client
// wrapper.
func NewVecstoreChunkStore(addr string) (*VecstoreChunkStore, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// 10s 无流 ping + PermitWithoutStream 会被 vecstore 的 C++
			// 服务端以 too_many_pings GoAway 踢掉连接；改为仅在活动流
			// 上以 60s 间隔探测。
			Time:                60 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: false,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: dial %s: %w", addr, err)
	}
	return &VecstoreChunkStore{
		conn:   conn,
		client: vecstorepb.NewChunkStorageServiceClient(conn),
	}, nil
}

// Close releases the underlying gRPC connection.
func (s *VecstoreChunkStore) Close() error {
	return s.conn.Close()
}

// VecstoreClient returns the underlying ChunkStorageServiceClient. Used
// by the data-sync leader handler to read chunk vectors directly.
func (s *VecstoreChunkStore) VecstoreClient() vecstorepb.ChunkStorageServiceClient {
	return s.client
}

// encodeKey builds the vecstore key for (kbID, chunkID): a 4-byte
// big-endian length prefix on kbID followed by kbID's raw bytes, followed
// by chunkID's raw bytes. The length prefix on kbID (mirroring
// internal/pebbleutil.EncodeString's approach on the Go-storage side)
// guarantees DeleteByPrefix(encodeKBPrefix(kbID)) can never match a
// different, longer kbID that happens to start with the same characters.
func encodeKey(kbID, chunkID string) string {
	return string(encodeKBPrefix(kbID)) + chunkID
}

// EncodeKey is the exported form of encodeKey. Used by IndexManager build
// data-source wiring (cmd/stratum/main.go) to read chunk vectors back
// from the vecstore's ChunkStorageService with the exact key the write
// path used.
func EncodeKey(kbID, chunkID string) string {
	return encodeKey(kbID, chunkID)
}

// encodeKBPrefix returns the length-prefixed encoding of kbID alone,
// usable both as the leading portion of encodeKey and as an exact,
// collision-free DeleteByPrefix argument.
func encodeKBPrefix(kbID string) []byte {
	b := make([]byte, 4+len(kbID))
	n := len(kbID)
	b[0] = byte(n >> 24)
	b[1] = byte(n >> 16)
	b[2] = byte(n >> 8)
	b[3] = byte(n)
	copy(b[4:], kbID)
	return b
}

func (s *VecstoreChunkStore) Write(ctx context.Context, kbID, chunkID string, vector []float32) error {
	_, err := s.client.Write(ctx, &vecstorepb.WriteChunkRequest{
		Key:    encodeKey(kbID, chunkID),
		Vector: vector,
	})
	if err != nil {
		return fmt.Errorf("chunkstore: Write(%s,%s): %w", kbID, chunkID, err)
	}
	return nil
}

func (s *VecstoreChunkStore) Exists(ctx context.Context, kbID, chunkID string) (bool, error) {
	resp, err := s.client.Exists(ctx, &vecstorepb.ExistsChunkRequest{
		Key: encodeKey(kbID, chunkID),
	})
	if err != nil {
		return false, fmt.Errorf("chunkstore: Exists(%s,%s): %w", kbID, chunkID, err)
	}
	return resp.GetExists(), nil
}

func (s *VecstoreChunkStore) Delete(ctx context.Context, kbID, chunkID string) error {
	_, err := s.client.Delete(ctx, &vecstorepb.DeleteChunkRequest{
		Key: encodeKey(kbID, chunkID),
	})
	if err != nil {
		return fmt.Errorf("chunkstore: Delete(%s,%s): %w", kbID, chunkID, err)
	}
	return nil
}

func (s *VecstoreChunkStore) DeleteByKB(ctx context.Context, kbID string) error {
	_, err := s.client.DeleteByPrefix(ctx, &vecstorepb.DeleteByPrefixRequest{
		Prefix: string(encodeKBPrefix(kbID)),
	})
	if err != nil {
		return fmt.Errorf("chunkstore: DeleteByKB(%s): %w", kbID, err)
	}
	return nil
}

// DiskUsage implements ChunkStore: asks the C++ vecstore for its RocksDB
// on-disk size.
func (s *VecstoreChunkStore) DiskUsage(ctx context.Context) (uint64, error) {
	resp, err := s.client.DiskUsage(ctx, &vecstorepb.DiskUsageRequest{})
	if err != nil {
		return 0, fmt.Errorf("chunkstore: DiskUsage: %w", err)
	}
	return resp.GetBytes(), nil
}

var _ ChunkStore = (*VecstoreChunkStore)(nil)
