package router

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "stratum/api/proto/stratum"
	serrors "stratum/internal/errors"
	"stratum/internal/owner"
	"stratum/internal/storage"
)

type endpointKey struct {
	id   int64
	addr string
}
type storagePeer struct {
	syncClient pb.DataSyncServiceClient
	key        endpointKey
	conn       *grpc.ClientConn
	query      pb.QueryServiceClient
	admin      pb.AdminServiceClient
	breaker    *breaker
	load       *inflight
	lag        *lagRecords
	refs       int
}

// storagePool retains endpoint generations until their last operation exits.
// Breakers, load and lag belong to a stable node/address pair, never a slot.
// A stale operation may use its captured endpoint without reverting the pool's
// authoritative directory or closing connections used by newer operations.
type storagePool struct {
	probing  bool
	mu       sync.Mutex
	revision uint64
	desired  map[endpointKey]bool
	peers    map[endpointKey]*storagePeer
	closed   bool
	dial     func(string) (*grpc.ClientConn, error)
}

func newStoragePool() *storagePool {
	return &storagePool{desired: map[endpointKey]bool{}, peers: map[endpointKey]*storagePeer{}, dial: func(addr string) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}}
}

func (p *storagePool) reconcile(d storage.Directory) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || d.Revision < p.revision {
		return
	}
	p.revision = d.Revision
	p.desired = make(map[endpointKey]bool, len(d.Nodes))
	for _, n := range d.Nodes {
		if n.State == storage.NodeRetired {
			continue
		}
		p.desired[endpointKey{n.ID, n.Addr}] = true
	}
	p.sweep()
}

func (p *storagePool) sweep() {
	for k, peer := range p.peers {
		if peer.refs == 0 && (p.closed || !p.desired[k]) {
			_ = peer.conn.Close()
			delete(p.peers, k)
		}
	}
}

func (p *storagePool) acquire(nodes []storage.Node) ([]*storagePeer, func(), error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, nil, fmt.Errorf("router: storage pool closed")
	}
	peers := make([]*storagePeer, 0, len(nodes))
	for _, n := range nodes {
		k := endpointKey{n.ID, n.Addr}
		peer := p.peers[k]
		if peer == nil {
			conn, err := p.dial(n.Addr)
			if err != nil {
				for _, held := range peers {
					held.refs--
				}
				p.sweep()
				return nil, nil, err
			}
			peer = &storagePeer{key: k, conn: conn, query: pb.NewQueryServiceClient(conn), admin: pb.NewAdminServiceClient(conn), breaker: newBreaker(defaultBreakerConfig), load: &inflight{}, lag: newLagRecords(defaultLagRecordTTL)}
			peer.syncClient = pb.NewDataSyncServiceClient(conn)
			if p.probing {
				peer.breaker.useExternalProbe()
			}
			p.peers[k] = peer
		}
		peer.refs++
		peers = append(peers, peer)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			for _, peer := range peers {
				peer.refs--
			}
			p.sweep()
		})
	}
	return peers, release, nil
}

func (p *storagePool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.sweep()
}

type storagePeersKey struct{}

func (r *Router) queryClient(ctx context.Context, idx int) pb.QueryServiceClient {
	if peers, ok := ctx.Value(storagePeersKey{}).([]*storagePeer); ok {
		return peers[idx].query
	}
	return r.querys[idx]
}
func (r *Router) adminClient(ctx context.Context, idx int) pb.AdminServiceClient {
	if peers, ok := ctx.Value(storagePeersKey{}).([]*storagePeer); ok {
		return peers[idx].admin
	}
	return r.admins[idx]
}

func forwardPeers[T any](r *Router, ctx context.Context, kb string, version int64, snap *storage.Snapshot, peers []*storagePeer, fn func(int, context.Context) (T, error), started time.Time) (T, error) {
	breakers := make([]*breaker, len(peers))
	loads := make([]*inflight, len(peers))
	var primary, other, behind []int
	owners := map[string]bool{}
	if snap != nil {
		k := r.ownerInactiveK
		if snap.ActiveVersion == version {
			k = 0
		} else if r.priority != nil {
			k += r.priority.Tier(kb, version)
		}
		if k > len(peers) {
			k = len(peers)
		}
		for _, addr := range owner.Owners(kb, version, snap.Addresses(0), k) {
			owners[addr] = true
		}
	}
	now := time.Now()
	for i, p := range peers {
		breakers[i], loads[i] = p.breaker, p.load
		switch {
		case kb != "" && p.lag.behind(kb, 0, version, now):
			behind = append(behind, i)
		case owners[p.key.addr]:
			primary = append(primary, i)
		default:
			other = append(other, i)
		}
	}
	var tiers [][]int
	for _, tier := range [][]int{primary, other, behind} {
		if len(tier) > 0 {
			tiers = append(tiers, leastLoadedFirst(tier, loads))
		}
	}
	ctx = context.WithValue(ctx, storagePeersKey{}, peers)
	return forwardTiers(r, ctx, readTrace{kbID: kb, layer: "storage", admission: time.Since(started)}, tiers, breakers, loads, func(idx int, c context.Context) (T, error) {
		result, err := fn(idx, c)
		if cursor, ok := serrors.ReplicaCursorOf(err); ok {
			peers[idx].lag.note(kb, 0, cursor, time.Now())
		}
		return result, err
	})
}

func (p *storagePool) enableProbes() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.probing = true
	for _, peer := range p.peers {
		peer.breaker.useExternalProbe()
	}
}

func (p *storagePool) probe(ctx context.Context) {
	p.mu.Lock()
	var peers []*storagePeer
	if !p.closed {
		for k, peer := range p.peers {
			if p.desired[k] && peer.breaker.needsProbe(time.Now()) {
				peer.refs++
				peers = append(peers, peer)
			}
		}
	}
	p.mu.Unlock()
	for _, peer := range peers {
		c, cancel := context.WithTimeout(ctx, breakerProbeTimeout)
		_, err := peer.syncClient.LocalVersion(c, &pb.LocalVersionRequest{})
		cancel()
		peer.breaker.probeResult(time.Now(), err == nil || !nodeLevelFailure(err))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, peer := range peers {
		peer.refs--
	}
	p.sweep()
}
