package router

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	pb "stratum/api/proto/stratum"
	"stratum/internal/storage"
)

func poolDirectory(revision uint64, nodes ...storage.Node) storage.Directory {
	d := storage.NewDirectory()
	d.Revision = revision
	for _, n := range nodes {
		d.Nodes[n.ID] = n
	}
	return d
}

func TestStoragePoolRetainsInflightEndpointAndRejectsRollback(t *testing.T) {
	p := newStoragePool()
	defer p.close()
	old := storage.Node{ID: 11, Addr: "old:9000", State: storage.NodeActive}
	next := storage.Node{ID: 11, Addr: "new:9000", State: storage.NodeActive}
	p.reconcile(poolDirectory(1, old))
	held, release, err := p.acquire([]storage.Node{old})
	if err != nil {
		t.Fatal(err)
	}
	p.reconcile(poolDirectory(2, next))
	if held[0].conn.GetState() == connectivity.Shutdown {
		t.Fatal("closed an inflight endpoint")
	}
	fresh, done, err := p.acquire([]storage.Node{next})
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	if held[0] == fresh[0] || held[0].breaker == fresh[0].breaker || held[0].lag == fresh[0].lag {
		t.Fatal("endpoint generations share failure state")
	}
	p.reconcile(poolDirectory(1, old))
	if p.revision != 2 || !p.desired[fresh[0].key] {
		t.Fatal("old snapshot reverted directory")
	}
	release()
	release()
	if held[0].conn.GetState() != connectivity.Shutdown {
		t.Fatal("retired connection leaked")
	}
	// A late request can still finish against its captured view without making
	// the obsolete endpoint current again.
	late, finish, err := p.acquire([]storage.Node{old})
	if err != nil {
		t.Fatal(err)
	}
	finish()
	if late[0].conn.GetState() != connectivity.Shutdown || p.desired[late[0].key] {
		t.Fatal("late acquisition revived endpoint")
	}
	p.close()
	if fresh[0].conn.GetState() == connectivity.Shutdown {
		t.Fatal("Close interrupted an active call")
	}
	done()
	if fresh[0].conn.GetState() != connectivity.Shutdown {
		t.Fatal("Close did not drain")
	}
	if _, _, err := p.acquire([]storage.Node{next}); err == nil {
		t.Fatal("acquisition after close succeeded")
	}
}

func TestStoragePoolDialFailureReleasesEarlierAcquisitions(t *testing.T) {
	p := newStoragePool()
	defer p.close()
	dial := p.dial
	p.dial = func(addr string) (*grpc.ClientConn, error) {
		if addr == "bad" {
			return nil, errors.New("dial failed")
		}
		return dial(addr)
	}
	if _, _, err := p.acquire([]storage.Node{{ID: 1, Addr: "first:9000"}, {ID: 2, Addr: "bad"}}); err == nil {
		t.Fatal("missing error")
	}
	if len(p.peers) != 0 {
		t.Fatal("partially acquired endpoints leaked")
	}
}

func TestStoragePoolConcurrentAcquisitionUsesStableNodeState(t *testing.T) {
	p := newStoragePool()
	defer p.close()
	n := storage.Node{ID: 9, Addr: "node:9000"}
	p.reconcile(poolDirectory(1, n))
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			peers, release, err := p.acquire([]storage.Node{n})
			if err != nil {
				t.Error(err)
				return
			}
			peers[0].load.n.Add(1)
			peers[0].load.n.Add(-1)
			release()
		}()
	}
	wg.Wait()
	if len(p.peers) != 1 {
		t.Fatal("node identity duplicated")
	}
	for _, peer := range p.peers {
		if peer.refs != 0 || peer.load.n.Load() != 0 {
			t.Fatal("references leaked")
		}
	}
}

func TestDirectoryDynamicPoolDiscoversUnconfiguredNode(t *testing.T) {
	d := poolDirectory(1, storage.Node{ID: 99, Addr: "new:9000", State: storage.NodeActive})
	if err := d.AssignNew(storage.Placement{KBID: "new-kb", Epoch: 1, ReplicaIDs: []int64{99}, RequiredReplicas: 1, OperationID: "new"}); err != nil {
		t.Fatal(err)
	}
	r := directoryRouter(t, func(context.Context) (routeSnapshot, error) { return routeSnapshot{directory: &d}, nil })
	r.storagePool = newStoragePool()
	defer r.Close()
	r.storagePool.reconcile(d)
	_, err := Forward(r, context.Background(), pb.QueryService_Query_FullMethodName, "new-kb", func(idx int, ctx context.Context) (int, error) {
		peers := ctx.Value(storagePeersKey{}).([]*storagePeer)
		if peers[idx].key.id != 99 || r.queryClient(ctx, idx) != peers[idx].query {
			t.Fatal("used a startup slot")
		}
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Cluster-wide storage calls must also use dynamic membership.
	_, err = Forward(r, context.Background(), pb.AdminService_HealthCheck_FullMethodName, "", func(idx int, ctx context.Context) (int, error) {
		peers := ctx.Value(storagePeersKey{}).([]*storagePeer)
		if peers[idx].key.id != 99 || r.adminClient(ctx, idx) != peers[idx].admin {
			t.Fatal("cluster method used startup nodes")
		}
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type poolProbeClient struct {
	pb.DataSyncServiceClient
	entered chan struct{}
	resume  chan struct{}
}

func (c poolProbeClient) LocalVersion(ctx context.Context, _ *pb.LocalVersionRequest, _ ...grpc.CallOption) (*pb.LocalVersionResponse, error) {
	close(c.entered)
	select {
	case <-c.resume:
		return &pb.LocalVersionResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestStoragePoolRetirementWaitsForBreakerProbe(t *testing.T) {
	p := newStoragePool()
	defer p.close()
	p.enableProbes()
	n := storage.Node{ID: 4, Addr: "node:9000"}
	p.reconcile(poolDirectory(1, n))
	held, release, err := p.acquire([]storage.Node{n})
	if err != nil {
		t.Fatal(err)
	}
	peer := held[0]
	entered, resume := make(chan struct{}), make(chan struct{})
	peer.syncClient = poolProbeClient{entered: entered, resume: resume}
	peer.breaker.mu.Lock()
	peer.breaker.state = breakerOpen
	peer.breaker.opened = time.Now().Add(-2 * defaultBreakerConfig.cooldown)
	peer.breaker.mu.Unlock()
	release()
	finished := make(chan struct{})
	go func() { p.probe(context.Background()); close(finished) }()
	<-entered
	p.reconcile(poolDirectory(2))
	if peer.conn.GetState() == connectivity.Shutdown {
		t.Fatal("retirement cancelled active probe")
	}
	close(resume)
	<-finished
	if peer.conn.GetState() != connectivity.Shutdown {
		t.Fatal("probe leaked retired endpoint")
	}
}
