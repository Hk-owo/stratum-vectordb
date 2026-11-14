package main

import (
	"context"
	"fmt"
	"sort"

	"stratum/internal/raft"
)

// clusterTopology is the deployment shape the assembly asks about over and over:
// which address belongs to which node id, which addresses can hold version data
// at all, this node's own address as its peers see it, and the replica set a
// write must reach.
//
// It is one value rather than four locals in main() because every consumer needs
// the SAME answers: the data-source lookup, the dispatcher's candidate list, the
// control layer's required-replica verdict and the fallback that asks the leader
// for data all derive from one declaration, and deriving them separately is how
// they drift. The numbers themselves are untouched — each method below is the
// body that used to sit inline in main().
type clusterTopology struct {
	// peerAddrByID maps a Raft member id to its Stratum gRPC address. Members
	// that declare no service address are absent: they cannot be reached for
	// data at all, so every consumer would otherwise have to filter them again.
	peerAddrByID map[int64]string

	// dataSourceAddrs is the set of addresses that can hold version data at all:
	// the declared storage group when there is one, otherwise every Raft member
	// (before the split every member also stored — the same fallback
	// resolveReplicaAddrs makes below).
	//
	// It exists to keep the data-source lookup from naming a CONTROL node in a
	// split deployment; see the resolver's third layer for what that cost.
	dataSourceAddrs map[string]bool

	// localAddr is this node's own service address as its peers see it: the
	// address the dispatcher falls back to for local coordination, and the one a
	// writer announces so replicas can find the data it just wrote. A storage
	// node appears in the storage group rather than the Raft member list.
	localAddr string

	// storageNodes is the declared storage group (empty = every Raft member
	// stores), kept so the two derivations below cannot disagree about which
	// shape the deployment is.
	storageNodes []NodeRef
	peerCount    int
	selfID       int64
}

// newClusterTopology derives the deployment shape from the configuration.
func newClusterTopology(cfg appConfig) *clusterTopology {
	t := &clusterTopology{
		peerAddrByID:    make(map[int64]string, len(cfg.Peers)),
		dataSourceAddrs: make(map[string]bool, len(cfg.StorageNodes)),
		storageNodes:    cfg.StorageNodes,
		peerCount:       len(cfg.Peers),
		selfID:          cfg.NodeID,
	}
	for _, p := range cfg.Peers {
		if p.ServiceAddr != "" {
			t.peerAddrByID[p.ID] = p.ServiceAddr
		}
	}
	if len(t.storageNodes) > 0 {
		for _, n := range t.storageNodes {
			t.dataSourceAddrs[n.Addr] = true
		}
	} else {
		for _, addr := range t.peerAddrByID {
			t.dataSourceAddrs[addr] = true
		}
	}
	t.localAddr = t.peerAddrByID[cfg.NodeID]
	for _, node := range t.storageNodes {
		if node.ID == cfg.NodeID {
			t.localAddr = node.Addr
		}
	}
	return t
}

// peerAddr is the reachable address of a Raft member, if it declares one.
func (t *clusterTopology) peerAddr(id int64) (string, bool) {
	addr, ok := t.peerAddrByID[id]
	return addr, ok
}

// canHoldData reports whether addr is a node that stores version data.
func (t *clusterTopology) canHoldData(addr string) bool {
	return t.dataSourceAddrs[addr]
}

// replicaCount is how many replicas should hold a written version: the declared
// storage group when there is one, the Raft members otherwise.
func (t *clusterTopology) replicaCount() int {
	if len(t.storageNodes) > 0 {
		return len(t.storageNodes)
	}
	return t.peerCount
}

// resolveReplicaAddrs is the replica topology the write dispatcher walks
// (§7.13.2) — the same list §8.4 distribution and the §8.5 confirmation use:
// every member holds the full dataset, so the candidates are the peers other
// than this node. Consumed through a closure so the assembly order does not
// matter.
func (t *clusterTopology) resolveReplicaAddrs(_ context.Context) ([]string, error) {
	// The declared storage group when there is one, otherwise the Raft
	// members: before the split every member also stored, and a deployment
	// that declares no storage.nodes keeps exactly that shape.
	if len(t.storageNodes) > 0 {
		addrs := make([]string, 0, len(t.storageNodes))
		for _, node := range t.storageNodes {
			if node.ID == t.selfID {
				continue // this node already holds its own copy
			}
			addrs = append(addrs, node.Addr)
		}
		return addrs, nil
	}
	addrs := make([]string, 0, len(t.peerAddrByID))
	for id, addr := range t.peerAddrByID {
		if id == t.selfID {
			continue // this node already holds its own copy
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// requiredReplicaIDs is the replica set a write must reach, for the control
// layer's redundancy verdict. It comes from the topology — never from who has
// reported — so a node that is merely silent cannot drop out of the requirement.
func (t *clusterTopology) requiredReplicaIDs() ([]int64, error) {
	var ids []int64
	if len(t.storageNodes) > 0 {
		ids = make([]int64, 0, len(t.storageNodes))
		for _, node := range t.storageNodes {
			ids = append(ids, node.ID)
		}
	} else {
		ids = make([]int64, 0, len(t.peerAddrByID))
		for id := range t.peerAddrByID {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// resolveControlLeader answers "where is the control leader, as an address this
// node can dial". It is the resolver every peer-facing helper re-asks rather
// than caching: a leader change must not leave the reporter talking to the old
// one forever.
func (t *clusterTopology) resolveControlLeader(rn raft.RaftNode) func(context.Context) (string, bool, error) {
	return func(rctx context.Context) (string, bool, error) {
		status, err := rn.GetClusterStatus(rctx)
		if err != nil {
			return "", false, fmt.Errorf("GetClusterStatus: %w", err)
		}
		if !status.HasLeader {
			return "", false, nil
		}
		addr, ok := t.peerAddr(status.LeaderID)
		if !ok {
			return "", false, fmt.Errorf("sync: leader address unknown for node ID %d", status.LeaderID)
		}
		return addr, true, nil
	}
}
