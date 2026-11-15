package main

import (
	"reflect"
	"testing"

	"stratum/internal/raft"
	"stratum/internal/storage"
)

func TestTopologyDirectoryRegistersOriginalFullStorageSet(t *testing.T) {
	topology := newClusterTopology(appConfig{NodeID: 9, Peers: []raft.PeerConfig{{ID: 9, ServiceAddr: "control"}}, StorageNodes: []NodeRef{{ID: 3, Addr: "s3"}, {ID: 1, Addr: "s1"}, {ID: 2, Addr: "s2"}}})
	got, err := topology.directoryNodes()
	want := []storage.Node{{ID: 1, Addr: "s1", State: storage.NodeActive}, {ID: 2, Addr: "s2", State: storage.NodeActive}, {ID: 3, Addr: "s3", State: storage.NodeActive}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestTopologyDirectoryRejectsMissingLegacyReplicaAddresses(t *testing.T) {
	for _, cfg := range []appConfig{
		{Peers: []raft.PeerConfig{{ID: 1, ServiceAddr: "s1"}, {ID: 2}}},
		{StorageNodes: []NodeRef{{ID: 1, Addr: "s1"}, {ID: 2}}},
		{},
	} {
		if _, err := newClusterTopology(cfg).directoryNodes(); err == nil {
			t.Fatalf("incomplete topology accepted: %+v", cfg)
		}
	}
}

func TestTopologyDirectoryIncludesSelfInAllInOneDeployment(t *testing.T) {
	topology := newClusterTopology(appConfig{NodeID: 1, Peers: []raft.PeerConfig{{ID: 1, ServiceAddr: "s1"}}})
	nodes, err := topology.directoryNodes()
	if err != nil || len(nodes) != 1 || nodes[0].ID != 1 {
		t.Fatalf("single replica: %+v %v", nodes, err)
	}
}

func TestTopologyDirectoryLegacySingleNodeUsesDeclaredLocalBind(t *testing.T) {
	for _, peers := range [][]raft.PeerConfig{nil, {{ID: 1}}} {
		topology := newClusterTopology(appConfig{NodeID: 1, GRPCAddr: "0.0.0.0:7000", Peers: peers})
		nodes, err := topology.directoryNodes()
		if err != nil || len(nodes) != 1 || nodes[0].Addr != "localhost:7000" {
			t.Fatalf("single-node compatibility: %v %v", nodes, err)
		}
	}
}
