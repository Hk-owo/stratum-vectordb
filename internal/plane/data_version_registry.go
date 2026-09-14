package plane

import (
	"sort"
	"sync"
	"time"
)

// DataVersionRegistry is the control leader's in-memory aggregate of the storage
// layer's periodic reports (§7.13.4): which version each node holds, per
// knowledge base.
//
// **Hard boundary — this is SOFT state.** It is never written to Raft and never
// feeds a correctness decision. §10.6's cleanup deletes real data, so acting on a
// stale "nobody has it" would delete the only copy; conversely a report that was
// never received must not be read as "the node does not have it". The registry
// answers "which node should I ask to serve version V", and nothing more. Whoever
// decides to delete must confirm against each node directly.
//
// It is also scoped to one leadership term: a new leader starts empty rather than
// inheriting a predecessor's view (which may describe nodes that have since
// restarted with wiped disks).
type DataVersionRegistry struct {
	mu     sync.RWMutex
	byNode map[int64]*nodeDataVersions
}

type nodeDataVersions struct {
	cursors    map[string]int64
	reportedAt time.Time
}

// NewDataVersionRegistry returns an empty registry.
func NewDataVersionRegistry() *DataVersionRegistry {
	return &DataVersionRegistry{byNode: make(map[int64]*nodeDataVersions)}
}

// Record stores one node's report, replacing its previous one wholesale: each
// report is a complete statement of that node's cursors (the reporter sends every
// KB it knows), so merging would keep resurrecting knowledge bases it has since
// dropped.
func (r *DataVersionRegistry) Record(nodeID int64, dataVersions map[string]int64) {
	cursors := make(map[string]int64, len(dataVersions))
	for kbID, version := range dataVersions {
		cursors[kbID] = version
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byNode[nodeID] = &nodeDataVersions{cursors: cursors, reportedAt: time.Now()}
}

// Holders returns, in ascending node order, the nodes whose recorded cursor
// reaches versionID for kbID. A node that has never reported is simply absent —
// which is why an empty result means "no one I have heard from", never "no one
// has it".
func (r *DataVersionRegistry) Holders(kbID string, versionID int64) []int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var holders []int64
	for nodeID, data := range r.byNode {
		if cursor, ok := data.cursors[kbID]; ok && cursor >= versionID {
			holders = append(holders, nodeID)
		}
	}
	sort.Slice(holders, func(i, j int) bool { return holders[i] < holders[j] })
	return holders
}

// Cursor returns the highest version nodeID reported for kbID, and whether that
// node has reported at all.
func (r *DataVersionRegistry) Cursor(nodeID int64, kbID string) (int64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	data, ok := r.byNode[nodeID]
	if !ok {
		return 0, false
	}
	cursor, ok := data.cursors[kbID]
	return cursor, ok
}

// ReportedAt returns when nodeID last reported, and whether it has reported.
func (r *DataVersionRegistry) ReportedAt(nodeID int64) (time.Time, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	data, ok := r.byNode[nodeID]
	if !ok {
		return time.Time{}, false
	}
	return data.reportedAt, true
}

// Nodes returns the nodes that have reported, in ascending order.
func (r *DataVersionRegistry) Nodes() []int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]int64, 0, len(r.byNode))
	for nodeID := range r.byNode {
		nodes = append(nodes, nodeID)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

// Forget drops one node's view. The caller uses it when a node leaves the
// cluster: keeping its last report would let a departed node keep "vouching" for
// versions it no longer holds.
func (r *DataVersionRegistry) Forget(nodeID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byNode, nodeID)
}

// Reset drops every node's view. A leadership change calls it: the aggregate is
// per-term soft state, and inheriting it would mean answering "who holds V" from
// reports the new leader never received.
func (r *DataVersionRegistry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byNode = make(map[int64]*nodeDataVersions)
}
