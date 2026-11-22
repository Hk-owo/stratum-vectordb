package router

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	pb "stratum/api/proto/stratum"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/owner"
)

// storageDegradation asks the control layer for the storage layer's redundancy verdict.
// Only the leader can judge, so the question goes round the control set until a node can.
func (r *Router) storageDegradation(ctx context.Context) (degradationVerdict, bool) {
	var lastErr error
	for i := range r.kbs {
		resp, err := r.kbs[i].GetStorageDegradation(ctx, &pb.GetStorageDegradationRequest{})
		if err != nil {
			lastErr = err
			continue
		}
		if !resp.GetKnown() {
			continue
		}
		return degradationVerdict{
			degraded:    resp.GetDegraded(),
			unavailable: resp.GetStorageUnavailable(),
			detail:      resp.GetDetail(),
		}, true
	}
	r.loggerOrNop().Debug("route table: no control node could judge storage redundancy", zap.Error(lastErr))
	return degradationVerdict{}, false
}

// routeTiers orders a storage-layer call's nodes into the tiers it tries them in:
//
//  1. the version's index OWNERS, which is what keeps every node's LRU from filling
//     with the same working set (R3 of docs/code-review-2026-10-14.md);
//  2. everyone else, because an owner can be behind or down while another replica
//     holds the data, and that replica's answer is just as correct;
//  3. nodes a recent refusal says are behind on this version, last rather than never.
//     A record is only a hint: the node may have caught up since, and when nobody else
//     could answer, asking it again is better than refusing on the strength of a hint.
//
// Nobody is ruled out. The freshness credential on the forwarded query is what makes a
// node that does not hold the version refuse, so the order only decides how many round
// trips finding the right node costs.
func (r *Router) routeTiers(kbID string, versionID int64, n int, now time.Time) [][]int {
	if r.routes == nil {
		return [][]int{allIndexes(n)}
	}
	var owners map[string]bool
	if r.ownerInactiveK > 0 && len(r.storageAddrs) > 0 {
		owners = r.ownersOf(kbID, versionID)
	}
	var primary, others, behind []int
	for _, idx := range allIndexes(n) {
		switch {
		case versionID > 0 && r.lag.behind(kbID, idx, versionID, now):
			behind = append(behind, idx)
		case owners == nil || (idx < len(r.storageAddrs) && owners[r.storageAddrs[idx]]):
			primary = append(primary, idx)
		default:
			others = append(others, idx)
		}
	}
	tiers := make([][]int, 0, 3)
	for _, tier := range [][]int{primary, others, behind} {
		if len(tier) > 0 {
			tiers = append(tiers, tier)
		}
	}
	return tiers
}

// ownerKFor decides how many replicas should hold this version's index.
//
// Two rules, in order:
//
//   - The ACTIVE version — the one the routing table says this knowledge base
//     serves — keeps the whole replica set (k = 0, "every member"). Any node may
//     be asked for it, so all of them are expected to hold its index.
//   - Everything else starts at OwnerInactiveK and climbs one rung per priority
//     tier, capped at the replica count. A version the cluster keeps querying earns
//     more owners; one it stops querying lets them go. With no half-life
//     configured the ladder is not consulted at all and k is simply the base.
func (r *Router) ownerKFor(kbID string, versionID int64) int {
	if expected, ok := r.ExpectedVersion(kbID); ok && expected == versionID {
		return 0
	}
	k := r.ownerInactiveK
	if r.priority != nil {
		k += r.priority.Tier(kbID, versionID)
		if k > len(r.storageAddrs) {
			k = len(r.storageAddrs)
		}
	}
	return k
}

// ownersOf computes the index owner set for one version, as the set of addresses
// that should hold it.
func (r *Router) ownersOf(kbID string, versionID int64) map[string]bool {
	owners := owner.Owners(kbID, versionID, r.storageAddrs, r.ownerKFor(kbID, versionID))
	if len(owners) == 0 {
		return nil
	}
	set := make(map[string]bool, len(owners))
	for _, addr := range owners {
		set[addr] = true
	}
	return set
}

// ExpectedVersion exposes the control layer's answer for kbID, so a caller can
// stamp it on the request it forwards (§9.3(2)).
func (r *Router) ExpectedVersion(kbID string) (int64, bool) {
	if r.routes == nil {
		return 0, false
	}
	return r.routes.ExpectedVersion(kbID)
}

// writeGate refuses a write the routing cache already knows cannot reach quorum
// (docs/已实施/storage-degradation-signal-plan.md §4.3, first line).
//
// This is the station's half of the gate and deliberately the cheap half: it
// answers from the snapshot the background refresh already took, so the common
// case costs a map lookup and the doomed control-layer round trip never happens.
// The control layer checks again regardless, because a request can reach it
// without passing a station at all.
//
// Unknown ALWAYS allows: no verdict in the snapshot, or an unwired table. The snapshot is periodic and what it carries is soft state,
// so it can be wrong in either direction — which is why the refusal is retryable
// (both sentinels are codes.Unavailable) and why this signal never decides
// correctness, only fails earlier and with a reason the caller can read.
//
// Which sentinel it carries follows the leader's verdict: the two tiers get the
// two names, so a client sees the same error whether the station caught it or the
// control layer did (§4.1).
func (r *Router) writeGate(kbID string) error {
	if r.routes == nil || kbID == "" {
		return nil
	}
	verdict, known := r.routes.Degradation()
	if r.requireDirectory {
		verdict, known = r.routes.KBDegradation(kbID)
	}
	if !known || !verdict.unhealthy() {
		return nil
	}
	sentinel := stratumerrors.ErrKBStorageDegraded
	if verdict.unavailable {
		sentinel = stratumerrors.ErrStorageUnavailable
	}
	return stratumerrors.ToGRPCStatus(fmt.Errorf("%w: %s: %s", sentinel, kbID, verdict.detail))
}
