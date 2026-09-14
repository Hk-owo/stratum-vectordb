package service

import (
	"context"

	"stratum/internal/types"
)

// PresenceChecker asks one replica whether it holds a version's data.
// *sync.PresenceChecker implements it over gRPC; tests substitute a stub.
type PresenceChecker interface {
	HasVersion(ctx context.Context, replicaAddr, kbID string, versionID int64) (bool, error)
}

// dataMissingVersions returns the PENDING versions that no candidate replica
// holds.
//
// This is the distinction the control layer could not make before: a PENDING
// version normally means "storage writes are in flight, or the index is still
// building". But if none of the replicas that were supposed to receive the
// version holds it, its data never landed at all — the writer died between
// allocating the version and writing it, or every push failed. Such a version
// is only recoverable by the client re-sending its changes under the same
// client_request_id (Stratum_设计文档v13.md §7.12), so it has to be visible
// rather than silently PENDING forever.
//
// Only versions older than minAgeSec are probed: one allocated a moment ago may
// legitimately still be mid-write, and probing its replicas would produce a
// false alarm on every status call.
//
// An unreachable replica counts as "unknown", not "missing": a version is only
// reported when at least one replica answered and none of the answering ones
// holds it. Reporting because a node happened to be down would be worse than
// reporting nothing.
func dataMissingVersions(
	ctx context.Context,
	versions []types.VersionMeta,
	replicas []string,
	checker PresenceChecker,
	nowUnix int64,
	minAgeSec int64,
) []types.VersionMeta {
	if checker == nil || len(replicas) == 0 {
		return nil
	}
	var out []types.VersionMeta
	for _, v := range versions {
		if v.IndexStatus != types.IndexStatusPending || v.Deleting {
			continue
		}
		if nowUnix-v.CreatedAt < minAgeSec {
			continue // writes may legitimately still be in flight
		}
		if held, reachable := versionPresence(ctx, v, replicas, checker); !reachable || held {
			continue
		}
		out = append(out, v)
	}
	return out
}

// versionPresence asks every replica whether it holds the version, reporting
// whether any replica holds it and whether any replica was reachable at all.
func versionPresence(ctx context.Context, v types.VersionMeta, replicas []string, checker PresenceChecker) (held, reachable bool) {
	for _, addr := range replicas {
		present, err := checker.HasVersion(ctx, addr, v.KBID, v.VersionID)
		if err != nil {
			continue
		}
		reachable = true
		if present {
			return true, true
		}
	}
	return false, reachable
}
