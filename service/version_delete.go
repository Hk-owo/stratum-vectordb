package service

import (
	"context"

	"go.uber.org/zap"

	"stratum/internal/coordinator"
	"stratum/internal/types"
)

// markVersionDeletingProposer is the single raft-node method this helper needs, declared
// locally so that the two services calling it don't have to agree on a node type — only on
// this one operation.
type markVersionDeletingProposer interface {
	ProposeMarkVersionDeleting(ctx context.Context, kbID string, versionID int64, mode types.VersionDeleteMode) ([]int64, error)
}

// markVersionDeletingThenCleanUp proposes the marking and hands the actual cleanup to the
// coordinator in the background.
//
// DeleteVersion (a healthy version) and ForceAbandonVersion (one carrying a FAILED_PERMANENT
// verdict) both funnel through here. They differ only in WHO may ask — each caller settles
// that before calling, and the state machine re-decides it at apply time (§5 contract 7) —
// never in what happens next. The asynchronous handoff and the reason a failure is logged
// rather than returned are identical for both:
//
//   - Execute's contract is to leave the version Deleting once its retries are exhausted,
//     which is exactly the state GetSystemStatus shows the operator. The caller has already
//     been told the version is on its way out, so returning this error would report a failure
//     that the API deliberately does not treat as one;
//   - because Execute re-discovers every Deleting version of the knowledge base, a later
//     delete on the same KB finishes the job. What was missing was knowing that it needed
//     finishing — hence the log line rather than silence.
//
// It lives in one place so the two call sites cannot drift apart: they had already grown two
// separate wordings of the same reasoning.
func markVersionDeletingThenCleanUp(
	ctx context.Context,
	rn markVersionDeletingProposer,
	coord coordinator.DeleteVersionCoordinator,
	logger *zap.Logger,
	kbID string,
	versionID int64,
	mode types.VersionDeleteMode,
) ([]int64, error) {
	deleted, err := rn.ProposeMarkVersionDeleting(ctx, kbID, versionID, mode)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := coord.Execute(context.Background(), kbID); err != nil {
			logger.Warn("version delete: background cleanup did not finish; the version stays in DELETING",
				zap.String("kb_id", kbID), zap.Int64("version_id", versionID), zap.Error(err))
		}
	}()

	return deleted, nil
}
