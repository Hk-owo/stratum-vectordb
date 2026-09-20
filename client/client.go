package client

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "stratum/api/proto/stratum"
)

// Client talks to a Stratum cluster through ONE address: the service station,
// or any control node when the deployment has no station. Every call it makes is
// a plain RPC — what makes it a client rather than a wrapper is that it also
// keeps the caller's half of the contract: the local record, and the decision
// that only the caller can make (docs/client-integration-guide.md §1).
type Client struct {
	conn  *grpc.ClientConn
	kb    pb.KnowledgeBaseServiceClient
	query pb.QueryServiceClient
	admin pb.AdminServiceClient
	store *Store

	// now and newID are injectable so tests can hold the clock and the key
	// generator still.
	now   func() time.Time
	newID func() string
}

// Dial connects to addr and records this client's work under stateDir.
func Dial(addr, stateDir string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}
	store, err := NewStore(stateDir)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Client{
		conn:  conn,
		kb:    pb.NewKnowledgeBaseServiceClient(conn),
		query: pb.NewQueryServiceClient(conn),
		admin: pb.NewAdminServiceClient(conn),
		store: store,
		now:   time.Now,
		newID: newRequestID,
	}, nil
}

// Close releases the connection. The local records stay on disk, which is the
// point: they outlive this process.
func (c *Client) Close() error { return c.conn.Close() }

// Store exposes the local records so a caller can list or inspect them.
func (c *Client) Store() *Store { return c.store }

// CreateKnowledgeBase provisions a knowledge base and returns its ID.
//
// The chunking parameters are fixed here rather than exposed: they are immutable
// after creation and only affect how text is split, which is not something a
// caller of this size should be choosing at startup.
func (c *Client) CreateKnowledgeBase(ctx context.Context, name, embedAddr, modelID string) (string, error) {
	resp, err := c.kb.CreateKnowledgeBase(ctx, &pb.CreateKnowledgeBaseRequest{
		Name:             name,
		ChunkWindowSize:  512,
		ChunkOverlapSize: 64,
		EmbedConfig:      &pb.EmbedConfig{ServiceAddr: embedAddr, ModelId: modelID},
	})
	if err != nil {
		return "", fmt.Errorf("client: CreateKnowledgeBase: %w", err)
	}
	return resp.GetKnowledgeBaseId(), nil
}

// Submit sends one batch and records it locally. Two orderings are deliberate:
//
//   - the record is written BEFORE the request leaves. A submission the cluster
//     accepted but nobody wrote down is exactly the write that can never be
//     recovered, and the window between the two is the caller's only chance to
//     create one;
//   - the idempotency key is minted here, not by the server, so the caller knows
//     it even if the response never arrives (the server echoes it back as well,
//     §7 Step 4 — both ends now agree on the same value).
func (c *Client) Submit(ctx context.Context, kbID string, changes []Change) (PendingWrite, error) {
	if len(changes) == 0 {
		return PendingWrite{}, fmt.Errorf("client: Submit with no changes: the server refuses an empty batch (empty_changes)")
	}
	for _, ch := range changes {
		if _, err := ch.ToProto(); err != nil {
			return PendingWrite{}, err // validate before recording anything
		}
	}

	key := c.newID()
	record := PendingWrite{
		ID:              key,
		KnowledgeBaseID: kbID,
		ClientRequestID: key,
		Changes:         changes,
		SubmittedAt:     c.now(),
	}
	if err := c.store.Upsert(record); err != nil {
		return PendingWrite{}, err
	}

	versionID, err := c.createVersion(ctx, record)
	if err != nil {
		// The record stays, with VersionID still zero: the caller has a key and
		// its changes, so it can re-send under the same key and land on the same
		// version the first attempt may still be allocating.
		return record, err
	}
	record.VersionID = versionID
	if err := c.store.Upsert(record); err != nil {
		return record, err
	}
	return record, nil
}

// createVersion is the one write call, shared by Submit and Resend: a re-send
// must not differ from the original, or it stops being a re-send.
func (c *Client) createVersion(ctx context.Context, w PendingWrite) (int64, error) {
	pbChanges := make([]*pb.DocChange, 0, len(w.Changes))
	for _, ch := range w.Changes {
		out, err := ch.ToProto()
		if err != nil {
			return 0, err
		}
		pbChanges = append(pbChanges, out)
	}
	resp, err := c.kb.CreateVersion(ctx, &pb.CreateVersionRequest{
		KnowledgeBaseId: w.KnowledgeBaseID,
		ClientRequestId: w.ClientRequestID,
		Changes:         pbChanges,
	})
	if err != nil {
		return 0, fmt.Errorf("client: CreateVersion(%s): %w", w.KnowledgeBaseID, err)
	}
	return resp.GetVersionId(), nil
}

// Resend re-sends a recorded batch under the SAME key, which is what makes the
// retry idempotent: the same version comes back instead of a second one.
func (c *Client) Resend(ctx context.Context, id string) (PendingWrite, error) {
	w, ok, err := c.store.Get(id)
	if err != nil {
		return PendingWrite{}, err
	}
	if !ok {
		return PendingWrite{}, fmt.Errorf("client: no pending write %s", id)
	}
	if len(w.Changes) == 0 {
		// Not an error the caller can retry through: without the changes there is
		// nothing to re-send, and only Discard is left.
		return w, fmt.Errorf("client: %s has no recorded changes; this process can only discard it now", id)
	}

	versionID, err := c.createVersion(ctx, w)
	if err != nil {
		return w, err
	}
	if w.VersionID != 0 && versionID != w.VersionID {
		// The server must hand back the same version for the same key. Saying so
		// beats overwriting the record: a disagreement here means the record and
		// the cluster read the same key differently.
		return w, fmt.Errorf("client: re-send under key %s allocated version %d, but the record holds %d",
			w.ClientRequestID, versionID, w.VersionID)
	}
	w.VersionID = versionID
	if err := c.store.Upsert(w); err != nil {
		return w, err
	}
	return w, nil
}

// AwaitOnce asks the cluster once about a recorded batch.
func (c *Client) AwaitOnce(ctx context.Context, id string) (PendingWrite, *pb.AwaitVersionResponse, error) {
	w, ok, err := c.store.Get(id)
	if err != nil {
		return PendingWrite{}, nil, err
	}
	if !ok {
		return PendingWrite{}, nil, fmt.Errorf("client: no pending write %s", id)
	}
	if w.VersionID == 0 {
		return w, nil, fmt.Errorf("client: %s has no version yet; its submission did not come back — re-send it (same key) first", id)
	}
	resp, err := c.kb.AwaitVersion(ctx, &pb.AwaitVersionRequest{
		KnowledgeBaseId: w.KnowledgeBaseID,
		VersionId:       w.VersionID,
		Target:          pb.AwaitTarget_AWAIT_TARGET_INDEX_READY,
		WaitTimeoutMs:   int64(awaitCallWindow / time.Millisecond),
	})
	if err != nil {
		return w, nil, fmt.Errorf("client: AwaitVersion(%s/%d): %w", w.KnowledgeBaseID, w.VersionID, err)
	}
	return w, resp, nil
}

const (
	// awaitCallWindow is what this client asks the server to wait per call. The
	// server caps it anyway; asking for the cap means fewer round trips.
	awaitCallWindow = 5 * time.Second
	// awaitBreath is the pause between calls when the answer was "wait".
	awaitBreath = 200 * time.Millisecond
)

// Decision is what the caller should do about a recorded batch right now.
func (c *Client) Decision(ctx context.Context, id string) (Decision, *pb.AwaitVersionResponse, error) {
	w, resp, err := c.AwaitOnce(ctx, id)
	if err != nil {
		return DecisionWait, nil, err
	}
	return Decide(resp, len(w.Changes) > 0), resp, nil
}

// WaitUntilSettled polls until the batch reaches a decision that is not WAIT.
//
// It returns the decision and does NOT act on it. Re-sending and discarding are
// the caller's calls to make — that is the entire point of §2.1 — so a library
// that silently did either would be making them behind the caller's back.
func (c *Client) WaitUntilSettled(ctx context.Context, id string, timeout time.Duration) (Decision, *pb.AwaitVersionResponse, error) {
	deadline := time.Now().Add(timeout)
	for {
		decision, resp, err := c.Decision(ctx, id)
		if err != nil {
			return DecisionWait, nil, err
		}
		if decision != DecisionWait {
			return decision, resp, nil
		}
		if !time.Now().Before(deadline) {
			return DecisionWait, resp, fmt.Errorf("client: %s did not settle within %s", id, timeout)
		}
		select {
		case <-time.After(awaitBreath):
		case <-ctx.Done():
			return DecisionWait, resp, ctx.Err()
		}
	}
}

// Discard abandons a version whose write never landed.
//
// It is the caller's last move for a batch it cannot re-send; a version whose
// data did land is refused by the server (version_not_pending), and that refusal
// is passed through rather than retried — DeleteVersion is the right call there,
// and this client deliberately does not guess between them.
func (c *Client) Discard(ctx context.Context, id string) error {
	w, ok, err := c.store.Get(id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("client: no pending write %s", id)
	}
	if w.VersionID == 0 {
		return fmt.Errorf("client: %s has no version to discard (its submission never came back)", id)
	}
	if _, err := c.kb.DiscardVersion(ctx, &pb.DiscardVersionRequest{
		KnowledgeBaseId: w.KnowledgeBaseID,
		VersionId:       w.VersionID,
	}); err != nil {
		return fmt.Errorf("client: DiscardVersion(%s/%d): %w", w.KnowledgeBaseID, w.VersionID, err)
	}
	w.Settled = true
	return c.store.Upsert(w)
}

// ForgetChanges drops the local copy of a batch's changes while keeping the key
// and version number.
//
// It exists because "my own data is gone" is a thing that happens to callers, and
// a client that cannot represent it would push callers into lying to themselves:
// after this call the record still identifies the version, but no re-send is
// possible, which is exactly the state Decide's haveChanges argument describes.
func (c *Client) ForgetChanges(id string) error {
	w, ok, err := c.store.Get(id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("client: no pending write %s", id)
	}
	w.Changes = nil
	return c.store.Upsert(w)
}

// MarkSettled records that a batch is finished with, so it stops showing up as
// work in progress.
func (c *Client) MarkSettled(id string) error {
	w, ok, err := c.store.Get(id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("client: no pending write %s", id)
	}
	w.Settled = true
	return c.store.Upsert(w)
}

// Query asks one vector question. The vector is the caller's: Stratum never
// embeds a query for anyone (docs/client-integration-guide.md §6).
func (c *Client) Query(ctx context.Context, kbID string, versionID int64, vector []float32, topK int) (*pb.QueryResponse, error) {
	req := &pb.QueryRequest{KnowledgeBaseId: kbID, Vector: vector, TopK: int32(topK)}
	if versionID != 0 {
		req.VersionId = &versionID
	}
	resp, err := c.query.Query(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("client: Query(%s): %w", kbID, err)
	}
	return resp, nil
}

// ListFailedVersions returns the versions the control layer declared
// FAILED_PERMANENT, each with the cause chain that says which half died
// (Stratum_设计文档v13.md §10.1). An empty knowledgeBaseID asks across every
// knowledge base — "this one has a version stuck" and "is anything else stuck?"
// are different questions, and both are worth answering.
//
// A read: nothing retries or abandons anything on its own. That decision is the
// operator's, which is the whole point of the verdict being terminal for machines.
func (c *Client) ListFailedVersions(ctx context.Context, knowledgeBaseID string) ([]*pb.FailedVersion, error) {
	resp, err := c.admin.ListFailedVersions(ctx, &pb.ListFailedVersionsRequest{KnowledgeBaseId: knowledgeBaseID})
	if err != nil {
		return nil, fmt.Errorf("client: list failed versions: %w", err)
	}
	return resp.GetVersions(), nil
}

// ForceRetryVersion revokes an INDEX-side FAILED_PERMANENT verdict and asks for
// another build (§10.1).
//
// The data side is not retryable, and the server says so instead of accepting the
// call: that verdict means the version's data will never arrive, so there is nothing
// to rebuild from. The error is passed through unaltered — it names
// ForceAbandonVersion as the operation that does apply, and re-wording it here would
// throw away the one part an operator can act on.
func (c *Client) ForceRetryVersion(ctx context.Context, knowledgeBaseID string, versionID int64) error {
	if _, err := c.admin.ForceRetryVersion(ctx, &pb.ForceRetryVersionRequest{
		KnowledgeBaseId: knowledgeBaseID,
		VersionId:       versionID,
	}); err != nil {
		return fmt.Errorf("client: force retry version %d: %w", versionID, err)
	}
	return nil
}

// ForceAbandonVersion abandons a FAILED_PERMANENT version: it leaves the chain under
// DeleteVersion's SINGLE semantics (any child is spliced onto its parent), and the
// physical cleanup runs asynchronously. Returns the versions marked for deletion.
//
// Only a version carrying a verdict can be abandoned — a healthy version is
// DeleteVersion's business, and the server refuses it here.
func (c *Client) ForceAbandonVersion(ctx context.Context, knowledgeBaseID string, versionID int64) ([]int64, error) {
	resp, err := c.admin.ForceAbandonVersion(ctx, &pb.ForceAbandonVersionRequest{
		KnowledgeBaseId: knowledgeBaseID,
		VersionId:       versionID,
	})
	if err != nil {
		return nil, fmt.Errorf("client: force abandon version %d: %w", versionID, err)
	}
	return resp.GetDeletedVersionIds(), nil
}
