//go:build docker
// +build docker

package docker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"stratum/client"
)

// The client/ package against a live cluster. The cases are split by what they
// need from the cluster, because that split is real:
//
//   - submitting, recording and re-sending need only the control tier;
//   - waiting for READY needs the storage tier's index build to work.
//
// A cluster whose index builds are failing would make the second case fail
// through no fault of this package — and mixing them would hide that.

// dialTestClient connects through the station (nodeAddrs in this suite) and keeps
// its records in a temp dir.
func dialTestClient(t *testing.T) *client.Client {
	t.Helper()
	c, err := client.Dial(nodeAddrs[0], t.TempDir())
	if err != nil {
		t.Fatalf("client.Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestT4_ClientSDK_SubmitsRecordsAndReSends covers the caller's loop up to the
// point where the storage tier is involved: the record exists before the request
// leaves, the key the caller holds is the key the cluster was given, and a
// re-send under that key is idempotent.
func TestT4_ClientSDK_SubmitsRecordsAndReSends(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := dialTestClient(t)
	kbID, err := c.CreateKnowledgeBase(ctx,
		fmt.Sprintf("sdk-record-%d", time.Now().UnixNano()),
		"http://stratum-embed:8080", "test-model")
	if err != nil {
		t.Fatalf("CreateKnowledgeBase: %v", err)
	}

	w, err := c.Submit(ctx, kbID, []client.Change{
		{Op: "ADD", DocID: "doc-1", Content: "客户端 SDK：提交、留档、重发。"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if w.ClientRequestID == "" {
		t.Fatal("Submit recorded no idempotency key")
	}
	if w.VersionID == 0 {
		t.Fatal("Submit recorded no version")
	}

	// Durable on the caller's side, with the changes and the key: that copy is
	// what makes a later recovery possible at all.
	stored, ok, err := c.Store().Get(w.ID)
	if err != nil || !ok {
		t.Fatalf("record %s missing (ok=%v, err=%v)", w.ID, ok, err)
	}
	if len(stored.Changes) != 1 || stored.ClientRequestID != w.ClientRequestID || stored.VersionID != w.VersionID {
		t.Errorf("stored record = %+v, want the changes, the key and version %d", stored, w.VersionID)
	}

	// Idempotent against a real cluster: same key, same version.
	again, err := c.Resend(ctx, w.ID)
	if err != nil {
		t.Fatalf("Resend: %v", err)
	}
	if again.VersionID != w.VersionID {
		t.Errorf("re-send allocated version %d, want the original %d", again.VersionID, w.VersionID)
	}

	// And with the changes gone locally, a re-send is refused rather than
	// attempted with an empty payload — the state Decide calls "only discard is
	// left".
	if err := c.ForgetChanges(w.ID); err != nil {
		t.Fatalf("ForgetChanges: %v", err)
	}
	if _, err := c.Resend(ctx, w.ID); err == nil {
		t.Error("Resend without local changes = nil error, want a refusal")
	}
}

// TestT4_ClientSDK_WaitsForReadiness is the other half: the wait, the decision,
// and the way out of a batch that can never land.
//
// It needs the storage tier's index build to work. If it fails while the
// submit/record case above passes, look at the cluster's index builds before
// suspecting this package: on a cluster where `index: Save RPC` is failing, NO
// version reaches READY, and the failure is cluster-wide (the existing
// stress/GCPressure cases fail the same way).
func TestT4_ClientSDK_WaitsForReadinessAndDiscards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	c := dialTestClient(t)
	kbID, err := c.CreateKnowledgeBase(ctx,
		fmt.Sprintf("sdk-wait-%d", time.Now().UnixNano()),
		"http://stratum-embed:8080", "test-model")
	if err != nil {
		t.Fatalf("CreateKnowledgeBase: %v", err)
	}

	w, err := c.Submit(ctx, kbID, []client.Change{
		{Op: "ADD", DocID: "doc-1", Content: "客户端 SDK：等就绪、拿决策。"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	decision, resp, err := c.WaitUntilSettled(ctx, w.ID, 120*time.Second)
	if err != nil {
		t.Fatalf("WaitUntilSettled: %v", err)
	}
	if resp.GetStage() != "INDEX_READY" {
		t.Fatalf("stage = %s, want INDEX_READY (a version that never becomes READY points at the cluster's index builds, not at this client)", resp.GetStage())
	}
	if decision != client.DecisionDone {
		t.Errorf("decision = %v, want DONE", decision)
	}

	// A batch that can never land: the embedder is stopped, so nothing reaches
	// the storage tier.
	//
	// The container name is the same in both supported topologies
	// (scripts/docker-cluster.sh and scripts/docker-cluster-both.sh both call it
	// stratum-embed), so this runs unchanged under CI's all-in-one cluster.
	dockerCmd(t, "stop", "stratum-embed")
	defer dockerCmd(t, "start", "stratum-embed")

	doomed, err := c.Submit(ctx, kbID, []client.Change{
		{Op: "ADD", DocID: "doc-2", Content: "embedder 停着，这篇永远落不了地。"},
	})
	if err != nil {
		t.Fatalf("Submit with the embedder stopped: %v", err)
	}
	if doomed.VersionID == w.VersionID {
		t.Fatalf("the second batch was given version %d, the same as the first: the keys collapsed", doomed.VersionID)
	}

	// Below the probe's age threshold the honest answer is "not decided yet", and
	// the client must not invent one.
	if decision, resp, err := c.WaitUntilSettled(ctx, doomed.ID, 15*time.Second); err == nil && decision != client.DecisionWait {
		t.Errorf("decision for an un-landable write = %v (stage %s), want it still waiting below the probe's threshold",
			decision, resp.GetStage())
	}

	// With the changes gone, discarding is the only move, and doing it twice is
	// harmless.
	if err := c.ForgetChanges(doomed.ID); err != nil {
		t.Fatalf("ForgetChanges: %v", err)
	}
	if err := c.Discard(ctx, doomed.ID); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	if err := c.Discard(ctx, doomed.ID); err != nil {
		t.Errorf("repeated Discard = %v, want it harmless (the server answers discarded=false)", err)
	}
}
