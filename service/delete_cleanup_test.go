package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	pb "stratum/api/proto/stratum"
)

// 运行期的清理失败必须说出来。
//
// 这两个方法的契约是：标记落进 Raft 就回 success: true，真正的清理异步跑。而
// Execute 自己的契约是"重试耗尽就把库/版本留在 DELETING，好让 GetSystemStatus
// 把它摆到运维面前"（internal/coordinator/delete_version.go 的注释写明了）。
//
// `_ = s.deleteCoord.Execute(...)` 恰好把最后一步废掉了：状态留在那儿，原因却
// 没了，而调用方早就被告知成功。结果是集群里堆着一堆 DELETING 的东西，没有任何
// 一条线索指向"为什么"——实测在容器集群上堆了 26 个。
func TestDeleteKnowledgeBase_VoicesACleanupThatDidNotFinish(t *testing.T) {
	h := newKBSvcTestHarness()
	core, logs := observer.New(zap.WarnLevel)
	h.svc.SetLogger(zap.New(core))
	kbID, _, _, _ := kbSvcTestHarnessWithChain(t, h)

	boom := errors.New("storage layer refused to drop the version")
	done := make(chan struct{})
	h.deleteC.SetExecuteFunc(func(context.Context, string) error {
		defer close(done)
		return boom
	})

	resp, err := h.svc.DeleteKnowledgeBase(context.Background(), &pb.DeleteKnowledgeBaseRequest{
		KnowledgeBaseId: kbID,
	})
	if err != nil {
		t.Fatalf("DeleteKnowledgeBase: %v", err)
	}
	// 标记已经写进 Raft 了，清理只是异步——接口回成功是对的，要修的不是它。
	if !resp.GetSuccess() {
		t.Error("the mark landed, so the RPC should still report success")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup never ran")
	}

	warns := logs.All()
	if len(warns) != 1 {
		t.Fatalf("got %d warning(s), want exactly 1", len(warns))
	}
	// 关键：带上原因。没有它，DELETING 这个状态对运维是不可行动的。
	if got := warns[0].ContextMap()["error"]; got != boom.Error() {
		t.Errorf("the warning did not carry the cause: error=%v, want %q", got, boom.Error())
	}
}

func TestDeleteVersion_VoicesACleanupThatDidNotFinish(t *testing.T) {
	h := newKBSvcTestHarness()
	core, logs := observer.New(zap.WarnLevel)
	h.svc.SetLogger(zap.New(core))
	kbID, _, v2, _ := kbSvcTestHarnessWithChain(t, h)

	boom := errors.New("index file is gone")
	done := make(chan struct{})
	h.deleteVersionC.SetExecuteFunc(func(context.Context, string) error {
		defer close(done)
		return boom
	})

	// v2 不是活跃版本，且已 READY——那是 DeleteVersion 接受的输入。
	if _, err := h.svc.DeleteVersion(context.Background(), &pb.DeleteVersionRequest{
		KnowledgeBaseId: kbID,
		VersionId:       v2,
		Mode:            pb.VersionDeleteMode_VERSION_DELETE_MODE_SUBTREE,
	}); err != nil {
		t.Fatalf("DeleteVersion: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup never ran")
	}

	warns := logs.All()
	if len(warns) != 1 {
		t.Fatalf("got %d warning(s), want exactly 1", len(warns))
	}
	if got := warns[0].ContextMap()["error"]; got != boom.Error() {
		t.Errorf("the warning did not carry the cause: error=%v, want %q", got, boom.Error())
	}
}

// 清理成功时不该有噪音。这条钉住的是"别把日志加过头"——一个每次删库都响的
// warning 会和这次修的缺陷一样没用。
func TestDeleteKnowledgeBase_QuietWhenCleanupSucceeds(t *testing.T) {
	h := newKBSvcTestHarness()
	core, logs := observer.New(zap.WarnLevel)
	h.svc.SetLogger(zap.New(core))
	kbID, _, _, _ := kbSvcTestHarnessWithChain(t, h)

	done := make(chan struct{})
	h.deleteC.SetExecuteFunc(func(context.Context, string) error {
		defer close(done)
		return nil
	})

	if _, err := h.svc.DeleteKnowledgeBase(context.Background(), &pb.DeleteKnowledgeBaseRequest{
		KnowledgeBaseId: kbID,
	}); err != nil {
		t.Fatalf("DeleteKnowledgeBase: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup never ran")
	}

	if n := logs.Len(); n != 0 {
		t.Errorf("a successful cleanup logged %d line(s), want 0", n)
	}
}
