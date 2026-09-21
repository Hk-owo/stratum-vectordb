package raft

import (
	"errors"
	"testing"
	"time"

	"stratum/internal/kvraft"
)

// 提案与它的 apply 有两种先后，两种都必须把结果交到提案方手里。
//
// apply 先到（本用例）此前是**丢结果**：`handleEntryMsg` 在 `pending` 里找不到
// waiter 就直接丢弃，而提案方的注册发生在 `Propose` 返回之后 —— 慢机器上这个
// 窗口足够宽（在注册前插 200 ms 延迟可稳定复现）。于是那条提案只能等自己的
// context：对 CI 里用 `context.Background()` 的用例来说就是**永久等待**，表现为
// `internal/raft` 打满整包的 180 s 超时，并且把其余测试的结果一起吞掉。
func TestRaftNodeImpl_SettledResultSurvivesAnEarlyApply(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)

	raw, err := encodeCommand(newCreateKBCommand(testKB("kb-1")))
	if err != nil {
		t.Fatalf("encodeCommand: %v", err)
	}
	// apply 先到：此刻没有任何 waiter 注册过。
	impl.handleEntryMsg(kvraft.ApplyMsg{Index: 7, Term: 3, Command: raw})

	impl.pendingMu.Lock()
	s, done := impl.takeSettledLocked(7)
	impl.pendingMu.Unlock()
	if !done {
		t.Fatal("the applied result was dropped: a proposer that registers after the apply " +
			"would block until its context instead of getting the result")
	}
	if s.term != 3 {
		t.Errorf("settled term = %d, want 3 — registration compares it with the term it proposed", s.term)
	}
	if s.res.Err != nil {
		t.Errorf("settled result carries an error: %v", s.res.Err)
	}

	impl.pendingMu.Lock()
	_, again := impl.takeSettledLocked(7)
	impl.pendingMu.Unlock()
	if again {
		t.Error("a settled result must be consumed once: two consumers means two callers believed they owned it")
	}
}

// 表必须有界：每个被丢掉的唤醒留一条记录，不设上限就是按 races 泄漏。
func TestRaftNodeImpl_SettledResultsAreBounded(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)

	impl.pendingMu.Lock()
	for i := uint64(1); i <= settledResultsLimit*2; i++ {
		impl.rememberSettledLocked(i, 1, applyResult{VersionID: int64(i)})
	}
	size := len(impl.settled)
	newest, hasNewest := impl.settled[settledResultsLimit*2]
	_, hasOldest := impl.settled[1]
	impl.pendingMu.Unlock()

	if size > settledResultsLimit+1 {
		t.Errorf("settled holds %d entries, want <= %d: an unbounded table leaks a result per dropped wakeup",
			size, settledResultsLimit+1)
	}
	if !hasNewest || newest.res.VersionID != int64(settledResultsLimit*2) {
		t.Error("the newest settled result must survive trimming")
	}
	if hasOldest {
		t.Error("superseded entries must be trimmed: apply indexes are monotonic, so a proposer " +
			"that far behind is never coming back")
	}
}

// 已注册的 waiter 拿到的 term 校验必须保持：这个 index 上提交的可能是别的 leader
// 的条目，而不是它提的那条。settled 路径（上面那条用例）用的是同一个判据。
func TestRaftNodeImpl_HandleEntryMsgTellsAWaiterItsEntryWasSuperseded(t *testing.T) {
	impl, _ := newTestRaftNodeImpl(t)

	ch := make(chan applyResult, 1)
	impl.pendingMu.Lock()
	impl.pending[9] = &pendingProposal{term: 2, resultCh: ch}
	impl.pendingMu.Unlock()

	raw, err := encodeCommand(newCreateKBCommand(testKB("kb-1")))
	if err != nil {
		t.Fatalf("encodeCommand: %v", err)
	}
	impl.handleEntryMsg(kvraft.ApplyMsg{Index: 9, Term: 3, Command: raw}) // 不同 term

	select {
	case res := <-ch:
		if !errors.Is(res.Err, errSuperseded) {
			t.Fatalf("waiter got %v, want errSuperseded: a waiter must not be handed another "+
				"leader's entry as its own result", res.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter was never told its entry had been superseded")
	}
}
