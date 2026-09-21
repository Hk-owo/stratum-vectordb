package kvraft

import (
	"go.uber.org/zap"
	"testing"

	kvraftpb "stratum/api/proto/kvraft"
)

// 复现 CI 上那个 panic 的形状：日志被截断到 3 条（基点 10），而 nextIndex 还是
// "当 leader 时"记录的旧末尾 + 1（17）。修复前 `logEntry(16)` 直接
// `index out of range [6] with length 3`；现在钳到这条日志能服务的上界。
func TestTransport_ClampsAReplicationIndexPastTheEndOfTheLog(t *testing.T) {
	rf, _, _ := newTestRaft(t, 1, WithLogger(zap.NewNop()))

	rf.mu.Lock()
	rf.state = Leader
	rf.log = []*kvraftpb.Entry{{Index: 10, Term: 1}, {Index: 11, Term: 1}, {Index: 12, Term: 1}}
	got := rf.clampNextIndex(2, 17)
	// 用它索引日志不再 panic —— 这才是那个 bug 的全部后果。
	prev := rf.logEntry(got - 1)
	rf.mu.Unlock()

	if got != 13 {
		t.Errorf("clamped nextIndex = %d, want 13 (lastLogIndex+1): anything larger panics in logEntry", got)
	}
	if prev.Index != 12 {
		t.Errorf("prev entry = %d, want 12", prev.Index)
	}
}

// follower 的 ConflictIndex 是**它自己**的 lastLogIndex+1，而它可能持有本 leader
// 没有的旧任期未提交尾巴 ⇒ 这个值可以指向本日志之外。采纳时必须钳制，否则下一次
// replicateToPeer 就 panic。
func TestTransport_ClampsAFollowersConflictIndex(t *testing.T) {
	rf, tr, _ := newTestRaft(t, 1, WithLogger(zap.NewNop()))

	rf.mu.Lock()
	rf.state = Leader
	rf.term = 7
	rf.log = []*kvraftpb.Entry{{Index: 10, Term: 1}, {Index: 11, Term: 1}, {Index: 12, Term: 1}}
	rf.nextIndex[2] = 5
	tr.applyAppendResponse(rf, 2,
		&kvraftpb.AppendEntriesRequest{PrevLogIndex: 4},
		&kvraftpb.AppendEntriesResponse{Term: 7, Success: false, ConflictIndex: 99})
	got := rf.nextIndex[2]
	rf.mu.Unlock()

	if got != 13 {
		t.Errorf("nextIndex = %d, want 13 (clamped to lastLogIndex+1): a follower's ConflictIndex can "+
			"point past this leader's log, and adopting it unclamped panics in logEntry", got)
	}
}

// ConflictTerm 命中时给出的必须是**全局** index，而不是切片下标 + 1：日志被快照
// 压缩过之后后者会落在基点之下（这里是 2，而基点是 10），于是 leader 以为对方"太
// 落后、该发快照"，白白多一轮。
func TestTransport_ConflictTermResolvesToAGlobalIndexNotASliceOffset(t *testing.T) {
	rf, tr, _ := newTestRaft(t, 1, WithLogger(zap.NewNop()))

	rf.mu.Lock()
	rf.state = Leader
	rf.term = 7
	rf.log = []*kvraftpb.Entry{{Index: 10, Term: 9}, {Index: 11, Term: 1}, {Index: 12, Term: 2}}
	rf.nextIndex[2] = 11
	rf.matchIndex[2] = 10
	tr.applyAppendResponse(rf, 2,
		&kvraftpb.AppendEntriesRequest{PrevLogIndex: 12},
		&kvraftpb.AppendEntriesResponse{Term: 7, Success: false, ConflictIndex: 12, ConflictTerm: 1})
	got := rf.nextIndex[2]
	rf.mu.Unlock()

	if got != 12 {
		t.Errorf("nextIndex = %d, want 12 (rf.log[i].Index+1 for the matching term); a value of 2 means "+
			"the slice offset was used as a global index, which sits below the log base %d", got, 10)
	}
}

// 成功响应的 match/next 推进保持原样（这条不修行为，只是把抽出来的响应处理钉住）。
func TestTransport_ApplyAppendResponseAdvancesMatchOnSuccess(t *testing.T) {
	rf, tr, _ := newTestRaft(t, 1, WithLogger(zap.NewNop()))

	rf.mu.Lock()
	rf.state = Leader
	rf.term = 7
	rf.log = []*kvraftpb.Entry{{Index: 10, Term: 1}, {Index: 11, Term: 1}, {Index: 12, Term: 1}}
	rf.nextIndex[2] = 10
	tr.applyAppendResponse(rf, 2,
		&kvraftpb.AppendEntriesRequest{PrevLogIndex: 11, Entries: []*kvraftpb.Entry{{Index: 12, Term: 7}}},
		&kvraftpb.AppendEntriesResponse{Term: 7, Success: true})
	next, match := rf.nextIndex[2], rf.matchIndex[2]
	rf.mu.Unlock()

	if match != 12 || next != 13 {
		t.Errorf("match/next = %d/%d, want 12/13", match, next)
	}
}

// 发现更高任期后必须退位（既有的 step-down 行为，同样钉住）。
func TestTransport_ApplyAppendResponseStepsDownOnAHigherTerm(t *testing.T) {
	rf, tr, _ := newTestRaft(t, 1, WithLogger(zap.NewNop()))

	rf.mu.Lock()
	rf.state = Leader
	rf.term = 7
	tr.applyAppendResponse(rf, 2,
		&kvraftpb.AppendEntriesRequest{},
		&kvraftpb.AppendEntriesResponse{Term: 9, Success: false})
	state, term := rf.state, rf.term
	rf.mu.Unlock()

	if state != Follower || term != 9 {
		t.Errorf("state/term = %v/%d, want Follower/9", state, term)
	}
}
