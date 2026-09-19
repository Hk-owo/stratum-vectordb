package index

import (
	"errors"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	stratumerrors "stratum/internal/errors"
	"stratum/internal/types"
)

// 版本已经不在了（被 DiscardVersion 放弃、或 DeleteVersion 删掉），而索引构建这时
// 可能才刚跑完——它的上报撞上 "version not found" 是**预期结果**，不是失败：
// 放弃一个版本的语义本来就是"当它没存在过"。
//
// 这条用例钉住的是"别把它当故障"：既不该重试（那 200+400+800ms 的退避纯粹是白等），
// 也不该报 error（真正的故障会淹在这片噪音里）。
func TestInvokeCallback_VersionGoneIsNotAFailure(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	im := &IndexManagerImpl{
		cfg:    IndexManagerConfig{CallbackMaxRetries: 3, CallbackRetryBaseMS: 1},
		logger: zap.New(core),
	}

	calls := 0
	im.invokeCallback(func(string, int64, types.IndexStatus) error {
		calls++
		return stratumerrors.ErrVersionNotFound
	}, "kb-1", 7, types.IndexStatusReady)

	if calls != 1 {
		t.Errorf("版本已不存在时不该重试，callback 却被调了 %d 次", calls)
	}
	if n := logs.FilterLevelExact(zap.ErrorLevel).Len(); n != 0 {
		t.Errorf("预期结果不该报 error，却有 %d 条", n)
	}
}

// 其他失败照旧重试到预算耗尽，并且**必须带上原因**——那正是这次修的东西：
// 原实现把 err 的作用域关在 if 里，日志只剩"重试耗尽"，无从判断失败落在哪一环。
func TestInvokeCallback_OtherFailuresRetryAndReportTheCause(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	im := &IndexManagerImpl{
		cfg:    IndexManagerConfig{CallbackMaxRetries: 2, CallbackRetryBaseMS: 1},
		logger: zap.New(core),
	}

	boom := errors.New("control plane unreachable")
	calls := 0
	im.invokeCallback(func(string, int64, types.IndexStatus) error {
		calls++
		return boom
	}, "kb-1", 7, types.IndexStatusReady)

	if calls != 3 { // 首次 + 2 次重试
		t.Errorf("应重试到预算耗尽（3 次），实际 %d 次", calls)
	}

	errs := logs.FilterLevelExact(zap.ErrorLevel).All()
	if len(errs) != 1 {
		t.Fatalf("应恰有 1 条 error，实际 %d 条", len(errs))
	}
	if got := errs[0].ContextMap()["error"]; got != "control plane unreachable" {
		t.Errorf("日志没带上失败原因，error 字段 = %v", got)
	}
}

// 成功即返回：不重试，也不留日志。
func TestInvokeCallback_SuccessIsQuiet(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	im := &IndexManagerImpl{
		cfg:    IndexManagerConfig{CallbackMaxRetries: 3, CallbackRetryBaseMS: 1},
		logger: zap.New(core),
	}

	calls := 0
	im.invokeCallback(func(string, int64, types.IndexStatus) error {
		calls++
		return nil
	}, "kb-1", 7, types.IndexStatusReady)

	if calls != 1 {
		t.Errorf("成功就不该重试，实际 %d 次", calls)
	}
	if logs.Len() != 0 {
		t.Errorf("成功路径不该有日志，实际 %d 条", logs.Len())
	}
}
