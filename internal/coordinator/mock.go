package coordinator

import (
	"context"
	"sync"

	"stratum/internal/types"
)

// MockWriteCoordinator is a configurable, call-recording stand-in for
// WriteCoordinator, for use in unit tests of the Service layer
// (KnowledgeBaseService.CreateVersion) before the real orchestration
// implementation exists (Phase 5-A). It does not perform any actual
// orchestration — it returns whatever has been configured via
// SetExecuteResult / SetExecuteFunc, and records every call it receives so
// tests can assert on what the Service layer passed through.
type MockWriteCoordinator struct {
	mu sync.Mutex

	nextVersionID int64
	nextErr       error
	executeFunc   func(ctx context.Context, kbID string, parentVersionID int64, changes []types.DocChange) (int64, error)

	replayErr  error
	replayFunc func(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) error

	calls       []WriteCoordinatorCall
	replayCalls []ReplayVersionCall
}

// WriteCoordinatorCall records a single Execute invocation for test
// assertions.
type WriteCoordinatorCall struct {
	KBID            string
	ParentVersionID int64
	Changes         []types.DocChange
	// ClientRequestID is what the service layer passed down. It is recorded
	// because §7 Step 4's contract is about the key's JOURNEY: generated or
	// echoed, it has to reach the coordinator under the same value the caller is
	// told about.
	ClientRequestID string
}

// ReplayVersionCall records a single ReplayVersionStorageWrites
// invocation for test assertions.
type ReplayVersionCall struct {
	KBID            string
	ParentVersionID int64
	VersionID       int64
	Changes         []types.DocChange
}

// NewMockWriteCoordinator constructs a MockWriteCoordinator that returns
// versionID 1 and a nil error by default.
func NewMockWriteCoordinator() *MockWriteCoordinator {
	return &MockWriteCoordinator{nextVersionID: 1}
}

func (c *MockWriteCoordinator) Execute(ctx context.Context, kbID string, parentVersionID int64, changes []types.DocChange, clientRequestID string) (int64, error) {
	c.mu.Lock()
	c.calls = append(c.calls, WriteCoordinatorCall{KBID: kbID, ParentVersionID: parentVersionID, Changes: changes, ClientRequestID: clientRequestID})
	fn := c.executeFunc
	versionID, err := c.nextVersionID, c.nextErr
	c.mu.Unlock()

	if fn != nil {
		return fn(ctx, kbID, parentVersionID, changes)
	}
	return versionID, err
}

// ReplayVersionStorageWrites implements WriteCoordinator for the mock: it
// records the call for assertions and returns nil by default, the
// configured replayErr when set, or the result of the configured
// replayFunc (which takes precedence over replayErr).
func (c *MockWriteCoordinator) ReplayVersionStorageWrites(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) error {
	c.mu.Lock()
	c.replayCalls = append(c.replayCalls, ReplayVersionCall{KBID: kbID, ParentVersionID: parentVersionID, VersionID: versionID, Changes: changes})
	fn := c.replayFunc
	err := c.replayErr
	c.mu.Unlock()

	if fn != nil {
		return fn(ctx, kbID, parentVersionID, versionID, changes)
	}
	return err
}

// SetReplayResult configures the error returned by subsequent
// ReplayVersionStorageWrites calls.
func (c *MockWriteCoordinator) SetReplayResult(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replayErr = err
	c.replayFunc = nil
}

// SetReplayFunc configures a full custom ReplayVersionStorageWrites
// implementation, for tests that need per-call behaviour (e.g. failing a
// fixed number of times before succeeding, to exercise the crash-recovery
// retry path). Overrides any SetReplayResult configuration.
func (c *MockWriteCoordinator) SetReplayFunc(fn func(ctx context.Context, kbID string, parentVersionID, versionID int64, changes []types.DocChange) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replayFunc = fn
	c.replayErr = nil
}

// ReplayCalls returns every ReplayVersionStorageWrites call recorded so
// far.
func (c *MockWriteCoordinator) ReplayCalls() []ReplayVersionCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ReplayVersionCall(nil), c.replayCalls...)
}

// SetExecuteResult configures the (versionID, error) pair returned by
// subsequent Execute calls, overriding any SetExecuteFunc configuration.
func (c *MockWriteCoordinator) SetExecuteResult(versionID int64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextVersionID = versionID
	c.nextErr = err
	c.executeFunc = nil
}

// SetExecuteFunc configures a full custom Execute implementation, for
// tests that need per-call logic (e.g. returning different results based
// on input, or simulating a slow call that respects ctx).
func (c *MockWriteCoordinator) SetExecuteFunc(fn func(ctx context.Context, kbID string, parentVersionID int64, changes []types.DocChange) (int64, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.executeFunc = fn
}

// Calls returns every Execute call recorded so far.
func (c *MockWriteCoordinator) Calls() []WriteCoordinatorCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]WriteCoordinatorCall(nil), c.calls...)
}

// Reset clears recorded calls and configuration back to defaults.
// Convenience for tests; not part of the WriteCoordinator interface.
func (c *MockWriteCoordinator) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = nil
	c.replayCalls = nil
	c.nextVersionID = 1
	c.nextErr = nil
	c.executeFunc = nil
	c.replayErr = nil
	c.replayFunc = nil
}

var _ WriteCoordinator = (*MockWriteCoordinator)(nil)

// MockDeleteCoordinator is the DeleteCoordinator analog of
// MockWriteCoordinator: a configurable, call-recording stand-in for use in
// Service-layer tests before the real Phase 5-B implementation exists.
type MockDeleteCoordinator struct {
	mu sync.Mutex

	nextErr     error
	executeFunc func(ctx context.Context, kbID string) error

	calls []string // kbIDs passed to Execute, in call order
}

// NewMockDeleteCoordinator constructs a MockDeleteCoordinator that returns
// nil error by default.
func NewMockDeleteCoordinator() *MockDeleteCoordinator {
	return &MockDeleteCoordinator{}
}

func (c *MockDeleteCoordinator) Execute(ctx context.Context, kbID string) error {
	c.mu.Lock()
	c.calls = append(c.calls, kbID)
	fn := c.executeFunc
	err := c.nextErr
	c.mu.Unlock()

	if fn != nil {
		return fn(ctx, kbID)
	}
	return err
}

// SetExecuteResult configures the error returned by subsequent Execute
// calls, overriding any SetExecuteFunc configuration.
func (c *MockDeleteCoordinator) SetExecuteResult(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextErr = err
	c.executeFunc = nil
}

// SetExecuteFunc configures a full custom Execute implementation.
func (c *MockDeleteCoordinator) SetExecuteFunc(fn func(ctx context.Context, kbID string) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.executeFunc = fn
}

// Calls returns every kbID passed to Execute so far, in call order.
func (c *MockDeleteCoordinator) Calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

// Reset clears recorded calls and configuration back to defaults.
// Convenience for tests; not part of the DeleteCoordinator interface.
func (c *MockDeleteCoordinator) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = nil
	c.nextErr = nil
	c.executeFunc = nil
}

var _ DeleteCoordinator = (*MockDeleteCoordinator)(nil)

// MockDeleteVersionCoordinator is the DeleteVersionCoordinator analog of
// MockDeleteCoordinator: a configurable, call-recording stand-in for use
// in Service-layer tests.
type MockDeleteVersionCoordinator struct {
	mu sync.Mutex

	nextErr     error
	executeFunc func(ctx context.Context, kbID string) error

	// resumeErr / resumeFunc are kept separate from the Execute pair: a test that wants
	// "everything fails" has to say so for both, and one that exercises the crash path
	// should not have its sibling scan (Execute) silently configured by the same knob.
	resumeErr  error
	resumeFunc func(ctx context.Context, kbID string, versionID int64) error

	calls       []string // kbIDs passed to Execute, in call order
	resumeCalls []VersionDeleteResume
}

// VersionDeleteResume records one ResumeVersionDelete call.
type VersionDeleteResume struct {
	KBID      string
	VersionID int64
}

// NewMockDeleteVersionCoordinator constructs a MockDeleteVersionCoordinator
// that returns nil error by default.
func NewMockDeleteVersionCoordinator() *MockDeleteVersionCoordinator {
	return &MockDeleteVersionCoordinator{}
}

func (c *MockDeleteVersionCoordinator) Execute(ctx context.Context, kbID string) error {
	c.mu.Lock()
	c.calls = append(c.calls, kbID)
	fn := c.executeFunc
	err := c.nextErr
	c.mu.Unlock()

	if fn != nil {
		return fn(ctx, kbID)
	}
	return err
}

// ResumeVersionDelete records the call and returns the configured result.
func (c *MockDeleteVersionCoordinator) ResumeVersionDelete(ctx context.Context, kbID string, versionID int64) error {
	c.mu.Lock()
	c.resumeCalls = append(c.resumeCalls, VersionDeleteResume{KBID: kbID, VersionID: versionID})
	fn := c.resumeFunc
	err := c.resumeErr
	c.mu.Unlock()

	if fn != nil {
		return fn(ctx, kbID, versionID)
	}
	return err
}

// SetExecuteResult configures the error returned by subsequent Execute
// calls, overriding any SetExecuteFunc configuration.
func (c *MockDeleteVersionCoordinator) SetExecuteResult(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextErr = err
	c.executeFunc = nil
}

// SetExecuteFunc configures a full custom Execute implementation.
func (c *MockDeleteVersionCoordinator) SetExecuteFunc(fn func(ctx context.Context, kbID string) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.executeFunc = fn
}

// SetResumeResult configures the error returned by subsequent ResumeVersionDelete
// calls, overriding any SetResumeFunc configuration.
func (c *MockDeleteVersionCoordinator) SetResumeResult(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeErr = err
	c.resumeFunc = nil
}

// SetResumeFunc configures a full custom ResumeVersionDelete implementation.
func (c *MockDeleteVersionCoordinator) SetResumeFunc(fn func(ctx context.Context, kbID string, versionID int64) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeFunc = fn
}

// Calls returns every kbID passed to Execute so far, in call order.
func (c *MockDeleteVersionCoordinator) Calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

// ResumeCalls returns every ResumeVersionDelete call so far, in call order.
func (c *MockDeleteVersionCoordinator) ResumeCalls() []VersionDeleteResume {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]VersionDeleteResume(nil), c.resumeCalls...)
}

var _ DeleteVersionCoordinator = (*MockDeleteVersionCoordinator)(nil)
