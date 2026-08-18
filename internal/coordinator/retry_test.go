package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRetry_ReturnsSuccess verifies retry returns nil as soon as fn
// succeeds.
func TestRetry_ReturnsSuccess(t *testing.T) {
	c := &WriteCoordinatorImpl{cfg: WriteCoordinatorConfig{MaxRetries: 3, RetryBaseIntervalMS: 1}}
	calls := 0
	err := c.retry(context.Background(), func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("retry = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1", calls)
	}
}

// TestRetry_Exhausted verifies the wrapped "retry exhausted" error after
// MaxRetries+1 attempts.
func TestRetry_Exhausted(t *testing.T) {
	c := &WriteCoordinatorImpl{cfg: WriteCoordinatorConfig{MaxRetries: 2, RetryBaseIntervalMS: 1}}
	sentinel := errors.New("flaky")
	calls := 0
	err := c.retry(context.Background(), func() error {
		calls++
		return sentinel
	})
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}
	if calls != 3 { // 2 retries + 1 initial = 3
		t.Errorf("fn called %d times, want 3", calls)
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error must wrap the underlying failure: %v", err)
	}
}

// TestRetry_CancelledBeforeStart verifies a cancelled context short-
// circuits before the first attempt.
func TestRetry_CancelledBeforeStart(t *testing.T) {
	c := &WriteCoordinatorImpl{cfg: WriteCoordinatorConfig{MaxRetries: 3, RetryBaseIntervalMS: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	err := c.retry(ctx, func() error {
		calls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retry = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Errorf("fn called %d times on cancelled context, want 0", calls)
	}
}

// TestRetry_CancelledDuringBackoff verifies a context cancelled while the
// retry loop is sleeping in exponential backoff aborts promptly.
func TestRetry_CancelledDuringBackoff(t *testing.T) {
	c := &WriteCoordinatorImpl{cfg: WriteCoordinatorConfig{MaxRetries: 3, RetryBaseIntervalMS: 1000}}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(10*time.Millisecond, cancel)

	calls := 0
	err := c.retry(ctx, func() error {
		calls++
		return errors.New("always fails")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retry = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1 (cancelled during first backoff)", calls)
	}
}
