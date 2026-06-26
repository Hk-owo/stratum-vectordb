package coordinator

import (
	"context"
	"errors"
	"testing"

	"stratum/internal/types"
)

func TestMockWriteCoordinator_DefaultAndConfigured(t *testing.T) {
	ctx := context.Background()
	c := NewMockWriteCoordinator()

	changes := []types.DocChange{{Op: types.ChangeOpAdd, DocID: "doc1", Content: "hello"}}
	versionID, err := c.Execute(ctx, "kb1", 0, changes)
	if err != nil {
		t.Fatalf("Execute (default): %v", err)
	}
	if versionID != 1 {
		t.Fatalf("versionID = %d, want default 1", versionID)
	}

	injected := errors.New("write failed")
	c.SetExecuteResult(0, injected)
	if _, err := c.Execute(ctx, "kb1", 1, changes); !errors.Is(err, injected) {
		t.Fatalf("err = %v, want injected", err)
	}

	calls := c.Calls()
	if len(calls) != 2 {
		t.Fatalf("recorded %d calls, want 2", len(calls))
	}
	if calls[0].KBID != "kb1" || calls[0].ParentVersionID != 0 {
		t.Fatalf("call[0] = %+v, want kb1/parent=0", calls[0])
	}
	if calls[1].ParentVersionID != 1 {
		t.Fatalf("call[1] ParentVersionID = %d, want 1", calls[1].ParentVersionID)
	}
}

func TestMockWriteCoordinator_CustomFunc(t *testing.T) {
	c := NewMockWriteCoordinator()
	c.SetExecuteFunc(func(_ context.Context, kbID string, parentVersionID int64, changes []types.DocChange) (int64, error) {
		return parentVersionID + 100, nil
	})
	versionID, err := c.Execute(context.Background(), "kb1", 5, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if versionID != 105 {
		t.Fatalf("versionID = %d, want 105 (from custom func)", versionID)
	}
}

func TestMockWriteCoordinator_Reset(t *testing.T) {
	c := NewMockWriteCoordinator()
	c.Execute(context.Background(), "kb1", 0, nil)
	c.Reset()
	if len(c.Calls()) != 0 {
		t.Fatalf("calls not cleared after Reset")
	}
	versionID, err := c.Execute(context.Background(), "kb1", 0, nil)
	if err != nil || versionID != 1 {
		t.Fatalf("Execute after Reset = (%d, %v), want (1, nil)", versionID, err)
	}
}

func TestMockDeleteCoordinator_DefaultAndConfigured(t *testing.T) {
	ctx := context.Background()
	c := NewMockDeleteCoordinator()

	if err := c.Execute(ctx, "kb1"); err != nil {
		t.Fatalf("Execute (default): %v", err)
	}

	injected := errors.New("delete failed")
	c.SetExecuteResult(injected)
	if err := c.Execute(ctx, "kb2"); !errors.Is(err, injected) {
		t.Fatalf("err = %v, want injected", err)
	}

	calls := c.Calls()
	if len(calls) != 2 || calls[0] != "kb1" || calls[1] != "kb2" {
		t.Fatalf("calls = %v, want [kb1 kb2]", calls)
	}
}

func TestMockDeleteCoordinator_CustomFunc(t *testing.T) {
	c := NewMockDeleteCoordinator()
	var seen string
	c.SetExecuteFunc(func(_ context.Context, kbID string) error {
		seen = kbID
		return nil
	})
	if err := c.Execute(context.Background(), "kb-xyz"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if seen != "kb-xyz" {
		t.Fatalf("custom func saw kbID = %q, want kb-xyz", seen)
	}
}
