package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A handler panic must cost one failed call, not the node.
func TestRecoveryUnaryTurnsPanicIntoInternal(t *testing.T) {
	var logged []string
	gate := RecoveryUnary(func(format string, args ...any) {
		logged = append(logged, fmt.Sprintf(format, args...))
	})

	_, err := gate(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/stratum.QueryService/Query"},
		func(context.Context, any) (any, error) {
			panic("index out of range: makeslice: len out of range")
		})

	if status.Code(err) != codes.Internal {
		t.Fatalf("panic must surface as INTERNAL, got %v (%v)", status.Code(err), err)
	}
	// The panic value stays in the log. It can describe internal state, and the
	// caller can do nothing with it.
	if strings.Contains(err.Error(), "makeslice") {
		t.Errorf("the panic value leaked to the caller: %v", err)
	}
	if len(logged) != 1 {
		t.Fatalf("expected one log line for the panic, got %d: %v", len(logged), logged)
	}
	if !strings.Contains(logged[0], "/stratum.QueryService/Query") {
		t.Errorf("the log line must name the method that panicked, got %q", logged[0])
	}
	if !strings.Contains(logged[0], "makeslice") {
		t.Errorf("the log line must carry the panic value, got %q", logged[0])
	}
	if !strings.Contains(logged[0], "goroutine") {
		t.Errorf("the log line must carry the stack, got %q", logged[0])
	}
}

func TestRecoveryUnaryPassesNormalCallsThrough(t *testing.T) {
	gate := RecoveryUnary(func(string, ...any) {})
	want := errors.New("handler error")

	_, err := gate(context.Background(), "req",
		&grpc.UnaryServerInfo{FullMethod: "/stratum.AdminService/GetSystemStatus"},
		func(context.Context, any) (any, error) { return nil, want })

	if !errors.Is(err, want) {
		t.Fatalf("a non-panicking handler's error must pass through unchanged, got %v", err)
	}
}

func TestRecoveryStreamTurnsPanicIntoInternal(t *testing.T) {
	gate := RecoveryStream(func(string, ...any) {})

	err := gate(nil, nil,
		&grpc.StreamServerInfo{FullMethod: "/stratum.DataSyncService/StreamSomething"},
		func(any, grpc.ServerStream) error { panic("boom") })

	if status.Code(err) != codes.Internal {
		t.Fatalf("panic must surface as INTERNAL, got %v (%v)", status.Code(err), err)
	}
}
