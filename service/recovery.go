package service

import (
	"context"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryUnary turns a panic in a handler into an INTERNAL status instead of a
// dead process.
//
// Why it is not optional: a gRPC handler panic is recovered by nothing by
// default. The panic unwinds out of the service implementation, past the
// generated code, and terminates the whole node — so any single request that
// trips an unchecked path (an allocation sized from a client field, a nil
// dereference on a half-built state) takes down every other client's traffic
// with it, and the node restarts into the same reachable code. A malformed
// request must cost one failed call.
//
// The panic's value never reaches the caller: it can carry internal state, and
// the caller cannot act on it anyway. It goes to the log with the stack, which
// is the only place it is useful.
//
// It belongs at the FRONT of the interceptor chain, so it also covers the
// interceptors behind it (see cmd/stratum/main.go).
func RecoveryUnary(logf func(format string, args ...any)) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logPanic(logf, info.FullMethod, r)
				err = status.Errorf(codes.Internal, "%s: internal error", info.FullMethod)
			}
		}()
		return handler(ctx, req)
	}
}

// RecoveryStream is RecoveryUnary for streaming calls. Every RPC in this system
// is unary today; this exists so the protection cannot be half-installed by
// adding a streaming method later and forgetting the stream side.
func RecoveryStream(logf func(format string, args ...any)) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logPanic(logf, info.FullMethod, r)
				err = status.Errorf(codes.Internal, "%s: internal error", info.FullMethod)
			}
		}()
		return handler(srv, ss)
	}
}

// logPanic records a recovered panic with the method and the stack it happened
// on. The stack is the point: the status returned to the caller says only
// "internal error", so this line is the whole of the evidence.
func logPanic(logf func(format string, args ...any), fullMethod string, recovered any) {
	if logf == nil {
		return
	}
	logf("grpc: recovered panic in %s: %v\n%s", fullMethod, recovered, debug.Stack())
}
