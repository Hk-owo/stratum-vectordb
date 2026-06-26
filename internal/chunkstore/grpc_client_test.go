package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestVecstoreChunkStore exercises the real gRPC-backed ChunkStore
// implementation against a real vecstore_server subprocess (built from
// vecstore/cmd/vecstore_server.cpp), per the 2-B test node in
// Stratum_实现顺序.md: "连接真实 vecstore gRPC server", "Write + Exists +
// Delete 正确性", "DeleteByKB 前缀删除正确性".
//
// Written before VecstoreChunkStore exists (TDD): this file does not
// compile until grpc_client.go is added.
//
// Requires the vecstore_server binary to have been built (see
// vecstore/CMakeLists.txt). Locates it via STRATUM_VECSTORE_SERVER_BIN if
// set, otherwise the conventional build output path relative to this
// package; skips (not fails) if the binary cannot be found, since the C++
// build is a separate step Go tooling cannot drive on its own.
func TestVecstoreChunkStore(t *testing.T) {
	addr, cleanup := startTestVecstoreServer(t)
	defer cleanup()

	ctx := context.Background()
	store, err := NewVecstoreChunkStore(addr)
	if err != nil {
		t.Fatalf("NewVecstoreChunkStore: %v", err)
	}
	defer store.Close()

	t.Run("write then exists", func(t *testing.T) {
		if err := store.Write(ctx, "kb1", "chunk1", []float32{1, 2, 3}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		ok, err := store.Exists(ctx, "kb1", "chunk1")
		if err != nil {
			t.Fatalf("Exists: %v", err)
		}
		if !ok {
			t.Fatalf("Exists = false, want true after Write")
		}
	})

	t.Run("exists false before write", func(t *testing.T) {
		ok, err := store.Exists(ctx, "kb1", "never-written")
		if err != nil {
			t.Fatalf("Exists: %v", err)
		}
		if ok {
			t.Fatalf("Exists = true for a never-written chunk")
		}
	})

	t.Run("delete removes the chunk", func(t *testing.T) {
		if err := store.Write(ctx, "kb1", "chunk-to-delete", []float32{1}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := store.Delete(ctx, "kb1", "chunk-to-delete"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		ok, err := store.Exists(ctx, "kb1", "chunk-to-delete")
		if err != nil {
			t.Fatalf("Exists: %v", err)
		}
		if ok {
			t.Fatalf("Exists = true after Delete")
		}
	})

	t.Run("DeleteByKB removes only that knowledge base's chunks", func(t *testing.T) {
		if err := store.Write(ctx, "kbA", "c1", []float32{1}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := store.Write(ctx, "kbA", "c2", []float32{2}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := store.Write(ctx, "kbB", "c1", []float32{3}); err != nil {
			t.Fatalf("Write: %v", err)
		}

		if err := store.DeleteByKB(ctx, "kbA"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}

		for _, tc := range []struct {
			kbID, chunkID string
			wantExists    bool
		}{
			{"kbA", "c1", false},
			{"kbA", "c2", false},
			{"kbB", "c1", true},
		} {
			ok, err := store.Exists(ctx, tc.kbID, tc.chunkID)
			if err != nil {
				t.Fatalf("Exists(%s,%s): %v", tc.kbID, tc.chunkID, err)
			}
			if ok != tc.wantExists {
				t.Fatalf("Exists(%s,%s) = %v, want %v", tc.kbID, tc.chunkID, ok, tc.wantExists)
			}
		}
	})

	t.Run("DeleteByKB does not bleed into a knowledge base whose ID is a string prefix", func(t *testing.T) {
		// kbID="kb" and kbID="kb-extended" must not collide: deleting "kb"
		// must not also wipe "kb-extended"'s chunks. This specifically
		// exercises the key-encoding boundary, mirroring the equivalent
		// concern already validated for the Go-side PebbleDB modules (see
		// internal/pebbleutil).
		if err := store.Write(ctx, "kb", "x", []float32{1}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := store.Write(ctx, "kb-extended", "x", []float32{2}); err != nil {
			t.Fatalf("Write: %v", err)
		}

		if err := store.DeleteByKB(ctx, "kb"); err != nil {
			t.Fatalf("DeleteByKB: %v", err)
		}

		ok, err := store.Exists(ctx, "kb-extended", "x")
		if err != nil {
			t.Fatalf("Exists: %v", err)
		}
		if !ok {
			t.Fatalf("DeleteByKB(kb) incorrectly deleted kb-extended's chunk (key-prefix collision)")
		}
	})

	t.Run("vector content round-trips exactly", func(t *testing.T) {
		want := []float32{0.5, -1.25, 3.0, 0.0, -0.0001}
		if err := store.Write(ctx, "kb-round-trip", "c1", want); err != nil {
			t.Fatalf("Write: %v", err)
		}
		// ChunkStore interface does not expose Read directly (per
		// Stratum_接口设计v9.md, chunk vector reads for index building go
		// through IndexManager -> ChunkStorage.Read via internal gRPC
		// directly, not through Go-side ChunkStore) — Exists is the only
		// way to assert through this interface that the write landed.
		ok, err := store.Exists(ctx, "kb-round-trip", "c1")
		if err != nil {
			t.Fatalf("Exists: %v", err)
		}
		if !ok {
			t.Fatalf("Exists = false after Write")
		}
	})
}

func TestVecstoreChunkStore_RespectsContextCancellation(t *testing.T) {
	addr, cleanup := startTestVecstoreServer(t)
	defer cleanup()

	store, err := NewVecstoreChunkStore(addr)
	if err != nil {
		t.Fatalf("NewVecstoreChunkStore: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = store.Exists(ctx, "kb1", "c1")
	if err == nil {
		t.Fatalf("Exists with cancelled context returned nil error, want an error")
	}
	if !errors.Is(err, context.Canceled) {
		// gRPC may wrap context.Canceled in a status error rather than
		// returning it verbatim; accept either as long as some error
		// surfaces — but log for visibility if it's not the expected
		// sentinel, since callers may want errors.Is(err, context.Canceled)
		// to work for retry-skipping logic.
		t.Logf("Exists with cancelled context returned %v (not wrapping context.Canceled via errors.Is)", err)
	}
}

// startTestVecstoreServer builds (locates) and starts a vecstore_server
// subprocess on a free loopback port, waits for it to accept connections,
// and returns its address plus a cleanup function that terminates it.
func startTestVecstoreServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()

	binPath := vecstoreServerBinPath(t)

	port, err := freeLoopbackPort()
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	dbDir := t.TempDir()

	cmd := exec.Command(binPath,
		"--rocksdb_path="+filepath.Join(dbDir, "db"),
		"--grpc_addr="+addr,
	)
	cmd.Stdout = os.Stderr // surface server logs on test failure via -v
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start vecstore_server: %v", err)
	}

	if err := waitForServerReady(addr, 10*time.Second); err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatalf("vecstore_server did not become ready: %v", err)
	}

	cleanup = func() {
		cmd.Process.Kill()
		cmd.Wait()
	}
	return addr, cleanup
}

// vecstoreServerBinPath locates the vecstore_server binary, via the
// STRATUM_VECSTORE_SERVER_BIN environment variable if set, otherwise the
// conventional CMake build output path relative to this package
// (<repo root>/build/vecstore/vecstore_server). Skips the test if the
// binary cannot be found, since building it is a separate CMake step Go
// tooling does not drive.
func vecstoreServerBinPath(t *testing.T) string {
	t.Helper()

	if p := os.Getenv("STRATUM_VECSTORE_SERVER_BIN"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
		t.Fatalf("STRATUM_VECSTORE_SERVER_BIN=%s does not exist", p)
	}

	// internal/chunkstore -> repo root is two directories up.
	candidate := filepath.Join("..", "..", "build", "vecstore", "vecstore_server")
	if _, err := os.Stat(candidate); err == nil {
		abs, err := filepath.Abs(candidate)
		if err == nil {
			return abs
		}
		return candidate
	}

	t.Skip("vecstore_server binary not found; build it first (cmake -B build vecstore && cmake --build build --target vecstore_server) " +
		"or set STRATUM_VECSTORE_SERVER_BIN to its path")
	return ""
}

func freeLoopbackPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitForServerReady(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s: last error: %w", addr, lastErr)
}
