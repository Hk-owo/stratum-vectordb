package rpcpool

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func newBufconnDialFunc(t *testing.T, dialCount *atomic.Int64) func(context.Context, string) (*grpc.ClientConn, error) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	return func(_ context.Context, addr string) (*grpc.ClientConn, error) {
		if dialCount != nil {
			dialCount.Add(1)
		}
		return grpc.NewClient("passthrough:///"+addr,
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
}

func TestPool_ConcurrentGetSameAddrDialsOnce(t *testing.T) {
	var dials atomic.Int64
	gate := make(chan struct{})
	baseDial := newBufconnDialFunc(t, nil)

	p := NewPool(NewDialer(DialConfig{
		DialFunc: func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
			n := dials.Add(1)
			if n == 1 {
				<-gate
			}
			return baseDial(ctx, addr)
		},
	}))
	defer func() { _ = p.Close() }()

	const workers = 16
	conns := make([]*grpc.ClientConn, workers)
	errs := make([]error, workers)

	var started sync.WaitGroup
	started.Add(workers)
	var done sync.WaitGroup
	done.Add(workers)

	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer done.Done()
			started.Done()
			conns[i], errs[i] = p.Get("node-1:7000")
		}()
	}

	started.Wait()
	close(gate)
	done.Wait()

	if got := dials.Load(); got != 1 {
		t.Fatalf("concurrent Get dials = %d, want 1", got)
	}
	first := conns[0]
	if first == nil {
		t.Fatalf("first Get conn = nil, err = %v", errs[0])
	}
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d Get error = %v", i, errs[i])
		}
		if conns[i] != first {
			t.Fatalf("worker %d conn %p != first %p", i, conns[i], first)
		}
	}
}

func TestPool_EvictTriggersRedialAndClosesOldConn(t *testing.T) {
	var dials atomic.Int64
	p := NewPool(NewDialer(DialConfig{
		DialFunc: newBufconnDialFunc(t, &dials),
	}))
	defer func() { _ = p.Close() }()

	c1, err := p.Get("node-1:7000")
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if dials.Load() != 1 {
		t.Fatalf("dials after first Get = %d, want 1", dials.Load())
	}

	// Second Get reuses c1 without dialing.
	c1Again, err := p.Get("node-1:7000")
	if err != nil || c1Again != c1 {
		t.Fatalf("second Get = (%p, %v), want %p", c1Again, err, c1)
	}
	if dials.Load() != 1 {
		t.Fatalf("dials after cached Get = %d, want 1", dials.Load())
	}

	p.Evict("node-1:7000")
	if st := c1.GetState(); st != connectivity.Shutdown {
		t.Fatalf("evicted conn state = %v, want Shutdown", st)
	}

	c2, err := p.Get("node-1:7000")
	if err != nil {
		t.Fatalf("Get after Evict: %v", err)
	}
	if c2 == c1 {
		t.Fatal("Get after Evict returned the same conn pointer")
	}
	if dials.Load() != 2 {
		t.Fatalf("dials after Evict+Get = %d, want 2", dials.Load())
	}
}

func TestPool_CloseIsIdempotentClosesAllAndRejectsNewGet(t *testing.T) {
	var dials atomic.Int64
	p := NewPool(NewDialer(DialConfig{
		DialFunc: newBufconnDialFunc(t, &dials),
	}))

	c1, err := p.Get("node-1:7000")
	if err != nil {
		t.Fatalf("Get node-1: %v", err)
	}
	c2, err := p.Get("node-2:7000")
	if err != nil {
		t.Fatalf("Get node-2: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close (idempotent): %v", err)
	}
	if st := c1.GetState(); st != connectivity.Shutdown {
		t.Fatalf("c1 state after Close = %v, want Shutdown", st)
	}
	if st := c2.GetState(); st != connectivity.Shutdown {
		t.Fatalf("c2 state after Close = %v, want Shutdown", st)
	}

	if _, err := p.Get("node-3:7000"); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Get after Close error = %v, want ErrPoolClosed", err)
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2 (no dial after Close)", dials.Load())
	}
}

func TestAcquire_PooledVsDialOwnership(t *testing.T) {
	ctx := context.Background()
	var dials atomic.Int64
	dialFn := newBufconnDialFunc(t, &dials)
	p := NewPool(NewDialer(DialConfig{DialFunc: dialFn}))
	defer func() { _ = p.Close() }()

	// Pooled mode: release is a no-op and connection stays alive for reuse.
	c1, release1, err := Acquire(ctx, p, nil, nil, "node-1:7000")
	if err != nil {
		t.Fatalf("Acquire pooled: %v", err)
	}
	release1()
	if st := c1.GetState(); st == connectivity.Shutdown {
		t.Fatal("pooled conn was closed by release()")
	}
	c2, release2, err := Acquire(ctx, p, nil, nil, "node-1:7000")
	if err != nil || c2 != c1 {
		t.Fatalf("second Acquire pooled = (%p, %v), want %p", c2, err, c1)
	}
	release2()
	if dials.Load() != 1 {
		t.Fatalf("pooled Acquire dials = %d, want 1", dials.Load())
	}

	// Direct dial mode (e.g. test Dial override): release closes the channel.
	c3, release3, err := Acquire(ctx, p, dialFn, nil, "node-1:7000")
	if err != nil {
		t.Fatalf("Acquire with Dial override: %v", err)
	}
	release3()
	if st := c3.GetState(); st != connectivity.Shutdown {
		t.Fatalf("dialed conn state after release = %v, want Shutdown", st)
	}
}

func TestDialerAndPoolEdgeCases(t *testing.T) {
	if DefaultDialer() == nil {
		t.Fatal("DefaultDialer() returned nil")
	}

	d := NewDialer(DialConfig{
		MaxCallRecvMsgSize: 4 << 20,
		MaxCallSendMsgSize: 4 << 20,
		DialOptions:        []grpc.DialOption{grpc.WithDefaultCallOptions()},
	})
	if _, err := d.Dial(""); !errors.Is(err, ErrEmptyAddr) {
		t.Fatalf("Dial empty addr = %v, want ErrEmptyAddr", err)
	}
	conn, err := d.Dial("passthrough:///127.0.0.1:0", grpc.WithAuthority("test"))
	if err != nil {
		t.Fatalf("Dial standard non-blocking: %v", err)
	}
	_ = conn.Close()

	var nilDialer *Dialer
	conn, err = nilDialer.DialContext(context.Background(), "passthrough:///127.0.0.1:0")
	if err != nil {
		t.Fatalf("nil Dialer.DialContext: %v", err)
	}
	_ = conn.Close()

	// NewPool(nil) uses DefaultDialer; Close on nil pool is a no-op.
	var nilPool *Pool
	if err := nilPool.Close(); err != nil {
		t.Fatalf("nil Pool.Close = %v", err)
	}
	nilPool.Evict("a:1")
	if _, err := nilPool.Get("a:1"); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("nil Pool.Get = %v, want ErrPoolClosed", err)
	}

	pDefault := NewPool(nil)
	if _, err := pDefault.Get(""); !errors.Is(err, ErrEmptyAddr) {
		t.Fatalf("Get empty addr = %v, want ErrEmptyAddr", err)
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pDefault.GetContext(canceledCtx, "a:1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetContext canceled = %v, want context.Canceled", err)
	}
	pDefault.Evict("")
	_ = pDefault.Close()
}

func TestPool_WaiterCancelAndInflightEvictCloseError(t *testing.T) {
	baseDial := newBufconnDialFunc(t, nil)

	// 1. Waiter cancels while dial is inflight; leader still caches conn.
	started := make(chan struct{})
	release := make(chan struct{})
	p := NewPool(NewDialer(DialConfig{
		DialFunc: func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
			close(started)
			<-release
			return baseDial(ctx, addr)
		},
	}))
	defer func() { _ = p.Close() }()

	leaderDone := make(chan error, 1)
	go func() {
		_, err := p.Get("node-1:7000")
		leaderDone <- err
	}()
	<-started

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		_, err := p.GetContext(waiterCtx, "node-1:7000")
		waiterDone <- err
	}()
	cancelWaiter()
	if err := <-waiterDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter error = %v, want context.Canceled", err)
	}
	close(release)
	if err := <-leaderDone; err != nil {
		t.Fatalf("leader Get error = %v", err)
	}

	// 2. Evict while inflight drops the newly dialed conn and returns an error.
	evictStarted := make(chan struct{})
	evictRelease := make(chan struct{})
	pEvict := NewPool(NewDialer(DialConfig{
		DialFunc: func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
			close(evictStarted)
			<-evictRelease
			return baseDial(ctx, addr)
		},
	}))
	defer func() { _ = pEvict.Close() }()
	evictErrCh := make(chan error, 1)
	go func() {
		_, err := pEvict.Get("node-2:7000")
		evictErrCh <- err
	}()
	<-evictStarted
	pEvict.Evict("node-2:7000")
	close(evictRelease)
	if err := <-evictErrCh; err == nil {
		t.Fatal("expected error when Evict raced with inflight dial")
	}

	// 3. Close while inflight returns ErrPoolClosed.
	closeStarted := make(chan struct{})
	closeRelease := make(chan struct{})
	pClose := NewPool(NewDialer(DialConfig{
		DialFunc: func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
			close(closeStarted)
			<-closeRelease
			return baseDial(ctx, addr)
		},
	}))
	closeErrCh := make(chan error, 1)
	go func() {
		_, err := pClose.Get("node-3:7000")
		closeErrCh <- err
	}()
	<-closeStarted
	_ = pClose.Close()
	close(closeRelease)
	if err := <-closeErrCh; !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Get raced with Close = %v, want ErrPoolClosed", err)
	}

	// 4. Dial error propagates to concurrent waiter.
	dialErr := errors.New("boom")
	errStarted := make(chan struct{})
	errRelease := make(chan struct{})
	var errOnce sync.Once
	var errDials atomic.Int64
	pErr := NewPool(NewDialer(DialConfig{
		DialFunc: func(context.Context, string) (*grpc.ClientConn, error) {
			errDials.Add(1)
			errOnce.Do(func() { close(errStarted) })
			<-errRelease
			return nil, dialErr
		},
	}))
	defer func() { _ = pErr.Close() }()
	var wg sync.WaitGroup
	wg.Add(2)
	var e1, e2 error
	go func() { defer wg.Done(); _, e1 = pErr.Get("bad:7000") }()
	<-errStarted
	go func() { defer wg.Done(); _, e2 = pErr.Get("bad:7000") }()
	// Let the second goroutine enter inflight wait before releasing DialFunc.
	for {
		pErr.mu.Lock()
		_, hasInflight := pErr.inflight["bad:7000"]
		pErr.mu.Unlock()
		if hasInflight {
			break
		}
	}
	close(errRelease)
	wg.Wait()
	if !errors.Is(e1, dialErr) || !errors.Is(e2, dialErr) {
		t.Fatalf("propagated errors = (%v, %v), want %v", e1, e2, dialErr)
	}

	// 5. Acquire unpooled and error paths.
	if _, _, err := Acquire(context.Background(), nil, func(context.Context, string) (*grpc.ClientConn, error) {
		return nil, dialErr
	}, nil, "a:1"); !errors.Is(err, dialErr) {
		t.Fatalf("Acquire dial error = %v", err)
	}
	if _, _, err := Acquire(context.Background(), pErr, nil, nil, ""); !errors.Is(err, ErrEmptyAddr) {
		t.Fatalf("Acquire pool error = %v", err)
	}
	if _, _, err := Acquire(context.Background(), nil, nil, nil, ""); !errors.Is(err, ErrEmptyAddr) {
		t.Fatalf("Acquire unpooled empty addr = %v", err)
	}
	unpooled, rel, err := Acquire(context.Background(), nil, nil, nil, "passthrough:///127.0.0.1:0")
	if err != nil {
		t.Fatalf("Acquire unpooled default dialer: %v", err)
	}
	rel()
	if st := unpooled.GetState(); st != connectivity.Shutdown {
		t.Fatalf("unpooled conn after release = %v, want Shutdown", st)
	}
}
