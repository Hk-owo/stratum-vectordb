// Package rpcpool unifies gRPC dial options and outbound client connection pools
// across Stratum nodes.
package rpcpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	// DefaultKeepaliveInterval is the interval between HTTP/2 keepalive pings
	// while a stream is active. Matching C++ vecstore's server keepalive
	// limits requires PermitWithoutStream=false so idle channels do not receive
	// too_many_pings GoAway.
	DefaultKeepaliveInterval = 60 * time.Second

	// DefaultKeepaliveTimeout is the wait window for a keepalive ping ack.
	DefaultKeepaliveTimeout = 3 * time.Second
)

var (
	// ErrPoolClosed is returned by Pool.Get after Pool.Close has been called.
	ErrPoolClosed = errors.New("rpcpool: pool is closed")

	// ErrEmptyAddr is returned when dialing or looking up an empty address.
	ErrEmptyAddr = errors.New("rpcpool: target address is empty")
)

// DialConfig holds unified gRPC dial settings for Stratum's outbound channels.
type DialConfig struct {
	// KeepaliveInterval / KeepaliveTimeout tune HTTP/2 client keepalive.
	// Non-positive values default to DefaultKeepaliveInterval /
	// DefaultKeepaliveTimeout.
	KeepaliveInterval time.Duration
	KeepaliveTimeout  time.Duration

	// PermitWithoutStream defaults to false so idle channels do not trigger
	// C++ vecstore's too_many_pings GoAway.
	PermitWithoutStream bool

	// Optional per-call message size limits (0 leaves gRPC defaults).
	MaxCallRecvMsgSize int
	MaxCallSendMsgSize int

	// DialOptions appends additional grpc.DialOption values (e.g. TLS
	// credentials in the future).
	DialOptions []grpc.DialOption

	// DialFunc overrides grpc.NewClient when non-nil (primarily for tests).
	DialFunc func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// Dialer creates non-blocking gRPC ClientConn channels using one consistent
// transport, keepalive, and message-limit configuration.
type Dialer struct {
	cfg      DialConfig
	baseOpts []grpc.DialOption
}

var defaultDialer = NewDialer(DialConfig{})

// NewDialer constructs a Dialer with defaults filled in for unset fields.
func NewDialer(cfg DialConfig) *Dialer {
	if cfg.KeepaliveInterval <= 0 {
		cfg.KeepaliveInterval = DefaultKeepaliveInterval
	}
	if cfg.KeepaliveTimeout <= 0 {
		cfg.KeepaliveTimeout = DefaultKeepaliveTimeout
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                cfg.KeepaliveInterval,
			Timeout:             cfg.KeepaliveTimeout,
			PermitWithoutStream: cfg.PermitWithoutStream,
		}),
	}
	var callOpts []grpc.CallOption
	if cfg.MaxCallRecvMsgSize > 0 {
		callOpts = append(callOpts, grpc.MaxCallRecvMsgSize(cfg.MaxCallRecvMsgSize))
	}
	if cfg.MaxCallSendMsgSize > 0 {
		callOpts = append(callOpts, grpc.MaxCallSendMsgSize(cfg.MaxCallSendMsgSize))
	}
	if len(callOpts) > 0 {
		opts = append(opts, grpc.WithDefaultCallOptions(callOpts...))
	}
	if len(cfg.DialOptions) > 0 {
		opts = append(opts, cfg.DialOptions...)
	}

	return &Dialer{
		cfg:      cfg,
		baseOpts: opts,
	}
}

// DefaultDialer returns the shared process-default Dialer.
func DefaultDialer() *Dialer {
	return defaultDialer
}

// Dial opens a new non-blocking gRPC ClientConn to addr.
func (d *Dialer) Dial(addr string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return d.DialContext(context.Background(), addr, extraOpts...)
}

// DialContext opens a new non-blocking gRPC ClientConn to addr. Context is
// passed through to custom DialFunc overrides (e.g. bufconn in tests); standard
// grpc.NewClient dials remain non-blocking.
func (d *Dialer) DialContext(ctx context.Context, addr string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if addr == "" {
		return nil, ErrEmptyAddr
	}
	if d == nil {
		d = defaultDialer
	}
	if d.cfg.DialFunc != nil {
		return d.cfg.DialFunc(ctx, addr)
	}
	opts := d.baseOpts
	if len(extraOpts) > 0 {
		opts = make([]grpc.DialOption, len(d.baseOpts)+len(extraOpts))
		copy(opts, d.baseOpts)
		copy(opts[len(d.baseOpts):], extraOpts)
	}
	return grpc.NewClient(addr, opts...)
}

type dialCall struct {
	done    chan struct{}
	conn    *grpc.ClientConn
	err     error
	evicted bool
}

// Pool caches long-lived outbound *grpc.ClientConn channels keyed by target
// address, deduplicating concurrent dials for the same destination.
type Pool struct {
	dialer *Dialer

	mu       sync.Mutex
	closed   bool
	conns    map[string]*grpc.ClientConn
	inflight map[string]*dialCall
}

// NewPool creates an outbound connection pool backed by dialer (or the default
// dialer when dialer is nil).
func NewPool(dialer *Dialer) *Pool {
	if dialer == nil {
		dialer = defaultDialer
	}
	return &Pool{
		dialer:   dialer,
		conns:    make(map[string]*grpc.ClientConn),
		inflight: make(map[string]*dialCall),
	}
}

// Get returns a cached ClientConn to addr, dialing lazily and deduplicating
// concurrent Get calls for the same address. Callers must NOT close the
// returned connection directly; use Evict or Close on the Pool.
func (p *Pool) Get(addr string) (*grpc.ClientConn, error) {
	return p.GetContext(context.Background(), addr)
}

// GetContext behaves like Get, honoring ctx cancellation while waiting on an
// in-flight dial started by another caller.
func (p *Pool) GetContext(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	if p == nil {
		return nil, ErrPoolClosed
	}
	if addr == "" {
		return nil, ErrEmptyAddr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		if conn, ok := p.conns[addr]; ok {
			p.mu.Unlock()
			return conn, nil
		}
		if call, ok := p.inflight[addr]; ok {
			p.mu.Unlock()
			select {
			case <-call.done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			// If the completed call succeeded cleanly, either it's in p.conns (picked
			// up on next iteration) or was evicted/closed in between; propagate dial
			// errors directly to avoid spinning.
			if call.err != nil {
				return nil, call.err
			}
			continue
		}

		call := &dialCall{done: make(chan struct{})}
		p.inflight[addr] = call
		p.mu.Unlock()

		conn, err := p.dialer.DialContext(ctx, addr)

		var staleConn *grpc.ClientConn
		var retErr error
		p.mu.Lock()
		delete(p.inflight, addr)
		switch {
		case err != nil:
			call.err = err
			retErr = err
		case p.closed:
			staleConn = conn
			call.err = ErrPoolClosed
			retErr = ErrPoolClosed
		case call.evicted:
			// Evict raced with an in-flight dial: drop this connection rather than
			// caching a stale channel; the caller (or subsequent Get) gets a clean
			// error or redials on retry.
			staleConn = conn
			call.err = fmt.Errorf("rpcpool: connection to %s evicted while dialing", addr)
			retErr = call.err
		default:
			p.conns[addr] = conn
			call.conn = conn
		}
		close(call.done)
		p.mu.Unlock()

		if staleConn != nil {
			_ = staleConn.Close()
		}
		if retErr != nil {
			return nil, retErr
		}
		return conn, nil
	}
}

// Evict removes and closes any cached connection for addr and marks any
// in-flight dial for addr so its resulting connection will not enter the pool.
func (p *Pool) Evict(addr string) {
	if p == nil || addr == "" {
		return
	}
	p.mu.Lock()
	conn, ok := p.conns[addr]
	if ok {
		delete(p.conns, addr)
	}
	if call, dialing := p.inflight[addr]; dialing {
		call.evicted = true
	}
	p.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
}

// Close idempotently closes every active connection in the pool and rejects any
// subsequent Get calls with ErrPoolClosed.
func (p *Pool) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	conns := make([]*grpc.ClientConn, 0, len(p.conns))
	for addr, c := range p.conns {
		if c != nil {
			conns = append(conns, c)
		}
		delete(p.conns, addr)
	}
	for _, call := range p.inflight {
		call.evicted = true
	}
	p.mu.Unlock()

	var errs []error
	for _, c := range conns {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Acquire returns a connection for a single RPC/operation along with an
// explicit release callback.
//
// Resolution order preserves existing test hooks while unifying production pools:
//  1. If dial is non-nil (e.g. a test bufconn dialer), calls dial(ctx, addr)
//     and release closes the returned connection.
//  2. Otherwise if pool is non-nil, borrows a pooled connection from pool and
//     release is a no-op (the pool retains ownership).
//  3. Otherwise dials an unpooled connection through dialer (or DefaultDialer)
//     and release closes it.
func Acquire(
	ctx context.Context,
	pool *Pool,
	dial func(context.Context, string) (*grpc.ClientConn, error),
	dialer *Dialer,
	addr string,
) (*grpc.ClientConn, func(), error) {
	if dial != nil {
		conn, err := dial(ctx, addr)
		if err != nil {
			return nil, nil, err
		}
		return conn, func() { _ = conn.Close() }, nil
	}
	if pool != nil {
		conn, err := pool.GetContext(ctx, addr)
		if err != nil {
			return nil, nil, err
		}
		return conn, func() {}, nil
	}
	if dialer == nil {
		dialer = defaultDialer
	}
	conn, err := dialer.DialContext(ctx, addr)
	if err != nil {
		return nil, nil, err
	}
	return conn, func() { _ = conn.Close() }, nil
}
