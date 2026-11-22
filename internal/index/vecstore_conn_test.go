package index

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestIndexManager_WithVecstoreConnCloseDoesNotShutdownSharedConn(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	conn, err := grpc.NewClient("passthrough:///vecstore",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()

	im := NewIndexManager(IndexManagerConfig{
		IndexDataDir: t.TempDir(),
	}, WithVecstoreConn(conn))
	if im.vecstoreConn != conn || im.ownsVecstoreConn {
		t.Fatalf("WithVecstoreConn did not bind unowned conn (got %p, owns=%v)", im.vecstoreConn, im.ownsVecstoreConn)
	}
	if err := im.Close(); err != nil {
		t.Fatalf("im.Close: %v", err)
	}
	if st := conn.GetState(); st == connectivity.Shutdown {
		t.Fatal("IndexManager.Close() shut down caller-owned shared vecstore connection")
	}
}
