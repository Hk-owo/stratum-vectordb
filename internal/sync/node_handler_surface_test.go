package sync

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "stratum/api/proto/stratum"
)

// TestNodeHandler_ForwardsEveryDataSyncMethod drives the whole data-plane
// service through a registered NodeHandler and fails if any method answers
// Unimplemented.
//
// This is the guard for the failure mode documented on NodeHandler: embedding
// UnimplementedDataSyncServiceServer makes a forgotten method compile, register
// and serve as Unimplemented. Four methods were missing that way —
// ExecuteVersionWrite (§7.13.2 dispatch), ConfirmVersionWrite (§7.3 takeover
// stand-down), PullVersionChanges (§7.5 catch-up) and ReportDataVersions
// (§7.13.4 cursor aggregation) — and each one took a feature down silently.
// Reflection cannot catch it (an embedded method is promoted into the type's
// method set, so it looks present), which is why this calls the wire instead.
//
// "Unimplemented" is only accepted when the handler says so on purpose — the
// deliberate stand-downs for a node that owns no storage. Everything else that
// is Unimplemented means nobody forwarded the method.
func TestNodeHandler_ForwardsEveryDataSyncMethod(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	// No stores at all: the methods that need none must still work, and the ones
	// that need some answer with a business error — which is exactly what this
	// test must not confuse with "the method was never forwarded".
	pb.RegisterDataSyncServiceServer(srv, NewNodeHandler(nil, NewPushHandler(nil, 1)))
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.DialContext(context.Background())
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewDataSyncServiceClient(conn)
	ctx := context.Background()

	// recvAll drains a server stream so its error surfaces here.
	recvAll := func(st any, err error) error {
		if err != nil {
			return err
		}
		switch s := st.(type) {
		case pb.DataSyncService_PullVersionDataClient:
			for {
				if _, err := s.Recv(); err != nil {
					return err
				}
			}
		case pb.DataSyncService_PullVersionChangesClient:
			for {
				if _, err := s.Recv(); err != nil {
					return err
				}
			}
		}
		return nil
	}

	sendStream := func(st any, err error) error {
		if err != nil {
			return err
		}
		switch s := st.(type) {
		case pb.DataSyncService_PushVersionDataClient:
			_, err := s.CloseAndRecv()
			return err
		case pb.DataSyncService_PushIndexDataClient:
			_, err := s.CloseAndRecv()
			return err
		}
		return nil
	}

	cases := []struct {
		method string
		call   func() error
	}{
		{"ExecuteVersionWrite", func() error {
			_, err := client.ExecuteVersionWrite(ctx, &pb.ExecuteVersionWriteRequest{})
			return err
		}},
		{"ConfirmVersionWrite", func() error {
			_, err := client.ConfirmVersionWrite(ctx, &pb.ConfirmVersionWriteRequest{})
			return err
		}},
		{"ReportDataVersions", func() error {
			_, err := client.ReportDataVersions(ctx, &pb.ReportDataVersionsRequest{})
			return err
		}},
		{"VersionPresence", func() error {
			_, err := client.VersionPresence(ctx, &pb.VersionPresenceRequest{})
			return err
		}},
		{"LocalVersion", func() error {
			_, err := client.LocalVersion(ctx, &pb.LocalVersionRequest{})
			return err
		}},
		{"DeleteVersionData", func() error {
			_, err := client.DeleteVersionData(ctx, &pb.DeleteVersionDataRequest{})
			return err
		}},
		{"PullVersionData", func() error {
			return recvAll(client.PullVersionData(ctx, &pb.PullVersionDataRequest{}))
		}},
		{"PullVersionChanges", func() error {
			return recvAll(client.PullVersionChanges(ctx, &pb.PullVersionChangesRequest{}))
		}},
		{"PushVersionData", func() error {
			return sendStream(client.PushVersionData(ctx))
		}},
		{"PushIndexData", func() error {
			return sendStream(client.PushIndexData(ctx))
		}},
	}

	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				return // served
			}
			if status.Code(err) != codes.Unimplemented {
				return // a business error: the method ran
			}
			// Unimplemented is acceptable only as a deliberate stand-down.
			msg := err.Error()
			if strings.Contains(msg, "holds no storage") || strings.Contains(msg, "exports no data") {
				return
			}
			t.Errorf("%s answers Unimplemented: NodeHandler does not forward it (embedded "+
				"UnimplementedDataSyncServiceServer swallowed the call)\ngot: %v", tc.method, err)
		})
	}
}
