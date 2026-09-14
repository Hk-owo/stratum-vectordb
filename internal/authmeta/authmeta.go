// Package authmeta carries the metadata keys that distinguish "this call arrived
// through a service station" from "this call reached the port directly".
//
// It is its own package because three layers need the same two constants and
// none of them owns the others: internal/router stamps the mark when it
// forwards, internal/raft stamps it when a storage node reads metadata on the
// cluster's behalf, and the service layer reads it to decide whether to serve a
// client-facing call.
package authmeta

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const (
	// CredentialMetadataKey carries a caller's API key. It is the standard
	// Authorization header, so an ordinary gRPC client can set it without
	// knowing anything about Stratum.
	CredentialMetadataKey = "authorization"

	// VerifiedMetadataKey marks a call that came from inside the system: a
	// station forwarding a client's request, or a node reading metadata on the
	// cluster's behalf.
	//
	// It carries no identity on purpose. A receiving node does not need to know
	// WHO is asking — only that something with the authority to ask vouched for
	// the request. Keeping identity out also keeps it from leaking into layers
	// with no use for it, and its safety rests on the cluster being unreachable
	// from outside rather than on the mark being unforgeable.
	VerifiedMetadataKey = "x-stratum-authenticated"
)

// WithVerifiedMark stamps an outbound context as coming from inside the system.
//
// Both halves of the system use it, and they must: a station forwarding a
// client's call, and a storage node reading metadata through the control
// layer's client-facing service. The second is why this is not simply "the
// station's mark" — a storage node reaches KnowledgeBaseService the same way a
// client does, and the gate cannot tell those apart by service name. What
// separates them is this mark, which only code inside the system applies.
func WithVerifiedMark(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, VerifiedMetadataKey, "1")
}

// IsVerified reports whether an incoming call carries the mark.
func IsVerified(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	return len(md.Get(VerifiedMetadataKey)) > 0
}
