package storage

import (
	"context"
	"errors"
	"strconv"

	"google.golang.org/grpc/metadata"
	serrors "stratum/internal/errors"
)

type Resolver func(context.Context, string) (Snapshot, error)

type snapshotKey struct{}

const epochHeader = "x-stratum-placement-epoch"

// RequireRequestEpoch is for remote DATA mutation entrances on upgraded nodes.
// Fresh in-process operations may bind their own authoritative snapshot.
func RequireRequestEpoch(ctx context.Context, kbID string) error {
	if _, ok := Bound(ctx, kbID); ok {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get(epochHeader)) != 1 {
		return serrors.ErrPlacementStale
	}
	return nil
}

// Bind carries one immutable operation configuration through local work and
// outgoing RPCs. It never combines members from one view with another policy.
func Bind(ctx context.Context, s Snapshot) context.Context {
	if s.Migration != nil {
		m := s.Migration.Clone()
		s.Migration = &m
	}
	s.Placement = s.Placement.Clone()
	s.Nodes = append([]Node(nil), s.Nodes...)
	ctx = context.WithValue(ctx, snapshotKey{}, s)
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set(epochHeader, strconv.FormatUint(s.Placement.Epoch, 10))
	return metadata.NewOutgoingContext(ctx, md)
}

func Bound(ctx context.Context, kbID string) (Snapshot, bool) {
	s, ok := ctx.Value(snapshotKey{}).(Snapshot)
	if !ok || s.Placement.KBID != kbID {
		return Snapshot{}, false
	}
	s.Placement = s.Placement.Clone()
	if s.Migration != nil {
		m := s.Migration.Clone()
		s.Migration = &m
	}
	s.Nodes = append([]Node(nil), s.Nodes...)
	return s, true
}

// Resolve reuses the bound configuration. An incoming epoch is checked before
// local writes, pulls or query work; malformed and obsolete credentials fail.
func Resolve(ctx context.Context, resolver Resolver, kbID string) (Snapshot, error) {
	if s, ok := Bound(ctx, kbID); ok {
		return s, nil
	}
	s, err := resolver(ctx, kbID)
	if err != nil {
		if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get(epochHeader)) > 0 && errors.Is(err, serrors.ErrKnowledgeBaseNotFound) {
			return Snapshot{}, serrors.ErrPlacementUnknown
		}
		return Snapshot{}, err
	}
	// Validate adapters too: duplicate physical targets must never count twice.
	d := NewDirectory()
	d.Revision = s.Revision
	d.Placements[kbID] = s.Placement
	d.ActiveVersions = map[string]int64{kbID: s.ActiveVersion}
	if s.Migration != nil {
		d.Migrations = map[string]Migration{s.Migration.Request.OperationID: s.Migration.Clone()}
	}
	for _, n := range s.Nodes {
		if _, exists := d.Nodes[n.ID]; exists {
			return Snapshot{}, serrors.ErrPlacementUnknown
		}
		d.Nodes[n.ID] = n
	}
	s, err = d.Resolve(kbID)
	if err != nil {
		return Snapshot{}, err
	}
	if err := CheckRequestEpoch(ctx, kbID, s.Placement.Epoch); err != nil {
		return Snapshot{}, err
	}
	return s, nil
}

// CheckRequestEpoch is used where a new authoritative view is required rather
// than an operation's cached snapshot, notably late physical cleanup requests.
func CheckRequestEpoch(ctx context.Context, kbID string, expected uint64) error {
	if previous, ok := Bound(ctx, kbID); ok && previous.Placement.Epoch != expected {
		return serrors.ErrPlacementStale
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get(epochHeader); len(values) > 0 {
			if len(values) != 1 {
				return serrors.ErrPlacementStale
			}
			epoch, err := strconv.ParseUint(values[0], 10, 64)
			if err != nil || epoch != expected {
				return serrors.ErrPlacementStale
			}
		}
	}
	return nil
}

func (s Snapshot) Addresses(except int64) []string {
	out := make([]string, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		if n.ID != except {
			out = append(out, n.Addr)
		}
	}
	return out
}

func (s Snapshot) HasAddress(addr string) bool {
	for _, n := range s.Nodes {
		if n.Addr == addr {
			return true
		}
	}
	return false
}
