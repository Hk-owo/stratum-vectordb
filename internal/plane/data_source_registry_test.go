package plane

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestDataSourceRegistryRegistersLooksUpAndForgets(t *testing.T) {
	reg := NewDataSourceRegistry()
	if _, ok := reg.Lookup("kb-1", 7); ok {
		t.Fatal("an empty registry must not claim to know a source")
	}

	reg.Register("kb-1", 7, "peer:7001")
	if addr, ok := reg.Lookup("kb-1", 7); !ok || addr != "peer:7001" {
		t.Fatalf("Lookup = (%q, %v), want (peer:7001, true)", addr, ok)
	}

	// A writer that announces itself again (the version was rebuilt and the
	// data moved) wins: the latest announcement is the fact.
	reg.Register("kb-1", 7, "peer:7002")
	if addr, _ := reg.Lookup("kb-1", 7); addr != "peer:7002" {
		t.Fatalf("the latest announcement must win, got %q", addr)
	}

	// An empty address means "did not announce", not "the source is nowhere":
	// storing it would turn a lookup into "known: no data anywhere".
	reg.Register("kb-1", 8, "")
	if _, ok := reg.Lookup("kb-1", 8); ok {
		t.Fatal("an empty source address must not be recorded")
	}

	// Entries are per (kb, version).
	reg.Register("kb-2", 7, "peer:7003")
	reg.ForgetVersion("kb-1", 7)
	if _, ok := reg.Lookup("kb-1", 7); ok {
		t.Fatal("ForgetVersion must drop the entry")
	}
	if _, ok := reg.Lookup("kb-2", 7); !ok {
		t.Fatal("ForgetVersion must not touch another KB")
	}
	reg.ForgetKB("kb-2")
	if reg.Len() != 0 {
		t.Fatalf("registry should be empty after ForgetKB, has %d entries", reg.Len())
	}
}

// The table is read on the Raft apply path and written from gRPC handlers, so
// reads and writes have to be able to overlap.
func TestDataSourceRegistryIsConcurrencySafe(t *testing.T) {
	reg := NewDataSourceRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				reg.Register("kb-1", int64(j), "peer:7001")
				_, _ = reg.Lookup("kb-1", int64(j))
				reg.ForgetVersion("kb-1", int64(j))
			}
		}()
	}
	wg.Wait()
}

// §8.5: the announced holder wins over the pre-§8.5 answer (the leader), and
// the fallback is only consulted when nothing has been announced.
func TestResolverWithRegistryPrefersAnnouncedSourceAndFallsBack(t *testing.T) {
	reg := NewDataSourceRegistry()
	fallbackCalls := 0
	fallback := func(context.Context, string, int64) (string, bool, error) {
		fallbackCalls++
		return "leader:7000", true, nil
	}
	resolve := ResolverWithRegistry(reg, nil, fallback, nil)

	addr, ok, err := resolve(context.Background(), "kb-1", 3)
	if err != nil || !ok || addr != "leader:7000" {
		t.Fatalf("resolve = (%q, %v, %v), want the fallback", addr, ok, err)
	}
	if fallbackCalls != 1 {
		t.Fatalf("fallback calls = %d, want 1", fallbackCalls)
	}

	reg.Register("kb-1", 3, "writer:7001")
	addr, ok, err = resolve(context.Background(), "kb-1", 3)
	if err != nil || !ok || addr != "writer:7001" {
		t.Fatalf("resolve = (%q, %v, %v), want the announced writer", addr, ok, err)
	}
	if fallbackCalls != 1 {
		t.Fatalf("an announced source must not consult the fallback, calls = %d", fallbackCalls)
	}

	// A different version of the same KB is unaffected by that announcement.
	if _, _, _ = resolve(context.Background(), "kb-1", 4); fallbackCalls != 2 {
		t.Fatalf("fallback calls = %d, want 2", fallbackCalls)
	}
}

func TestResolverWithRegistryPropagatesFallbackErrors(t *testing.T) {
	wantErr := errors.New("no leader")
	resolve := ResolverWithRegistry(NewDataSourceRegistry(), nil, func(context.Context, string, int64) (string, bool, error) {
		return "", false, wantErr
	}, nil)
	if _, _, err := resolve(context.Background(), "kb-1", 1); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want the fallback's error", err)
	}
}

// A nil registry behaves like an empty one: the resolver is wired before any
// announcement can have arrived. A nil cache is the same kind of absent — the
// resolver is assembled before either source can answer.
func TestResolverWithRegistryToleratesNilRegistry(t *testing.T) {
	resolve := ResolverWithRegistry(nil, nil, func(context.Context, string, int64) (string, bool, error) {
		return "leader:7000", true, nil
	}, nil)
	if addr, ok, err := resolve(context.Background(), "kb-1", 1); err != nil || !ok || addr != "leader:7000" {
		t.Fatalf("resolve = (%q, %v, %v), want the fallback", addr, ok, err)
	}
}

// The announced table is a writer's own statement and stays the most specific
// fact there is: the aggregate mirror must not override it.
func TestResolverWithRegistryPrefersAnnouncementOverHolders(t *testing.T) {
	reg := NewDataSourceRegistry()
	reg.Register("kb-1", 3, "writer:7001")
	holders := NewHoldersCache(HoldersCacheConfig{})
	holders.Store("kb-1", 3, []string{"aggregate:7009"})

	resolve := ResolverWithRegistry(reg, holders, func(context.Context, string, int64) (string, bool, error) {
		return "leader:7000", true, nil
	}, nil)
	if addr, ok, err := resolve(context.Background(), "kb-1", 3); err != nil || !ok || addr != "writer:7001" {
		t.Fatalf("resolve = (%q, %v, %v), want the announced writer", addr, ok, err)
	}
}

// The fourth layer (docs/data-source-holders-fallback-plan.md §5 step 3): when
// the table misses but the mirror has an answer, the mirror wins and the
// fallback is never consulted. It answers lower versions too — a node whose
// cursor reached 5 holds everything below it — but never a higher one.
func TestResolverWithRegistryAnswersFromHoldersBeforeFallingBack(t *testing.T) {
	reg := NewDataSourceRegistry()
	fallbackCalls := 0
	fallback := func(context.Context, string, int64) (string, bool, error) {
		fallbackCalls++
		return "leader:7000", true, nil
	}
	holders := NewHoldersCache(HoldersCacheConfig{})
	holders.Store("kb-1", 5, []string{"holder-a:7001", "holder-b:7002"})
	resolve := ResolverWithRegistry(reg, holders, fallback, nil)

	// Ascending node id ⇒ the first address is the reproducible first choice.
	addr, ok, err := resolve(context.Background(), "kb-1", 5)
	if err != nil || !ok || addr != "holder-a:7001" {
		t.Fatalf("resolve = (%q, %v, %v), want the mirror's first holder", addr, ok, err)
	}
	if fallbackCalls != 0 {
		t.Fatalf("a mirror hit must not consult the fallback, calls = %d", fallbackCalls)
	}

	// P(5) ⊆ P(3): whoever reached 5 reached 3 as well.
	if addr, ok, err := resolve(context.Background(), "kb-1", 3); err != nil || !ok || addr != "holder-a:7001" {
		t.Fatalf("resolve v3 = (%q, %v, %v), want the mirror's first holder", addr, ok, err)
	}
	if fallbackCalls != 0 {
		t.Fatalf("a lower version is covered by a higher answer, calls = %d", fallbackCalls)
	}

	// The reverse does not hold: "these nodes reached 5" is no evidence about 6.
	if _, _, _ = resolve(context.Background(), "kb-1", 6); fallbackCalls != 1 {
		t.Fatalf("a higher version must fall back, calls = %d", fallbackCalls)
	}
}

// A miss on the holders mirror falls through to the leader, and NOTHING on this
// path dials anyone: the function runs on the Raft apply path, where the first
// attempt at §8.5 drove `integration` from 26s to 462s by probing peers. The
// mirror is filled by the report RESPONSE, never by work done here.
func TestResolverWithRegistryUsesTheMirrorAndNeverDials(t *testing.T) {
	holders := NewHoldersCache(HoldersCacheConfig{})
	holders.StoreHolders(
		map[string][]string{"kb-1": {"holder:7001"}},
		map[string]int64{"kb-1": 5},
	)

	calls := 0
	resolve := ResolverWithRegistry(NewDataSourceRegistry(), holders, func(context.Context, string, int64) (string, bool, error) {
		calls++
		return "leader:7000", true, nil
	}, nil)

	// A hit on the mirror: no fallback, no dial.
	if addr, ok, err := resolve(context.Background(), "kb-1", 4); err != nil || !ok || addr != "holder:7001" {
		t.Fatalf("resolve = (%q, %v, %v), want the mirrored holder", addr, ok, err)
	}
	if calls != 0 {
		t.Fatalf("fallback calls = %d, want none on a mirror hit", calls)
	}

	// Above the version the answer was fetched with is not evidence about this
	// node set, so this one falls back.
	if addr, ok, err := resolve(context.Background(), "kb-1", 6); err != nil || !ok || addr != "leader:7000" {
		t.Fatalf("resolve = (%q, %v, %v), want the fallback", addr, ok, err)
	}
	if calls != 1 {
		t.Fatalf("fallback calls = %d, want 1", calls)
	}
}

// Not every address can hold data: in a split deployment the control tier holds
// none, and its leader is what §4 layer 3 used to answer with. Every layer must
// be able to refuse such an address, or a replica keeps pulling from a node
// whose only possible answer is `Unimplemented: this node exports no data`.
//
// Measured on the 3+3 cluster before this was fixed: 100+ failures for a single
// knowledge base, spread over several versions, every one of them under
// Unimplemented and every one aimed at a control node — and none of them could
// ever succeed, because EnsureIndex re-resolves on each attempt (it must: the
// writer announces itself only after its own writes land) while the lag
// catch-up re-enters every few seconds.
func TestResolverWithRegistrySkipsSourcesThatCannotHoldData(t *testing.T) {
	ctx := context.Background()
	canHold := func(addr string) bool {
		return addr == "storage1:7000" || addr == "storage2:7000"
	}
	unusableFallback := func(context.Context, string, int64) (string, bool, error) {
		return "control1:7000", true, nil
	}

	// Layer 2: an entry that cannot hold data must not shadow one that can.
	holders := NewHoldersCache(HoldersCacheConfig{})
	holders.Store("kb-1", 5, []string{"control1:7000", "storage2:7000"})
	resolve := ResolverWithRegistry(NewDataSourceRegistry(), holders, unusableFallback, canHold)
	if addr, ok, err := resolve(ctx, "kb-1", 5); err != nil || !ok || addr != "storage2:7000" {
		t.Fatalf("resolve = (%q, %v, %v), want the mirror entry that can hold data", addr, ok, err)
	}

	// Layer 1: an announcement that cannot hold data is not a source either.
	reg := NewDataSourceRegistry()
	reg.Register("kb-2", 1, "control3:7000")
	holders.Store("kb-2", 1, []string{"storage1:7000"})
	resolve = ResolverWithRegistry(reg, holders, unusableFallback, canHold)
	if addr, ok, err := resolve(ctx, "kb-2", 1); err != nil || !ok || addr != "storage1:7000" {
		t.Fatalf("resolve = (%q, %v, %v), want the mirror entry rather than the unusable announcement", addr, ok, err)
	}

	// Layer 3: a fallback naming an address that cannot hold data is declined
	// here rather than passed on. The invariant is the resolver's own — what
	// comes out of it can hold data — so a caller (EnsureIndex) turns the refusal
	// into a retryable error and keeps waiting, instead of looping on a node that
	// can only refuse.
	resolve = ResolverWithRegistry(NewDataSourceRegistry(), nil, unusableFallback, canHold)
	if addr, ok, err := resolve(ctx, "kb-9", 1); err != nil || ok || addr != "" {
		t.Fatalf("resolve = (%q, %v, %v), want a refusal (no usable source)", addr, ok, err)
	}

	// nil disables the check: the single-tier shape, where the leader does store,
	// must keep every answer it used to give.
	resolve = ResolverWithRegistry(NewDataSourceRegistry(), nil, unusableFallback, nil)
	if addr, ok, err := resolve(ctx, "kb-3", 1); err != nil || !ok || addr != "control1:7000" {
		t.Fatalf("resolve = (%q, %v, %v), want the fallback untouched (no topology check)", addr, ok, err)
	}
}
