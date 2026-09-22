package service

import (
	"context"
	"sync"

	stratinternalsync "stratum/internal/sync"
	"stratum/internal/versiondoc"
)

// docSetCacheCap is how many versions' document-ID sets one replica keeps. A set
// is one entry per document (~1.5 MB for 8,000 documents, the entries and the
// membership map together), and a replica serves a handful of active versions at
// a time, so a small bound holds the hot set in a few megabytes.
const docSetCacheCap = 4

// docSetCache memoizes a version's document-ID set — and the membership map
// derived from it — for this replica.
//
// Why caching is possible at all: a version's document set is immutable once the
// write that produced it has landed. A new version gets a new id, and version ids
// are never reused (the Raft counter is monotonic), so one (kbID, versionID)
// always describes one set.
//
// Why caching is safe: an entry is written only after the set read from the store
// hashes to the digest the control layer committed for that version
// (types.VersionMeta.DocIDSetHash — which Query already reads as meta_us, so the
// verification costs no extra round trip). A cache hit therefore means "this is
// the set the control layer says this version has", which is exactly what
// re-reading the store and verifying would establish — the hit is equivalent to
// the miss, minus the read.
//
// The failure this rules out is the one §7.4 was about. A replica that reads the
// set before its data arrives sees an empty — or a partially filled — set, and
// caching THAT would make it answer wrongly long after its data had arrived,
// because nothing would ever come back to correct it. An unverifiable read (no
// committed digest yet, or a digest that does not match) is never cached, so such
// a replica keeps re-reading the store on every query, exactly as it did before
// this cache existed.
type docSetCache struct {
	mu      sync.Mutex
	entries map[docSetKey]docSetEntry
	order   []docSetKey // insertion order, for FIFO eviction
}

type docSetKey struct {
	kbID      string
	versionID int64
}

type docSetEntry struct {
	ids    []string
	docs   map[string]struct{}
	digest string // the committed digest this set was verified against
}

// lookup returns (kbID, versionID)'s document-ID set and its membership map: from
// the cache when a verified entry for the same digest is present, else by reading
// store. digest is the control layer's committed DocIDSetHash for the version —
// "" means none is committed yet, which makes the read unverifiable and therefore
// uncacheable (and unhittable).
//
// The returned map is shared with other callers and must not be mutated. A store
// error is returned as-is and never cached.
func (c *docSetCache) lookup(ctx context.Context, store versiondoc.VersionDocList, kbID string, versionID int64, digest string) ([]string, map[string]struct{}, error) {
	if digest != "" {
		if e, ok := c.get(kbID, versionID, digest); ok {
			return e.ids, e.docs, nil
		}
	}

	ids, err := store.ListDocIDs(ctx, kbID, versionID)
	if err != nil {
		return nil, nil, err
	}
	docs := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		docs[id] = struct{}{}
	}

	// Verified against the control layer's digest, the set is the authoritative
	// one for this version and can be reused. Anything else is left uncached.
	if digest != "" && stratinternalsync.ComputeDocIDSetHash(ids) == digest {
		c.put(kbID, versionID, docSetEntry{ids: ids, docs: docs, digest: digest})
	}
	return ids, docs, nil
}

func (c *docSetCache) get(kbID string, versionID int64, digest string) (docSetEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[docSetKey{kbID: kbID, versionID: versionID}]
	if !ok || e.digest != digest {
		return docSetEntry{}, false
	}
	return e, true
}

func (c *docSetCache) put(kbID string, versionID int64, e docSetEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[docSetKey]docSetEntry)
	}
	key := docSetKey{kbID: kbID, versionID: versionID}
	if _, exists := c.entries[key]; !exists {
		if len(c.order) >= docSetCacheCap {
			oldest := c.order[0]
			c.order = c.order[1:]
			delete(c.entries, oldest)
		}
		c.order = append(c.order, key)
	}
	c.entries[key] = e
}
