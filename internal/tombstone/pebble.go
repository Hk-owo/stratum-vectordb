package tombstone

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"

	"stratum/internal/pebbleutil"
)

// keyPrefix namespaces the table inside a shared database. Every entry is one key:
//
//	"tombstone/" + EncodeString(kbID) + EncodeVersionID(versionID)
//
// The prefix is explicit rather than implied by a dedicated database so the table
// can live beside the node's other small state instead of opening a database of
// its own.
var keyPrefix = []byte("tombstone/")

// syncKey holds the watermark. It lives in the same database as the entries so that
// advancing it and writing the page it describes can be ONE batch — see NoteBatch.
//
// The prefix is deliberately NOT "tombstone/": the entry scan walks that range and
// decodes every key in it as (kbID, versionID), so a key of another shape inside the
// range would fail the scan. '-' sorts below '/', which keeps this key strictly outside
// [keyPrefix, PrefixSuccessor(keyPrefix)).
var syncKey = []byte("tombstone-meta/sync")

// entryLen is the fixed value width: state(1) + notedAt(8) + doneAt(8), both Unix
// nanoseconds. Fixed width keeps decoding allocation-free and makes a torn or
// foreign value detectable by length.
const entryLen = 1 + 8 + 8

// PebbleStore is the durable Store: one entry per deleted version, on disk, so a
// node that restarts between "told" and "cleaned up" resumes by itself.
type PebbleStore struct {
	db *pebble.DB
	// ownDB is true when this store opened the database (and therefore closes it).
	// A caller may hand in a shared handle instead — see NewPebbleStoreIn.
	ownDB bool
}

// NewPebbleStore opens (creating if necessary) a PebbleDB at path and uses it for
// the table alone.
func NewPebbleStore(path string) (*PebbleStore, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("tombstone: open PebbleDB at %s: %w", path, err)
	}
	return &PebbleStore{db: db, ownDB: true}, nil
}

// NewPebbleStoreIn uses an already-open database, which this store does NOT close.
// The keys are namespaced, so the handle may be shared with other users.
func NewPebbleStoreIn(db *pebble.DB) *PebbleStore {
	return &PebbleStore{db: db}
}

// Close implements Store.
func (p *PebbleStore) Close() error {
	if !p.ownDB {
		return nil
	}
	return p.db.Close()
}

func entryKey(kbID string, versionID int64) []byte {
	key := append([]byte(nil), keyPrefix...)
	key = append(key, pebbleutil.EncodeString(kbID)...)
	key = append(key, pebbleutil.EncodeVersionID(versionID)...)
	return key
}

func encodeEntry(e Entry) []byte {
	buf := make([]byte, entryLen)
	buf[0] = byte(e.State)
	binary.BigEndian.PutUint64(buf[1:9], uint64(e.NotedAt.UnixNano()))
	binary.BigEndian.PutUint64(buf[9:17], uint64(e.DoneAt.UnixNano()))
	return buf
}

func decodeEntry(kbID string, versionID int64, value []byte) (Entry, error) {
	if len(value) != entryLen {
		return Entry{}, fmt.Errorf("tombstone: %s/%d: value is %d bytes, want %d", kbID, versionID, len(value), entryLen)
	}
	return Entry{
		KBID:      kbID,
		VersionID: versionID,
		State:     State(value[0]),
		NotedAt:   time.Unix(0, int64(binary.BigEndian.Uint64(value[1:9]))),
		DoneAt:    time.Unix(0, int64(binary.BigEndian.Uint64(value[9:17]))),
	}, nil
}

// read loads one entry. ok is false when there is none.
func (p *PebbleStore) read(kbID string, versionID int64) (Entry, bool, error) {
	value, closer, err := p.db.Get(entryKey(kbID, versionID))
	if errors.Is(err, pebble.ErrNotFound) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, fmt.Errorf("tombstone: read %s/%d: %w", kbID, versionID, err)
	}
	defer closer.Close()
	entry, err := decodeEntry(kbID, versionID, value)
	if err != nil {
		return Entry{}, false, err
	}
	return entry, true, nil
}

// Note implements Store.
func (p *PebbleStore) Note(_ context.Context, kbID string, versionID int64, at time.Time) error {
	// Read first so a retried notification cannot undo a finished reclaim: the
	// entry keeps whatever state it already reached, and only its noted time moves.
	existing, ok, err := p.read(kbID, versionID)
	if err != nil {
		return err
	}
	entry := Entry{KBID: kbID, VersionID: versionID, State: Pending, NotedAt: at}
	if ok {
		entry.State = existing.State
		entry.DoneAt = existing.DoneAt
	}
	return p.db.Set(entryKey(kbID, versionID), encodeEntry(entry), pebble.Sync)
}

// MarkDone implements Store.
func (p *PebbleStore) MarkDone(_ context.Context, kbID string, versionID int64, at time.Time) error {
	entry, ok, err := p.read(kbID, versionID)
	if err != nil {
		return err
	}
	if !ok {
		// Nothing was noted, so there is nothing to finish. The reconciliation
		// reclaims such a version; recording a tombstone here would invent a
		// notification that never arrived.
		return nil
	}
	entry.State = Done
	entry.DoneAt = at
	return p.db.Set(entryKey(kbID, versionID), encodeEntry(entry), pebble.Sync)
}

// Contains implements Store.
func (p *PebbleStore) Contains(_ context.Context, kbID string, versionID int64) (bool, error) {
	_, ok, err := p.read(kbID, versionID)
	return ok, err
}

// ContainsKB implements Store: a KB-level entry (VersionID 0) refuses every version of
// that knowledge base.
func (p *PebbleStore) ContainsKB(_ context.Context, kbID string) (bool, error) {
	_, ok, err := p.read(kbID, 0)
	return ok, err
}

// Pending implements Store.
func (p *PebbleStore) Pending(_ context.Context) ([]Entry, error) {
	return p.scan(func(e Entry) bool { return e.State == Pending })
}

// Prune implements Store.
func (p *PebbleStore) Prune(ctx context.Context, now time.Time, grace time.Duration) (int, error) {
	if grace < 0 {
		grace = DefaultGrace
	}
	stale, err := p.scan(func(e Entry) bool { return e.State == Done && now.Sub(e.DoneAt) > grace })
	if err != nil {
		return 0, err
	}
	for _, entry := range stale {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := p.db.Delete(entryKey(entry.KBID, entry.VersionID), pebble.Sync); err != nil {
			return 0, fmt.Errorf("tombstone: prune %s/%d: %w", entry.KBID, entry.VersionID, err)
		}
	}
	return len(stale), nil
}

// SyncSeq implements Store.
func (p *PebbleStore) SyncSeq(_ context.Context) (SyncState, bool, error) {
	value, closer, err := p.db.Get(syncKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return SyncState{}, false, nil
	}
	if err != nil {
		return SyncState{}, false, fmt.Errorf("tombstone: read the sync watermark: %w", err)
	}
	defer closer.Close()
	state, err := decodeSyncState(value)
	if err != nil {
		return SyncState{}, false, err
	}
	return state, true, nil
}

// NoteBatch implements Store: the page's entries and the watermark move together.
func (p *PebbleStore) NoteBatch(ctx context.Context, state SyncState, entries []DeletedVersion) error {
	batch := p.db.NewBatch()
	defer batch.Close()

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		kbID, versionID, at := entry.KBID, entry.VersionID, time.Now()
		existing, ok, err := p.read(kbID, versionID)
		if err != nil {
			return err
		}
		record := Entry{KBID: kbID, VersionID: versionID, State: Pending, NotedAt: at}
		if ok {
			// A retried page must not resurrect a finished reclaim.
			record.State = existing.State
			record.DoneAt = existing.DoneAt
		}
		if err := batch.Set(entryKey(kbID, versionID), encodeEntry(record), nil); err != nil {
			return fmt.Errorf("tombstone: batch %s/%d: %w", kbID, versionID, err)
		}
	}
	encoded, err := encodeSyncState(state)
	if err != nil {
		return err
	}
	if err := batch.Set(syncKey, encoded, nil); err != nil {
		return fmt.Errorf("tombstone: batch the sync watermark: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("tombstone: commit %d entries and the watermark: %w", len(entries), err)
	}
	return nil
}

// encodeSyncState / decodeSyncState are a length-prefixed epoch plus an 8-byte
// sequence, so the value stays opaque to Pebble and self-describing to us.
func encodeSyncState(state SyncState) ([]byte, error) {
	if len(state.Epoch) > 0xFFFF {
		return nil, fmt.Errorf("tombstone: epoch is %d bytes, too long to encode", len(state.Epoch))
	}
	buf := make([]byte, 2+len(state.Epoch)+8)
	binary.BigEndian.PutUint16(buf[:2], uint16(len(state.Epoch)))
	copy(buf[2:], state.Epoch)
	binary.BigEndian.PutUint64(buf[2+len(state.Epoch):], uint64(state.Seq))
	return buf, nil
}

func decodeSyncState(value []byte) (SyncState, error) {
	if len(value) < 10 {
		return SyncState{}, fmt.Errorf("tombstone: sync watermark is %d bytes, want at least 10", len(value))
	}
	nameLen := int(binary.BigEndian.Uint16(value[:2]))
	if len(value) != 2+nameLen+8 {
		return SyncState{}, fmt.Errorf("tombstone: sync watermark is %d bytes, want %d", len(value), 2+nameLen+8)
	}
	return SyncState{
		Epoch: string(value[2 : 2+nameLen]),
		Seq:   int64(binary.BigEndian.Uint64(value[2+nameLen:])),
	}, nil
}

// scan walks the whole table and returns the entries keep accepts.
func (p *PebbleStore) scan(keep func(Entry) bool) ([]Entry, error) {
	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: keyPrefix,
		UpperBound: pebbleutil.PrefixSuccessor(keyPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("tombstone: iterate: %w", err)
	}
	defer iter.Close()

	var out []Entry
	for iter.First(); iter.Valid(); iter.Next() {
		kbID, versionID, err := decodeEntryKey(iter.Key())
		if err != nil {
			return nil, err
		}
		entry, err := decodeEntry(kbID, versionID, iter.Value())
		if err != nil {
			return nil, err
		}
		if keep(entry) {
			out = append(out, entry)
		}
	}
	return out, iter.Error()
}

// decodeEntryKey recovers (kbID, versionID) from an entry key, using the shared
// encoding's own length prefix so it stays correct whatever the strings contain.
func decodeEntryKey(key []byte) (string, int64, error) {
	rest := key[len(keyPrefix):]
	if len(rest) < 4 {
		return "", 0, fmt.Errorf("tombstone: entry key %x: too short for a length prefix", key)
	}
	nameLen := int(binary.BigEndian.Uint32(rest[:4]))
	if nameLen == 0 || len(rest) < 4+nameLen {
		return "", 0, fmt.Errorf("tombstone: entry key %x: truncated knowledge base id", key)
	}
	kbID := string(rest[4 : 4+nameLen])
	versionID, ok := pebbleutil.DecodeVersionID(rest[4+nameLen:])
	if !ok {
		return "", 0, fmt.Errorf("tombstone: entry key %x: truncated version id", key)
	}
	return kbID, versionID, nil
}
