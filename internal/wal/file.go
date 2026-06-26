package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"stratum/internal/types"
)

// recordType is the on-disk tag identifying a WAL record's kind. Values
// are deliberately explicit (not iota-based) since they are a persisted
// wire format — changing the numeric value of an existing tag would
// silently corrupt the interpretation of every WAL file written before
// the change.
type recordType byte

const (
	recordTypeBegin          recordType = 0x01
	recordTypeVersionID      recordType = 0x02
	recordTypeCommit         recordType = 0x03
	recordTypeDeleteMark     recordType = 0x04
	recordTypeDeleteComplete recordType = 0x05
)

// On-disk record framing: [1 byte type][4 byte big-endian payload
// length][payload][4 byte big-endian CRC32 of (type byte + length bytes +
// payload)].
//
// The CRC32 lets Recover distinguish a genuinely complete record from one
// that was cut short by a crash mid-write (the trailing bytes either fail
// the length check via io.ErrUnexpectedEOF, or — in the rare case where
// exactly enough garbage bytes happen to be present — fail the checksum).
// Either way, Recover stops reading at the first incomplete or corrupt
// record and treats everything read so far as the valid log; it does not
// error out, because a crash mid-write is an expected, recoverable
// condition, not a real error condition.
//
// Payload layouts:
//   - BEGIN: empty.
//   - VERSION_ID, COMMIT: 8-byte big-endian versionID.
//   - DELETE_MARK, DELETE_COMPLETE: raw kbID bytes (no length prefix
//     needed within the payload, since the outer record framing already
//     carries the total payload length).
const (
	recordHeaderLen = 1 + 4 // type + length
	recordCRCLen    = 4
)

// FileWAL is the real, disk-persistent WAL implementation: an append-only
// binary log plus an in-memory index (rebuilt by scanning the file once,
// on Open) tracking which versions/knowledge-bases have incomplete
// flows.
//
// Every Write* call fsyncs before returning. This trades write throughput
// for implementation simplicity and an easy-to-reason-about durability
// guarantee (a successful Write* call means the record is durably on
// disk) — consistent with the project's general preference for simpler,
// more robust mechanisms over cleverness. Batching/group-commit is a
// possible future optimization if write latency becomes a bottleneck;
// not pursued here since no test or design-doc requirement currently
// calls for it.
type FileWAL struct {
	mu   sync.Mutex
	file *os.File

	// idempotency / pending-state tracking, rebuilt from the file on Open
	// and kept up to date on every write thereafter.
	versionIDsWritten map[int64]bool
	committedVersions map[int64]bool
	deleteMarked      map[string]bool
	deleteCompleted   map[string]bool

	replayCounters map[types.PendingRecord]int
}

// NewFileWAL opens (creating if necessary) the WAL file at path, scans any
// existing contents to rebuild in-memory idempotency state, and returns a
// ready-to-use FileWAL positioned to append further records.
func NewFileWAL(path string) (*FileWAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	w := &FileWAL{
		file:              f,
		versionIDsWritten: make(map[int64]bool),
		committedVersions: make(map[int64]bool),
		deleteMarked:      make(map[string]bool),
		deleteCompleted:   make(map[string]bool),
		replayCounters:    make(map[types.PendingRecord]int),
	}

	if err := w.rebuildIndex(); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: rebuild index from %s: %w", path, err)
	}

	return w, nil
}

// Close releases the underlying file handle.
func (w *FileWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// rebuildIndex scans the file from the start, applying each valid record
// to the in-memory idempotency maps, and leaves the file positioned at
// the end of the last valid (complete, checksum-verified) record —
// discarding any trailing incomplete/corrupt bytes left by a crash
// mid-write, so subsequent appends start from a clean position rather
// than after garbage.
func (w *FileWAL) rebuildIndex() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(w.file)

	var validEnd int64
	for {
		rec, recLen, err := readRecord(r)
		if err != nil {
			break // incomplete/corrupt trailing record, or clean EOF: stop here
		}
		w.applyRecordLocked(rec)
		validEnd += recLen
	}

	// Truncate away any trailing garbage so future appends start cleanly,
	// then reposition the file offset at the end of the valid prefix.
	if err := w.file.Truncate(validEnd); err != nil {
		return fmt.Errorf("truncate trailing incomplete record: %w", err)
	}
	if _, err := w.file.Seek(validEnd, io.SeekStart); err != nil {
		return err
	}
	return nil
}

// parsedRecord is the decoded form of a single WAL record, used both
// during rebuildIndex (initial scan) and Recover (computing pending
// records from the current in-memory state, which rebuildIndex already
// populated — see Recover's implementation).
type parsedRecord struct {
	kind      recordType
	versionID int64
	kbID      string
}

// readRecord reads and validates a single record from r, returning the
// decoded record and its total on-disk length (header + payload + CRC).
// Returns an error — without distinguishing the cause further, since
// every cause is handled identically by the caller (stop reading) — if
// the stream ends before a complete, checksum-valid record is available.
func readRecord(r *bufio.Reader) (parsedRecord, int64, error) {
	header := make([]byte, recordHeaderLen)
	if _, err := io.ReadFull(r, header); err != nil {
		return parsedRecord{}, 0, err
	}
	kind := recordType(header[0])
	payloadLen := binary.BigEndian.Uint32(header[1:5])

	// Sanity bound: a corrupt length prefix (e.g. from garbage bytes
	// matching the partial-record test scenario) could otherwise claim an
	// enormous payload and force a huge allocation. No real WAL payload
	// approaches this size (the largest payload is an 8-byte versionID or
	// a kbID string), so treat anything over 1 MiB as corrupt.
	const maxSanePayload = 1 << 20
	if payloadLen > maxSanePayload {
		return parsedRecord{}, 0, fmt.Errorf("wal: implausible payload length %d, treating as corrupt trailing record", payloadLen)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return parsedRecord{}, 0, err
	}

	crcBytes := make([]byte, recordCRCLen)
	if _, err := io.ReadFull(r, crcBytes); err != nil {
		return parsedRecord{}, 0, err
	}
	wantCRC := binary.BigEndian.Uint32(crcBytes)

	gotCRC := crc32.ChecksumIEEE(header)
	gotCRC = crc32.Update(gotCRC, crc32.IEEETable, payload)
	if gotCRC != wantCRC {
		return parsedRecord{}, 0, fmt.Errorf("wal: checksum mismatch, treating as corrupt trailing record")
	}

	rec := parsedRecord{kind: kind}
	switch kind {
	case recordTypeBegin:
		// no payload
	case recordTypeVersionID, recordTypeCommit:
		if len(payload) != 8 {
			return parsedRecord{}, 0, fmt.Errorf("wal: malformed versionID payload length %d", len(payload))
		}
		rec.versionID = int64(binary.BigEndian.Uint64(payload))
	case recordTypeDeleteMark, recordTypeDeleteComplete:
		rec.kbID = string(payload)
	default:
		return parsedRecord{}, 0, fmt.Errorf("wal: unknown record type 0x%02x", kind)
	}

	totalLen := int64(recordHeaderLen) + int64(payloadLen) + int64(recordCRCLen)
	return rec, totalLen, nil
}

// applyRecordLocked updates the in-memory idempotency-tracking maps to
// reflect rec. Must be called with w.mu held (or during single-threaded
// construction in rebuildIndex, before the WAL is shared).
func (w *FileWAL) applyRecordLocked(rec parsedRecord) {
	switch rec.kind {
	case recordTypeVersionID:
		w.versionIDsWritten[rec.versionID] = true
	case recordTypeCommit:
		w.committedVersions[rec.versionID] = true
	case recordTypeDeleteMark:
		w.deleteMarked[rec.kbID] = true
	case recordTypeDeleteComplete:
		w.deleteCompleted[rec.kbID] = true
	case recordTypeBegin:
		// BEGIN carries no state of its own to track; its presence only
		// matters as a transaction-start marker for whoever reads the raw
		// log, not for idempotency bookkeeping.
	}
}

// writeRecordLocked appends a single framed record to the file and
// fsyncs before returning. Must be called with w.mu held.
func (w *FileWAL) writeRecordLocked(kind recordType, payload []byte) error {
	header := make([]byte, recordHeaderLen)
	header[0] = byte(kind)
	binary.BigEndian.PutUint32(header[1:5], uint32(len(payload)))

	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, payload)
	crcBytes := make([]byte, recordCRCLen)
	binary.BigEndian.PutUint32(crcBytes, crc)

	if _, err := w.file.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.file.Write(payload); err != nil {
			return err
		}
	}
	if _, err := w.file.Write(crcBytes); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *FileWAL) WriteBegin(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writeRecordLocked(recordTypeBegin, nil); err != nil {
		return fmt.Errorf("wal: WriteBegin: %w", err)
	}
	return nil
}

func (w *FileWAL) WriteVersionID(_ context.Context, versionID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.versionIDsWritten[versionID] {
		return nil // idempotent
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, uint64(versionID))
	if err := w.writeRecordLocked(recordTypeVersionID, payload); err != nil {
		return fmt.Errorf("wal: WriteVersionID(%d): %w", versionID, err)
	}
	w.versionIDsWritten[versionID] = true
	return nil
}

func (w *FileWAL) WriteCommit(_ context.Context, versionID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.committedVersions[versionID] {
		return nil // idempotent
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, uint64(versionID))
	if err := w.writeRecordLocked(recordTypeCommit, payload); err != nil {
		return fmt.Errorf("wal: WriteCommit(%d): %w", versionID, err)
	}
	w.committedVersions[versionID] = true
	return nil
}

func (w *FileWAL) WriteDeleteMark(_ context.Context, kbID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.deleteMarked[kbID] {
		return nil // idempotent
	}
	if err := w.writeRecordLocked(recordTypeDeleteMark, []byte(kbID)); err != nil {
		return fmt.Errorf("wal: WriteDeleteMark(%s): %w", kbID, err)
	}
	w.deleteMarked[kbID] = true
	return nil
}

func (w *FileWAL) WriteDeleteComplete(_ context.Context, kbID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.deleteCompleted[kbID] {
		return nil // idempotent
	}
	if err := w.writeRecordLocked(recordTypeDeleteComplete, []byte(kbID)); err != nil {
		return fmt.Errorf("wal: WriteDeleteComplete(%s): %w", kbID, err)
	}
	w.deleteCompleted[kbID] = true
	return nil
}

// Recover returns every PendingRecord implied by the current in-memory
// idempotency state (populated by rebuildIndex on Open, and kept current
// by every Write* call since). It does not re-scan the file — the index
// is already authoritative for everything durably written.
func (w *FileWAL) Recover(_ context.Context) ([]types.PendingRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var out []types.PendingRecord
	for versionID, written := range w.versionIDsWritten {
		if written && !w.committedVersions[versionID] {
			out = append(out, types.PendingRecord{Type: types.PendingRecordTypeVersionWrite, VersionID: versionID})
		}
	}
	for kbID := range w.deleteMarked {
		if !w.deleteCompleted[kbID] {
			out = append(out, types.PendingRecord{Type: types.PendingRecordTypeDeleteMark, KBID: kbID})
		}
	}
	return out, nil
}

func (w *FileWAL) GetReplayCounters() []types.ReplayCounter {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]types.ReplayCounter, 0, len(w.replayCounters))
	for rec, count := range w.replayCounters {
		out = append(out, types.ReplayCounter{Record: rec, RetryCount: count})
	}
	return out
}

// IncrementReplayCounter records a replay failure against rec. In-memory
// only; not persisted, and resets to empty on every process restart (a
// fresh FileWAL via NewFileWAL always starts with zero counters,
// regardless of what the underlying file contains) — per the documented
// ReplayCounter semantics.
func (w *FileWAL) IncrementReplayCounter(rec types.PendingRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.replayCounters[rec]++
}

var _ WAL = (*FileWAL)(nil)
