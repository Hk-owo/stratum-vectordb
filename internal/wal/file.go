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
	recordTypeBegin                 recordType = 0x01
	recordTypeVersionID             recordType = 0x02
	recordTypeCommit                recordType = 0x03
	recordTypeDeleteMark            recordType = 0x04
	recordTypeDeleteComplete        recordType = 0x05
	recordTypeVersionDeleteMark     recordType = 0x06
	recordTypeVersionDeleteComplete recordType = 0x07
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
//   - BEGIN: encoded transaction replay input — see encodeBeginPayload.
//   - VERSION_ID, COMMIT: 8-byte big-endian versionID.
//   - DELETE_MARK, DELETE_COMPLETE: raw kbID bytes (no length prefix
//     needed within the payload, since the outer record framing already
//     carries the total payload length).
//   - VERSION_DELETE_MARK, VERSION_DELETE_COMPLETE: 8-byte big-endian
//     versionID followed by raw kbID bytes (kbID for recovery context;
//     the versionID alone keys idempotency).
const (
	recordHeaderLen = 1 + 4 // type + length
	recordCRCLen    = 4
)

// maxSanePayload bounds the payload length readRecord will accept before
// treating a record as corrupt garbage. The largest legitimate payload is
// a BEGIN record carrying a CreateVersion's full changes list (documents
// can be arbitrarily large), so this is deliberately generous; the bound
// still protects against a corrupt length prefix claiming a multi-GB
// payload and forcing a huge allocation.
const maxSanePayload = 1 << 26 // 64 MiB

// beginData is the replay input persisted inside a BEGIN record. The
// recovery path for a VERSION_ID-without-COMMIT record needs the original
// kbID, parentVersionID and changes to replay the storage writes from
// scratch (steps 3-6 of the write path); a crash may have left only a
// subset of those writes on disk, and that subset cannot be inferred back
// into the missing documents.
type beginData struct {
	kbID            string
	parentVersionID int64
	changes         []types.DocChange
}

// encodeBeginPayload serializes a BEGIN record's replay input:
//
//	kbID:            uint32 big-endian length + raw bytes
//	parentVersionID: int64 big-endian
//	changeCount:     uint32 big-endian
//	per change:
//	  op:      single byte (types.ChangeOp)
//	  docID:   uint32 big-endian length + raw bytes
//	  content: uint32 big-endian length + raw bytes
func encodeBeginPayload(kbID string, parentVersionID int64, changes []types.DocChange) []byte {
	payload := make([]byte, 4+len(kbID)+8+4)
	binary.BigEndian.PutUint32(payload[:4], uint32(len(kbID)))
	copy(payload[4:], kbID)
	off := 4 + len(kbID)
	binary.BigEndian.PutUint64(payload[off:off+8], uint64(parentVersionID))
	off += 8
	binary.BigEndian.PutUint32(payload[off:off+4], uint32(len(changes)))
	off += 4
	for _, ch := range changes {
		payload = append(payload, 0)
		payload[off] = byte(ch.Op)
		off++
		payload = append(payload, make([]byte, 4+len(ch.DocID)+4+len(ch.Content))...)
		binary.BigEndian.PutUint32(payload[off:off+4], uint32(len(ch.DocID)))
		off += 4
		copy(payload[off:], ch.DocID)
		off += len(ch.DocID)
		binary.BigEndian.PutUint32(payload[off:off+4], uint32(len(ch.Content)))
		off += 4
		copy(payload[off:], ch.Content)
		off += len(ch.Content)
	}
	return payload
}

// decodeBeginPayload parses a BEGIN record's payload back into its replay
// input. An empty payload (written by an older WAL format whose BEGIN
// records carried no data) yields a zero beginData with nil changes.
func decodeBeginPayload(payload []byte) (beginData, error) {
	if len(payload) == 0 {
		// Legacy empty BEGIN record: no replay input available.
		return beginData{}, nil
	}
	need := func(n int) error {
		if len(payload) < n {
			return fmt.Errorf("wal: truncated BEGIN payload (need %d bytes, have %d)", n, len(payload))
		}
		return nil
	}
	if err := need(4 + 8 + 4); err != nil {
		return beginData{}, err
	}
	kbLen := binary.BigEndian.Uint32(payload[:4])
	off := 4
	if err := need(off + int(kbLen)); err != nil {
		return beginData{}, err
	}
	kbID := string(payload[off : off+int(kbLen)])
	off += int(kbLen)
	parentVersionID := int64(binary.BigEndian.Uint64(payload[off : off+8]))
	off += 8
	count := binary.BigEndian.Uint32(payload[off : off+4])
	off += 4
	changes := make([]types.DocChange, 0, count)
	for i := uint32(0); i < count; i++ {
		if err := need(off + 1); err != nil {
			return beginData{}, err
		}
		op := types.ChangeOp(payload[off])
		off++
		readStr := func() (string, error) {
			if err := need(off + 4); err != nil {
				return "", err
			}
			n := binary.BigEndian.Uint32(payload[off : off+4])
			off += 4
			if err := need(off + int(n)); err != nil {
				return "", err
			}
			s := string(payload[off : off+int(n)])
			off += int(n)
			return s, nil
		}
		docID, err := readStr()
		if err != nil {
			return beginData{}, err
		}
		content, err := readStr()
		if err != nil {
			return beginData{}, err
		}
		changes = append(changes, types.DocChange{Op: op, DocID: docID, Content: content})
	}
	return beginData{kbID: kbID, parentVersionID: parentVersionID, changes: changes}, nil
}

// replayKey is the map key for replay-failure counters. PendingRecord
// itself cannot be a map key (it carries a slice: Changes), and the
// counters only need to identify a record by type + KB + version — not by
// its full replay payload.
type replayKey struct {
	typ       types.PendingRecordType
	kbID      string
	versionID int64
}

func makeReplayKey(rec types.PendingRecord) replayKey {
	return replayKey{typ: rec.Type, kbID: rec.KBID, versionID: rec.VersionID}
}

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
	versionIDsWritten   map[int64]bool
	committedVersions   map[int64]bool
	deleteMarked        map[string]bool
	deleteCompleted     map[string]bool
	versionDeleteMarked map[int64]string // versionID -> kbID
	versionDeleteDone   map[int64]bool

	// beginDataByVersion maps each versionID whose VERSION_ID record is in
	// the log to the replay input of the BEGIN record that preceded it.
	// Populated during rebuildIndex (sequential scan: the most recent
	// unpaired BEGIN binds to the next VERSION_ID; correctness relies on
	// the coordinator serializing each CreateVersion transaction end to
	// end, so no other BEGIN can interleave) and consulted by Recover to
	// fill PendingRecord.ParentVersionID / Changes.
	beginDataByVersion map[int64]beginData

	replayCounters map[replayKey]int
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
		file:                f,
		versionIDsWritten:   make(map[int64]bool),
		committedVersions:   make(map[int64]bool),
		deleteMarked:        make(map[string]bool),
		deleteCompleted:     make(map[string]bool),
		versionDeleteMarked: make(map[int64]string),
		versionDeleteDone:   make(map[int64]bool),
		beginDataByVersion:  make(map[int64]beginData),
		replayCounters:      make(map[replayKey]int),
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
	var lastBegin *beginData // most recent unpaired BEGIN, binds to the next VERSION_ID
	for {
		rec, recLen, err := readRecord(r)
		if err != nil {
			break // incomplete/corrupt trailing record, or clean EOF: stop here
		}
		if rec.kind == recordTypeBegin {
			bd := rec.begin
			lastBegin = &bd
		} else if rec.kind == recordTypeVersionID {
			if lastBegin != nil {
				w.beginDataByVersion[rec.versionID] = *lastBegin
				lastBegin = nil
			}
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
	begin     beginData // populated for recordTypeBegin
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
	// enormous payload and force a huge allocation. The bound (see
	// maxSanePayload) is generous enough for legitimate BEGIN records
	// carrying a full changes list.
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
		bd, err := decodeBeginPayload(payload)
		if err != nil {
			return parsedRecord{}, 0, fmt.Errorf("wal: corrupt BEGIN payload: %w", err)
		}
		rec.begin = bd
	case recordTypeVersionID, recordTypeCommit:
		if len(payload) != 8 {
			return parsedRecord{}, 0, fmt.Errorf("wal: malformed versionID payload length %d", len(payload))
		}
		rec.versionID = int64(binary.BigEndian.Uint64(payload))
	case recordTypeDeleteMark, recordTypeDeleteComplete:
		rec.kbID = string(payload)
	case recordTypeVersionDeleteMark, recordTypeVersionDeleteComplete:
		if len(payload) < 8 {
			return parsedRecord{}, 0, fmt.Errorf("wal: malformed version-delete payload length %d", len(payload))
		}
		rec.versionID = int64(binary.BigEndian.Uint64(payload[:8]))
		rec.kbID = string(payload[8:])
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
	case recordTypeVersionDeleteMark:
		w.versionDeleteMarked[rec.versionID] = rec.kbID
	case recordTypeVersionDeleteComplete:
		w.versionDeleteDone[rec.versionID] = true
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

func (w *FileWAL) WriteBegin(_ context.Context, kbID string, parentVersionID int64, changes []types.DocChange) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writeRecordLocked(recordTypeBegin, encodeBeginPayload(kbID, parentVersionID, changes)); err != nil {
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

// WriteVersionDeleteMark records that a DeleteVersion flow has started.
// Payload: 8-byte versionID + kbID. Idempotent per versionID.
func (w *FileWAL) WriteVersionDeleteMark(_ context.Context, kbID string, versionID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, done := w.versionDeleteMarked[versionID]; done {
		return nil // idempotent
	}
	if err := w.writeRecordLocked(recordTypeVersionDeleteMark, versionDeletePayload(versionID, kbID)); err != nil {
		return fmt.Errorf("wal: WriteVersionDeleteMark(%s,%d): %w", kbID, versionID, err)
	}
	w.versionDeleteMarked[versionID] = kbID
	return nil
}

// WriteVersionDeleteComplete records that the DeleteVersion cleanup for
// versionID has finished. Payload: 8-byte versionID + kbID. Idempotent per
// versionID.
func (w *FileWAL) WriteVersionDeleteComplete(_ context.Context, kbID string, versionID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.versionDeleteDone[versionID] {
		return nil // idempotent
	}
	if err := w.writeRecordLocked(recordTypeVersionDeleteComplete, versionDeletePayload(versionID, kbID)); err != nil {
		return fmt.Errorf("wal: WriteVersionDeleteComplete(%s,%d): %w", kbID, versionID, err)
	}
	w.versionDeleteDone[versionID] = true
	return nil
}

// versionDeletePayload encodes the version-delete record payload: 8-byte
// big-endian versionID followed by the raw kbID bytes.
func versionDeletePayload(versionID int64, kbID string) []byte {
	payload := make([]byte, 8+len(kbID))
	binary.BigEndian.PutUint64(payload[:8], uint64(versionID))
	copy(payload[8:], kbID)
	return payload
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
			rec := types.PendingRecord{Type: types.PendingRecordTypeVersionWrite, VersionID: versionID}
			if bd, ok := w.beginDataByVersion[versionID]; ok {
				rec.KBID = bd.kbID
				rec.ParentVersionID = bd.parentVersionID
				rec.Changes = bd.changes
			}
			// A versionID without bound BEGIN data (nil Changes) means the
			// VERSION_ID was applied by a node that never ran the local
			// Execute (a follower applying leader's log), or was written
			// by an older WAL format. It cannot be replayed locally; the
			// caller decides how to handle it (see Recover's doc comment).
			out = append(out, rec)
		}
	}
	for kbID := range w.deleteMarked {
		if !w.deleteCompleted[kbID] {
			out = append(out, types.PendingRecord{Type: types.PendingRecordTypeDeleteMark, KBID: kbID})
		}
	}
	for versionID, kbID := range w.versionDeleteMarked {
		if !w.versionDeleteDone[versionID] {
			out = append(out, types.PendingRecord{Type: types.PendingRecordTypeVersionDelete, KBID: kbID, VersionID: versionID})
		}
	}
	return out, nil
}

func (w *FileWAL) GetReplayCounters() []types.ReplayCounter {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]types.ReplayCounter, 0, len(w.replayCounters))
	for rec, count := range w.replayCounters {
		out = append(out, types.ReplayCounter{
			Record:     types.PendingRecord{Type: rec.typ, KBID: rec.kbID, VersionID: rec.versionID},
			RetryCount: count,
		})
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
	w.replayCounters[makeReplayKey(rec)]++
}

var _ WAL = (*FileWAL)(nil)
