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
	recordTypeCursor                recordType = 0x08
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
//   - CURSOR: uint32 big-endian kbID length followed by the raw kbID
//     bytes, then an 8-byte big-endian versionID. It is deliberately not
//     shaped like DELETE_MARK's "the payload is exactly the kbID": the
//     version is what the record is about, so both fields are explicit.
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

	// path is where the log lives. Compaction needs it: it writes a rewritten log
	// beside the original and swaps it in with an atomic rename.
	path string

	// idempotency / pending-state tracking, rebuilt from the file on Open
	// and kept up to date on every write thereafter.
	versionIDsWritten   map[int64]bool
	committedVersions   map[int64]bool
	deleteMarked        map[string]bool
	deleteCompleted     map[string]bool
	versionDeleteMarked map[int64]string // versionID -> kbID
	versionDeleteDone   map[int64]bool

	// beginDataByVersion maps each versionID whose VERSION_ID record is in
	// the log to the replay input of the BEGIN record that preceded it. The
	// pairing is PER KNOWLEDGE BASE (see pendingBeginByKB): writes to different
	// knowledge bases run concurrently, so their BEGIN/VERSION_ID pairs interleave
	// in the log and a single "most recent unpaired BEGIN" could no longer identify
	// the right transaction. Within one knowledge base the coordinator still
	// serializes transactions end to end, which is what keeps the pairing
	// unambiguous there. Rebuilt by rebuildIndex on Open and kept current by
	// WriteBegin/WriteVersionID, so it also answers ChangesFor (a lagging peer's
	// backfill request, §7.5) for versions written since this process started — not
	// only for those on disk at Open.
	beginDataByVersion map[int64]beginData

	// cursors is the data cursor per knowledge base that this log has
	// recorded: the highest version the node's contiguous history was known to
	// reach. Rebuilt by rebuildIndex on Open so a restart can read it back
	// instead of inferring it (§7.8), which is the whole point of the record.
	cursors map[string]int64

	// pendingBeginByKB holds the BEGIN records still awaiting their VERSION_ID, one
	// per knowledge base — the runtime counterpart of rebuildIndex's local
	// pendingBegin. Per knowledge base rather than a single slot because writes to
	// different KBs now run concurrently (M7 of docs/code-review-2026-09-24.md): a
	// single slot would let KB A's BEGIN bind to KB B's VERSION_ID, and the replay
	// input ChangesFor hands a lagging peer would then belong to the wrong
	// knowledge base.
	pendingBeginByKB map[string]beginData

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
		path:                path,
		versionIDsWritten:   make(map[int64]bool),
		committedVersions:   make(map[int64]bool),
		deleteMarked:        make(map[string]bool),
		deleteCompleted:     make(map[string]bool),
		versionDeleteMarked: make(map[int64]string),
		versionDeleteDone:   make(map[int64]bool),
		beginDataByVersion:  make(map[int64]beginData),
		pendingBeginByKB:    make(map[string]beginData),
		cursors:             make(map[string]int64),
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

// KnowledgeBases reports which knowledge bases this log holds recorded changes for.
// Reclaiming works on the log's contents, not on what the node currently has data
// for, so this — not the storage layer's cursor map — is the right scope for a
// compaction pass: a knowledge base whose data was dropped still has log records to
// drop, and one the node never wrote has none.
func (w *FileWAL) KnowledgeBases() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	seen := make(map[string]bool, len(w.beginDataByVersion))
	for _, begin := range w.beginDataByVersion {
		if begin.kbID != "" {
			seen[begin.kbID] = true
		}
	}
	kbs := make([]string, 0, len(seen))
	for kbID := range seen {
		kbs = append(kbs, kbID)
	}
	return kbs
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

	// BEGIN and VERSION_ID are paired regardless of which of the two comes
	// first in the file; both orders occur. The original write path wrote
	// BEGIN first, so that an already-allocated version always had its replay
	// input on disk. The split write path of the control/data separation
	// (Stratum_设计文档v13.md §7.12) allocates the version ID first and writes
	// BEGIN as part of the storage layer's own transaction. Writes to one knowledge
	// base are serialized, so at most one BEGIN per KB is ever unpaired; different
	// KBs pair independently (M7 of docs/code-review-2026-09-24.md).
	//
	// The pairing itself is by kbID, because a VERSION_ID now records which
	// knowledge base it belongs to (see WriteVersionID). Records written before that
	// change carry no kbID — they come from an era when the whole write path was
	// serialized, where "the one unpaired BEGIN" was unambiguous, so legacy records
	// keep pairing that way. Anything ambiguous is left UNPAIRED on purpose:
	// ChangesFor then reports the version as missing, and a lagging peer falls back
	// to a full-state transfer, which is recoverable — a wrong pairing would hand it
	// another knowledge base's changes.
	var validEnd int64
	pendingBegin := make(map[string]beginData) // KB → BEGIN awaiting its VERSION_ID
	var pendingVersionID int64                 // VERSION_ID seen, waiting for its BEGIN
	for {
		rec, recLen, err := readRecord(r)
		if err != nil {
			break // incomplete/corrupt trailing record, or clean EOF: stop here
		}
		switch rec.kind {
		case recordTypeBegin:
			bd := rec.begin
			if pendingVersionID != 0 {
				w.beginDataByVersion[pendingVersionID] = bd
				pendingVersionID = 0
			} else {
				pendingBegin[bd.kbID] = bd
			}
		case recordTypeVersionID:
			paired := false
			if rec.kbID != "" {
				if bd, ok := pendingBegin[rec.kbID]; ok {
					w.beginDataByVersion[rec.versionID] = bd
					delete(pendingBegin, rec.kbID)
					paired = true
				}
			} else if len(pendingBegin) == 1 {
				for kb, bd := range pendingBegin {
					w.beginDataByVersion[rec.versionID] = bd
					delete(pendingBegin, kb)
				}
				paired = true
			}
			if !paired {
				// Either an ambiguous pairing or a VERSION_ID whose BEGIN comes
				// later in the log (the historical order this file still accepts):
				// hold it for the next BEGIN of that knowledge base.
				pendingVersionID = rec.versionID
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
	case recordTypeVersionID:
		// 8 bytes from records written before the payload carried the knowledge
		// base; more is the version id followed by the kbID's raw bytes (M7 of
		// docs/code-review-2026-09-24.md).
		if len(payload) < 8 {
			return parsedRecord{}, 0, fmt.Errorf("wal: malformed versionID payload length %d", len(payload))
		}
		rec.versionID = int64(binary.BigEndian.Uint64(payload[:8]))
		rec.kbID = string(payload[8:])
	case recordTypeCommit:
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
	case recordTypeCursor:
		kbID, versionID, err := decodeCursorPayload(payload)
		if err != nil {
			return parsedRecord{}, 0, err
		}
		rec.kbID = kbID
		rec.versionID = versionID
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
	case recordTypeCursor:
		// A cursor is a scalar: the newest record for a knowledge base says
		// everything an older one does, so recovery only ever needs the
		// maximum (and compaction keeps exactly that — see Compact).
		if rec.versionID > w.cursors[rec.kbID] {
			w.cursors[rec.kbID] = rec.versionID
		}
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
	// Remember it for the VERSION_ID that will bind to it (and, through
	// beginDataByVersion, for a peer's later backfill request). Only after the
	// record is durably written: an in-memory binding for a record that failed
	// to land would be a lie.
	//
	// Per knowledge base, because writes to different KBs are concurrent now
	// (M7 of docs/code-review-2026-09-24.md): a single slot would let this BEGIN
	// bind to another KB's VERSION_ID.
	w.pendingBeginByKB[kbID] = beginData{kbID: kbID, parentVersionID: parentVersionID, changes: changes}
	return nil
}

func (w *FileWAL) WriteVersionID(_ context.Context, kbID string, versionID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.versionIDsWritten[versionID] {
		return nil // idempotent
	}
	// The record carries the knowledge base, so the BEGIN/VERSION_ID pairing is
	// exact regardless of how two KBs' writes interleave on disk (M7 of
	// docs/code-review-2026-09-24.md). Records written before this change carry only
	// the version id; rebuildIndex still reads those (len==8) and pairs them the old
	// way.
	payload := make([]byte, 8+len(kbID))
	binary.BigEndian.PutUint64(payload[:8], uint64(versionID))
	copy(payload[8:], kbID)
	if err := w.writeRecordLocked(recordTypeVersionID, payload); err != nil {
		return fmt.Errorf("wal: WriteVersionID(%d): %w", versionID, err)
	}
	w.versionIDsWritten[versionID] = true
	// Bind the BEGIN record this transaction started with — the same pairing
	// rebuildIndex performs when it replays the pair from disk. Without this, a
	// version written by this process could not answer a peer's backfill
	// request until the next restart.
	if bd, ok := w.pendingBeginByKB[kbID]; ok {
		w.beginDataByVersion[versionID] = bd
		delete(w.pendingBeginByKB, kbID)
	}
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

// ChangesFor returns the replay input recorded for (kbID, versionID): the
// changes its writer applied. It answers a lagging peer's backfill request
// (Stratum_设计文档v13.md §7.5) with the same BEGIN record crash recovery
// replays — one record, two readers. ok is false when no BEGIN record is bound
// to that version: never written on this node (a follower that merely applied
// the leader's log writes none), already reclaimed, or predating the format.
func (w *FileWAL) ChangesFor(_ context.Context, kbID string, versionID int64) ([]types.DocChange, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	bd, ok := w.beginDataByVersion[versionID]
	if !ok || bd.kbID != kbID {
		return nil, false, nil
	}
	return bd.changes, true, nil
}

// VersionDelta is one version's recorded replay input: the changes its writer
// applied, plus the parent they were applied to. The parent is not decoration — a
// version's document set is derived from its parent's set plus its own changes
// (writeVersionDocList) — so a peer replaying the delta needs the authoritative
// value rather than one inferred from the (currently linear) version chain.
type VersionDelta struct {
	VersionID       int64
	ParentVersionID int64
	Changes         []types.DocChange
}

// ChangesInRange returns the recorded replay input for every version in
// (fromExclusive, toInclusive] this node wrote, keyed by version ID. A version
// with no bound BEGIN record is absent rather than present-and-empty: a lagging
// peer has to see the gap so it can fall back to a full state transfer (§7.5)
// instead of applying a version as if nothing had changed.
func (w *FileWAL) ChangesInRange(_ context.Context, kbID string, fromExclusive, toInclusive int64) (map[int64]VersionDelta, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := make(map[int64]VersionDelta)
	for versionID, bd := range w.beginDataByVersion {
		if versionID > fromExclusive && versionID <= toInclusive && bd.kbID == kbID {
			out[versionID] = VersionDelta{VersionID: versionID, ParentVersionID: bd.parentVersionID, Changes: bd.changes}
		}
	}
	return out, nil
}

// encodeCursorPayload serializes a CURSOR record's payload:
//
//	kbID:      uint32 big-endian length + raw bytes
//	versionID: int64 big-endian
func encodeCursorPayload(kbID string, versionID int64) []byte {
	payload := make([]byte, 4+len(kbID)+8)
	binary.BigEndian.PutUint32(payload[:4], uint32(len(kbID)))
	copy(payload[4:], kbID)
	binary.BigEndian.PutUint64(payload[4+len(kbID):], uint64(versionID))
	return payload
}

// decodeCursorPayload parses a CURSOR record's payload. The length check is
// exact rather than a minimum: unlike a version-delete record, whose kbID is
// context trailing a fixed prefix, both fields here are explicitly sized, so
// anything else is a corrupt payload rather than a shape this code must accept.
func decodeCursorPayload(payload []byte) (string, int64, error) {
	if len(payload) < 4 {
		return "", 0, fmt.Errorf("wal: truncated CURSOR payload (have %d bytes)", len(payload))
	}
	kbLen := int(binary.BigEndian.Uint32(payload[:4]))
	if len(payload) != 4+kbLen+8 {
		return "", 0, fmt.Errorf("wal: malformed CURSOR payload length %d (kb_id %d)", len(payload), kbLen)
	}
	return string(payload[4 : 4+kbLen]), int64(binary.BigEndian.Uint64(payload[4+kbLen:])), nil
}

// WriteCursor records that this node's CONTIGUOUS data cursor for kbID has
// reached versionID. It is what lets a restart read the cursor back instead of
// inferring it from disk facts (§7.8): the cursor lives in memory, so without
// this record a restarted node starts at 0 while holding everything.
//
// The value is a cursor, not a per-version receipt: it means "my history for
// this knowledge base is unbroken to here". A scalar is therefore enough —
// nothing about the versions below it has to be replayed — and an older value
// can be discarded rather than stored, because it says strictly less than the
// newer one that supersedes it.
//
// Idempotent and monotone: a versionID at or below the one already recorded
// returns without writing. The on-disk value can thus never move backwards,
// and a repeated advance (a retry, a replay) costs nothing.
//
// Ordering is the caller's obligation, not this method's: the cursor may never
// exceed what the node actually holds, so it is written only AFTER the data it
// describes has landed (see LocalDataPlane.advanceLocalVersion).
func (w *FileWAL) WriteCursor(_ context.Context, kbID string, versionID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if versionID <= w.cursors[kbID] {
		return nil // idempotent: the cursor is a scalar and only moves forward
	}
	if err := w.writeRecordLocked(recordTypeCursor, encodeCursorPayload(kbID, versionID)); err != nil {
		return fmt.Errorf("wal: WriteCursor(%s, %d): %w", kbID, versionID, err)
	}
	w.cursors[kbID] = versionID
	return nil
}

// RecoverCursors returns the persisted cursor of every knowledge base this log
// has a CURSOR record for. It is read once at startup.
//
// A knowledge base ABSENT from the map is one this log never recorded a cursor
// for: an older WAL written before the record type existed, or a knowledge base
// this node has not advanced since. The caller must read that as "unknown"
// rather than as "cursor 0" — the distinction is exactly what lets it fall back
// to inferring the cursor from local facts (and persist the result) instead of
// claiming a node with a full disk holds nothing.
func (w *FileWAL) RecoverCursors(_ context.Context) (map[string]int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]int64, len(w.cursors))
	for kbID, versionID := range w.cursors {
		out[kbID] = versionID
	}
	return out, nil
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
