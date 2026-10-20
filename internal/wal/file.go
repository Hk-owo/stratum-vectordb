package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"

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

// ErrLogCorrupt reports a log damaged in a way no interrupted write can explain:
// a record whose frame reads back in full but does not check out, with intact
// records behind it. The frame is [type][len][payload][crc] and the checksum is
// written last, so a write cut short by a crash stops before the checksum instead
// of producing a wrong one — which means those bytes changed on disk after they
// were written (failing media, bit rot, another process writing the same path).
// Opening or rewriting such a log fails rather than truncating at the damage,
// because the records behind it were often already acknowledged to their callers.
//
// It is exported so the startup path can tell this apart from an ordinary I/O
// failure: the remedy is an operator's decision, not a retry.
var ErrLogCorrupt = errors.New("wal: log is corrupt")

// ErrUnknownRecord reports a record whose framing and checksum are both valid but
// whose type tag this build does not understand — a log written by a newer
// binary. Deliberately separate from ErrLogCorrupt: the log is in perfect
// condition and truncating it would destroy healthy records, so the remedy is to
// run the matching version, not to inspect the device.
var ErrUnknownRecord = errors.New("wal: unsupported record type")

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
// Every Write* call returns only once its record is durably on disk. That
// guarantee is what the rest of the system leans on (a COMMIT that returns means
// the version's data is vouched for), so it is kept exactly — but the fsyncs are
// group-committed: a Write* appends its record under the log lock and then waits,
// outside it, for the next fsync round to cover the record. Concurrent writers
// therefore share one fsync instead of serialising N of them, which is the whole
// cost of the write path when several knowledge bases are written at once.
//
// See awaitDurable for the protocol and for what a failed round means.
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
	// WriteBegin/WriteVersionID, so it also answers ChangesInRange (a lagging
	// peer's backfill request, §7.5) for versions written since this process
	// started — not only for those on disk at Open.
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
	// input ChangesInRange hands a lagging peer would then belong to the wrong
	// knowledge base.
	pendingBeginByKB map[string]beginData

	// pendingVersionIDByKB holds the VERSION_ID records still awaiting their
	// BEGIN, one per knowledge base — the mirror of pendingBeginByKB, and the
	// reason both directions need a slot (C5 of docs/code-review-2026-10-18.md).
	//
	// The pairing is written from two places with the two possible orders: the
	// inline write path writes BEGIN then proposes (apply writes VERSION_ID), while
	// the dispatch topology proposes first (apply writes VERSION_ID) and only then
	// fans the storage write out to this node, which writes BEGIN. A single
	// BEGIN-side slot silently bound each version to the PREVIOUS version's replay
	// input in the second order, and ChangesInRange then handed a lagging peer the
	// wrong changes.
	pendingVersionIDByKB map[string]int64

	// broken is set when the log can no longer be trusted to append: a partial
	// record could not be rolled back (see writeRecordLocked), or a group-commit
	// fsync round failed. Every later write fails with it, because appending after
	// an unknown partial record only shows up on the next restart — as silently
	// missing history.
	broken error

	replayCounters map[replayKey]int

	// --- group commit (awaitDurable) ---
	//
	// appended is the offset just past the last record written into the file. Only
	// diagnostics and tests read it (published so the group-commit test can wait for
	// writers to gather); durability is judged by seq, below.
	appended atomic.Int64

	// appendedSeq counts the records written into the log, so a writer can be told
	// "your record is durable" without referring to a byte offset. Offsets alone
	// cannot express that: Compact/DeleteByKB swap in a NEW file, whose offsets mean
	// something else entirely, and a waiter holding an old offset would either be
	// satisfied by an unrelated position or never be satisfied at all.
	appendedSeq atomic.Uint64

	// syncFor is the handle fsync rounds use: a copy of w.file the round can reach
	// without taking mu. It is atomic because the round runs with mu released
	// (holding it would serialise appends behind the fsync, which is the cost this
	// exists to remove), while Compact/DeleteByKB replace the file under mu. The
	// pointer identity is how a round notices the log was swapped under it.
	syncFor atomic.Pointer[os.File]

	// syncMu guards the round state below. Lock order is mu → syncMu, never the
	// other way: a round holds syncMu with mu released, and every path that takes
	// both (Compact's swap) takes mu first.
	syncMu sync.Mutex
	// syncCond broadcasts the end of a round (success or failure) and the log swap.
	syncCond *sync.Cond
	// syncing is true while one goroutine is inside Sync for the current file.
	syncing bool
	// syncGen counts completed rounds, so a waiter can tell "a round finished while
	// I slept" from a spurious wakeup.
	syncGen uint64
	// durableSeq is the highest record sequence number known to be on stable
	// storage. Monotone: a round only ever raises it, and a log swap raises it to
	// "everything written so far" (the replacement file already holds those records
	// and was synced before the rename).
	durableSeq uint64
	// syncErr is set by the first failed round and never cleared: after an fsync
	// that did not reach the device, no later round on this log can be trusted to
	// have covered earlier records either, so the log goes fail-stop (see broken).
	syncErr error

	// syncFile is the fsync call itself, as a field so tests can count rounds,
	// stretch them (to make concurrent writers overlap on purpose) and make them
	// fail.
	syncFile func(*os.File) error
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
		file:                 f,
		path:                 path,
		versionIDsWritten:    make(map[int64]bool),
		committedVersions:    make(map[int64]bool),
		deleteMarked:         make(map[string]bool),
		deleteCompleted:      make(map[string]bool),
		versionDeleteMarked:  make(map[int64]string),
		versionDeleteDone:    make(map[int64]bool),
		beginDataByVersion:   make(map[int64]beginData),
		pendingBeginByKB:     make(map[string]beginData),
		pendingVersionIDByKB: make(map[string]int64),
		cursors:              make(map[string]int64),
		replayCounters:       make(map[replayKey]int),
		syncFile:             func(f *os.File) error { return f.Sync() },
	}
	w.syncCond = sync.NewCond(&w.syncMu)
	w.syncFor.Store(f)

	if err := w.rebuildIndex(); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: rebuild index from %s: %w", path, err)
	}
	// Publish where appends will start. The records already in the file are durable
	// by construction (rebuildIndex treats a short tail as the end of the log, and
	// everything before it was fsynced by whoever wrote it), and they carry no
	// sequence numbers, so nothing has to be marked durable here: the first new
	// record gets seq 1 and its own round.
	if end, err := f.Seek(0, io.SeekCurrent); err == nil {
		w.appended.Store(end)
	}

	return w, nil
}

// Close releases the underlying file handle.
//
// mu is enough: a write releases mu only while it waits on a sync round, and that
// wait is what the file handle is needed for. Closing between an append and its
// round would leave the round syncing a closed descriptor, so the round is
// drained first.
func (w *FileWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.drainSyncLocked()
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
	// ChangesInRange then reports the version as missing, and a lagging peer falls back
	// to a full-state transfer, which is recoverable — a wrong pairing would hand it
	// another knowledge base's changes.
	var validEnd int64
	pendingBegin := make(map[string]beginData) // KB → BEGIN awaiting its VERSION_ID
	var pendingVersionID int64                 // VERSION_ID seen, waiting for its BEGIN
	for {
		rec, recLen, err := readRecord(r)
		if err != nil {
			if errors.Is(err, ErrUnknownRecord) {
				// The record checked out — its checksum is valid — this build just
				// cannot say what it means. Cutting the log there would delete records
				// that are in perfect condition, so the log stays as it is and the
				// node refuses to start until it runs a binary that understands it.
				return fmt.Errorf("wal: %s: record at offset %d: %w", w.path, validEnd, err)
			}
			// Otherwise it is either the tail of a write a crash cut short (cut it
			// away, as before) or damage in the middle (refuse: the records behind it
			// may already have been acknowledged, and the peers may hold the other
			// half of the changes they describe). Only the rest of the file can say
			// which, so ask it.
			if derr := damageIsInTheMiddle(w.path, w.file, validEnd, err); derr != nil {
				return derr
			}
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
//
// A failure means one of two very different things, and the caller has to tell
// them apart — see damageIsInTheMiddle and ErrUnknownRecord:
//
//   - a short read: the file ends before the record does, which is what a write
//     cut short by a crash leaves behind;
//   - a record whose frame reads back in full but does not check out, or a length
//     prefix that cannot be trusted: the bytes changed after they were written.
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
		return parsedRecord{}, 0, fmt.Errorf("%w: implausible payload length %d", ErrLogCorrupt, payloadLen)
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
		return parsedRecord{}, 0, fmt.Errorf("%w: checksum mismatch", ErrLogCorrupt)
	}

	rec, err := decodeRecord(kind, payload)
	if err != nil {
		return parsedRecord{}, 0, err
	}
	totalLen := int64(recordHeaderLen) + int64(payloadLen) + int64(recordCRCLen)
	return rec, totalLen, nil
}

// decodeRecord interprets a record's payload. Shared by the recovery reader, the
// compaction rewriter and the damage scan on purpose: the scan's verdict ("there
// is another readable record behind the damage") is only trustworthy if it is
// asking the same question recovery answers.
//
// Errors wrap ErrLogCorrupt for a payload whose shape is wrong and return
// ErrUnknownRecord for a type tag this build does not know — the second is not
// damage, since the checksum over those bytes is valid.
func decodeRecord(kind recordType, payload []byte) (parsedRecord, error) {
	rec := parsedRecord{kind: kind}
	switch kind {
	case recordTypeBegin:
		bd, err := decodeBeginPayload(payload)
		if err != nil {
			return parsedRecord{}, fmt.Errorf("%w: corrupt BEGIN payload: %w", ErrLogCorrupt, err)
		}
		rec.begin = bd
	case recordTypeVersionID:
		// 8 bytes from records written before the payload carried the knowledge
		// base; more is the version id followed by the kbID's raw bytes (M7 of
		// docs/code-review-2026-09-24.md).
		if len(payload) < 8 {
			return parsedRecord{}, fmt.Errorf("%w: malformed versionID payload length %d", ErrLogCorrupt, len(payload))
		}
		rec.versionID = int64(binary.BigEndian.Uint64(payload[:8]))
		rec.kbID = string(payload[8:])
	case recordTypeCommit:
		if len(payload) != 8 {
			return parsedRecord{}, fmt.Errorf("%w: malformed commit payload length %d", ErrLogCorrupt, len(payload))
		}
		rec.versionID = int64(binary.BigEndian.Uint64(payload))
	case recordTypeDeleteMark, recordTypeDeleteComplete:
		rec.kbID = string(payload)
	case recordTypeVersionDeleteMark, recordTypeVersionDeleteComplete:
		if len(payload) < 8 {
			return parsedRecord{}, fmt.Errorf("%w: malformed version-delete payload length %d", ErrLogCorrupt, len(payload))
		}
		rec.versionID = int64(binary.BigEndian.Uint64(payload[:8]))
		rec.kbID = string(payload[8:])
	case recordTypeCursor:
		kbID, versionID, err := decodeCursorPayload(payload)
		if err != nil {
			return parsedRecord{}, fmt.Errorf("%w: %w", ErrLogCorrupt, err)
		}
		rec.kbID = kbID
		rec.versionID = versionID
	default:
		return parsedRecord{}, fmt.Errorf("%w: 0x%02x", ErrUnknownRecord, byte(kind))
	}
	return rec, nil
}

// maxDamageScan bounds how much of the log damageIsInTheMiddle will read while
// looking for an intact record behind the damage — and doubles as the proof that
// there is one. A write path that is interrupted is interrupted once, and one
// record is at most maxSanePayload long, so a crashed write can leave at most one
// unterminated record behind: anything longer than that cannot be its tail, which
// means the log continues past the damage. The rest is read whole because a record
// that long has to fit inside the scanned window for the scan to recognise it.
const maxDamageScan = maxSanePayload + recordHeaderLen + recordCRCLen

// damageIsInTheMiddle decides what a failed record read at recordStart means for
// the log. It returns an error wrapping ErrLogCorrupt when readable records still
// follow that point — damage in the middle, where truncating would delete records
// that were already acknowledged — and nil when nothing readable follows, which is
// the tail a crashed write leaves behind and which recovery may cut away.
//
// Neither the read error nor the position answers this on its own. A torn write
// stops before the checksum, so it shows up as a short read; a length prefix
// damaged into pointing past the end of the file shows up the same way. What
// separates them is whether good records are still behind the damage, and finding
// those means scanning byte by byte, because the framing is precisely what cannot
// be trusted.
//
// The asymmetry decides the bias: a false positive costs a refusal to start, which
// an operator clears in a minute, while a false negative costs every record after
// the damage. So any offset whose record parses and checks out counts as a
// survivor, including the ones that only look like one.
func damageIsInTheMiddle(path string, f *os.File, recordStart int64, cause error) error {
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("wal: %s: inspect the log after a damaged record at offset %d: %w", path, recordStart, err)
	}
	from := recordStart + 1
	rest := info.Size() - from
	if rest <= 0 {
		return nil
	}
	if rest > maxDamageScan {
		return fmt.Errorf("%w: %s: record at offset %d does not check out (%v) with %d bytes behind it",
			ErrLogCorrupt, path, recordStart, cause, rest)
	}
	buf := make([]byte, rest)
	if _, err := io.ReadFull(io.NewSectionReader(f, from, rest), buf); err != nil {
		return fmt.Errorf("wal: %s: read the log after a damaged record at offset %d: %w", path, recordStart, err)
	}
	if off, ok := findRecordIn(buf); ok {
		return fmt.Errorf("%w: %s: record at offset %d does not check out (%v), but the log continues at offset %d",
			ErrLogCorrupt, path, recordStart, cause, from+int64(off))
	}
	return nil
}

// findRecordIn returns the first offset in data at which a complete, checksum-
// valid, interpretable record begins. It answers the only question the damage scan
// has: is there anything readable left in here?
func findRecordIn(data []byte) (int, bool) {
	for off := 0; off+recordHeaderLen+recordCRCLen <= len(data); off++ {
		payloadLen := int(binary.BigEndian.Uint32(data[off+1 : off+5]))
		if payloadLen > maxSanePayload {
			continue
		}
		body := off + recordHeaderLen
		end := body + payloadLen + recordCRCLen
		if end > len(data) {
			continue
		}
		wantCRC := binary.BigEndian.Uint32(data[end-recordCRCLen : end])
		gotCRC := crc32.ChecksumIEEE(data[off:body])
		gotCRC = crc32.Update(gotCRC, crc32.IEEETable, data[body:body+payloadLen])
		if gotCRC != wantCRC {
			continue
		}
		if _, err := decodeRecord(recordType(data[off]), data[body:body+payloadLen]); err != nil {
			continue
		}
		return off, true
	}
	return 0, false
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

// writeRecordLocked appends a single framed record to the file and returns its
// sequence number plus the offset just past it. Must be called with w.mu held; the
// caller is responsible for calling awaitDurable on the returned sequence number
// AFTER releasing mu, because making the record durable is what several writers
// share (see awaitDurable).
//
// A record is three writes (header, payload, CRC). If any of them fails the file
// is rewound to where the record started before the error is returned: recovery
// stops at the first unparseable record and truncates from there, so a half
// record left in the middle of the log silently discards every valid record after
// it — ones whose callers were already told the write succeeded (H6 of
// docs/code-review-2026-10-18.md).
func (w *FileWAL) writeRecordLocked(kind recordType, payload []byte) (uint64, int64, error) {
	if w.broken != nil {
		return 0, 0, fmt.Errorf("wal: %s cannot accept writes: %w", w.path, w.broken)
	}

	header := make([]byte, recordHeaderLen)
	header[0] = byte(kind)
	binary.BigEndian.PutUint32(header[1:5], uint32(len(payload)))

	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, payload)
	crcBytes := make([]byte, recordCRCLen)
	binary.BigEndian.PutUint32(crcBytes, crc)

	start, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("wal: %s: record start offset: %w", w.path, err)
	}
	if err := w.appendRecord(header, payload, crcBytes); err != nil {
		w.rewindTo(start)
		return 0, 0, err
	}
	// Read the end back from the file rather than computing it, so this value is
	// whatever the kernel actually accepted.
	end, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("wal: %s: record end offset: %w", w.path, err)
	}
	w.appended.Store(end)
	return w.appendedSeq.Add(1), end, nil
}

// appendRecord performs the three writes of one record. Split out so
// writeRecordLocked can rewind on any of their failures. It deliberately does not
// fsync: that is the group-commit round's job (awaitDurable).
func (w *FileWAL) appendRecord(header, payload, crcBytes []byte) error {
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
	return nil
}

// rewindTo truncates the log back to start (dropping a partially written record)
// and repositions the write cursor there, so the next record begins at a clean
// boundary. If any of that fails the log is marked broken: continuing to append
// after a partial record of unknown length is worse than refusing to, because the
// damage would only appear on the next restart.
func (w *FileWAL) rewindTo(start int64) {
	if err := w.file.Truncate(start); err != nil {
		w.broken = fmt.Errorf("roll back partial record at offset %d: %w", start, err)
		return
	}
	if _, err := w.file.Seek(start, io.SeekStart); err != nil {
		w.broken = fmt.Errorf("reposition after rolling back partial record at offset %d: %w", start, err)
		return
	}
	w.appended.Store(start)
	// The truncate has to reach the disk, otherwise a restart can still see the
	// partial record. This one is outside the group-commit rounds on purpose: it is
	// a rollback, so it must not share a fsync with records that are being kept.
	//
	// No durability bookkeeping is needed here: the record that was rolled back has
	// no waiter (its writer already has the error), and every earlier record keeps
	// the sequence number it was given, so durableSeq stays a truthful — if
	// conservative — statement about them.
	if err := w.file.Sync(); err != nil {
		w.broken = fmt.Errorf("sync after rolling back partial record at offset %d: %w", start, err)
	}
}

// awaitDurable blocks until the record with sequence number seq is known to be on
// stable storage, and reports the error of the round that tried and failed.
//
// One goroutine at a time plays the leader: it reads the sequence number the file
// has reached, syncs, and then records that everything up to it reached the device.
// Writers that appended while the fsync was in flight are covered by it too, so N
// concurrent writers cost one fsync instead of N — the whole point, since the fsync
// is the write path's dominant cost once several knowledge bases are written at
// once.
//
// Two orderings matter:
//   - The leader reads appendedSeq BEFORE syncing. Anything appended after that
//     read is not claimed to be durable by this round; its own writer leads (or
//     joins) the next one.
//   - Waiters are told "durable" by sequence number, never by byte offset. A log
//     swap (Compact/DeleteByKB) brings in a different file whose offsets mean
//     something else entirely; a sequence number stays meaningful across it, and
//     adoptFileLocked marks every record written so far as durable because the
//     replacement file already holds them and was synced before the rename.
//
// A round that fails poisons the log (syncErr, and broken through markBroken): an
// fsync that did not reach the device says nothing about the records before it
// either, and there is no way to tell which of the covered records are on disk. The
// previous implementation could roll a single failed record back; with a round
// covering several, refusing all of them — fail-stop — is the only safe answer.
//
// Must be called with mu released.
func (w *FileWAL) awaitDurable(seq uint64) error {
	w.syncMu.Lock()
	for {
		if w.syncErr != nil {
			err := w.syncErr
			w.syncMu.Unlock()
			w.markBroken(fmt.Errorf("fsync round failed: %w", err))
			return err
		}
		if w.durableSeq >= seq {
			w.syncMu.Unlock()
			return nil
		}
		if w.syncing {
			gen := w.syncGen
			for w.syncGen == gen && w.syncErr == nil {
				w.syncCond.Wait()
			}
			continue
		}

		// Lead the round.
		w.syncing = true
		file := w.syncFor.Load()
		target := w.appendedSeq.Load()
		w.syncMu.Unlock()

		var err error
		if file == nil {
			err = fmt.Errorf("wal: %s: no file to fsync", w.path)
		} else {
			err = w.syncFile(file)
		}

		w.syncMu.Lock()
		w.syncing = false
		if w.syncFor.Load() != file {
			// Compact/DeleteByKB swapped the log while this round was syncing: the
			// result describes a file that is no longer the log. adoptFileLocked has
			// already marked everything written so far durable, so just start the
			// question over against the new file.
			w.finishRoundLocked()
			continue
		}
		if err != nil {
			w.syncErr = err
		} else if target > w.durableSeq {
			w.durableSeq = target
		}
		w.finishRoundLocked()
	}
}

// finishRoundLocked publishes the end of a round: bump the generation (so waiters
// can tell a finished round from a spurious wakeup) and wake everyone. Must be
// called with syncMu held.
func (w *FileWAL) finishRoundLocked() {
	w.syncGen++
	w.syncCond.Broadcast()
}

// adoptFileLocked points the group-commit machinery at a newly opened log file.
// Called by Compact/DeleteByKB after the rename, with mu held.
//
// Every record written so far is marked durable: the replacement file holds those
// records (compaction copies what it keeps, and the ones it drops were unreachable
// — see Compact), and it was fsynced before being renamed into place. Without this
// a waiter holding a sequence number from before the swap would wait for a round
// against the old file that no longer exists.
func (w *FileWAL) adoptFileLocked(f *os.File, size int64) {
	w.syncMu.Lock()
	w.syncFor.Store(f)
	w.appended.Store(size)
	if written := w.appendedSeq.Load(); written > w.durableSeq {
		w.durableSeq = written
	}
	w.finishRoundLocked()
	w.syncMu.Unlock()
}

// drainSyncLocked waits out an in-flight fsync round before the caller closes or
// replaces the file. Must be called with mu held (and syncMu is taken second, in
// the documented order).
func (w *FileWAL) drainSyncLocked() {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	for w.syncing {
		w.syncCond.Wait()
	}
}

// markBroken records that no further append can be trusted. First error wins: it
// names the failure that made the log unusable.
func (w *FileWAL) markBroken(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.broken == nil {
		w.broken = err
	}
}

// appendDurable runs one record's write under the log lock and then waits, with
// the lock released, for a group-commit round to cover it.
//
// fn returns 0 as the sequence number when there was nothing to write (the record
// is already in the log — every Write* is idempotent). The lock is what makes the
// append atomic against other writers and against Compact/DeleteByKB swapping the
// file; the wait has to happen outside it, or the next writer could not append while
// this one's fsync is in flight and there would be nothing to group.
//
// fn runs the in-memory index update too, before the wait. That is deliberate: the
// update has to be inside the same critical section as the append (or the
// idempotency check and the append could be split by another writer), and the
// alternative — a second critical section after the wait — would let two writers
// append the same record. The cost is that a failed round leaves the index claiming
// a record that may not be durable; a failed round also poisons the log (see
// awaitDurable), so those maps are never read again in that process, and the next
// start rebuilds them from the file.
func (w *FileWAL) appendDurable(fn func() (uint64, int64, error)) error {
	w.mu.Lock()
	// Checked here as well as in writeRecordLocked: a poisoned log must refuse even
	// an idempotent repeat, otherwise a caller whose write failed would be told the
	// second attempt "succeeded" by an entry the failed round put in the index.
	if w.broken != nil {
		err := w.broken
		w.mu.Unlock()
		return fmt.Errorf("wal: %s cannot accept writes: %w", w.path, err)
	}
	seq, _, err := fn()
	w.mu.Unlock()
	if err != nil || seq == 0 {
		return err
	}
	return w.awaitDurable(seq)
}

// wrapWALErr prefixes err with a per-call message when there is one. It keeps the
// Write* bodies to a single return statement, which is what makes the shared
// appendDurable shape readable at each of the nine call sites.
func wrapWALErr(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(format, append(args, err)...)
}

// WriteBegin records a transaction's replay input: kbID, parentVersionID and the
// full changes list, so a crash before COMMIT can replay the version from scratch.
func (w *FileWAL) WriteBegin(_ context.Context, kbID string, parentVersionID int64, changes []types.DocChange) error {
	bd := beginData{kbID: kbID, parentVersionID: parentVersionID, changes: changes}
	return wrapWALErr(w.appendDurable(func() (uint64, int64, error) {
		seq, end, err := w.writeRecordLocked(recordTypeBegin, encodeBeginPayload(kbID, parentVersionID, changes))
		if err != nil {
			return 0, 0, err
		}
		// Remember it for the VERSION_ID that will bind to it (and, through
		// beginDataByVersion, for a peer's later backfill request). The binding is
		// made as soon as the record is in the file, before the fsync round: a
		// failed round poisons the whole log (see awaitDurable), so there is no
		// state in which this claim is read back after the record turned out not to
		// be durable.
		//
		// Per knowledge base, because writes to different KBs are concurrent now
		// (M7 of docs/code-review-2026-09-24.md): a single slot would let this BEGIN
		// bind to another KB's VERSION_ID.
		//
		// A VERSION_ID may already be waiting for this BEGIN: under the dispatch
		// topology the proposal — and therefore the apply that writes VERSION_ID —
		// happens BEFORE the storage write that lands here (C5 of
		// docs/code-review-2026-10-18.md). Binding it now is what keeps the replay
		// input attached to the right version in that order too.
		if versionID, ok := w.pendingVersionIDByKB[kbID]; ok {
			w.beginDataByVersion[versionID] = bd
			delete(w.pendingVersionIDByKB, kbID)
		} else {
			w.pendingBeginByKB[kbID] = bd
		}
		return seq, end, nil
	}), "wal: WriteBegin: %w")
}

func (w *FileWAL) WriteVersionID(_ context.Context, kbID string, versionID int64) error {
	// The record carries the knowledge base, so the BEGIN/VERSION_ID pairing is
	// exact regardless of how two KBs' writes interleave on disk (M7 of
	// docs/code-review-2026-09-24.md). Records written before this change carry only
	// the version id; rebuildIndex still reads those (len==8) and pairs them the old
	// way.
	payload := make([]byte, 8+len(kbID))
	binary.BigEndian.PutUint64(payload[:8], uint64(versionID))
	copy(payload[8:], kbID)

	return wrapWALErr(w.appendDurable(func() (uint64, int64, error) {
		if w.versionIDsWritten[versionID] {
			return 0, 0, nil // idempotent
		}
		seq, end, err := w.writeRecordLocked(recordTypeVersionID, payload)
		if err != nil {
			return 0, 0, err
		}
		w.versionIDsWritten[versionID] = true
		// Bind the BEGIN record this transaction started with — the same pairing
		// rebuildIndex performs when it replays the pair from disk. Without this, a
		// version written by this process could not answer a peer's backfill
		// request until the next restart.
		//
		// When no BEGIN is waiting, the write is in the other order (propose/apply
		// first, storage write later: the dispatch topology), so THIS version waits
		// for its BEGIN instead of the next version silently stealing the BEGIN that
		// arrives afterwards (C5 of docs/code-review-2026-10-18.md).
		if bd, ok := w.pendingBeginByKB[kbID]; ok {
			w.beginDataByVersion[versionID] = bd
			delete(w.pendingBeginByKB, kbID)
		} else {
			w.pendingVersionIDByKB[kbID] = versionID
		}
		return seq, end, nil
	}), "wal: WriteVersionID(%d): %w", versionID)
}

func (w *FileWAL) WriteCommit(_ context.Context, versionID int64) error {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, uint64(versionID))

	return wrapWALErr(w.appendDurable(func() (uint64, int64, error) {
		if w.committedVersions[versionID] {
			return 0, 0, nil // idempotent
		}
		seq, end, err := w.writeRecordLocked(recordTypeCommit, payload)
		if err != nil {
			return 0, 0, err
		}
		w.committedVersions[versionID] = true
		return seq, end, nil
	}), "wal: WriteCommit(%d): %w", versionID)
}

func (w *FileWAL) WriteDeleteMark(_ context.Context, kbID string) error {
	return wrapWALErr(w.appendDurable(func() (uint64, int64, error) {
		if w.deleteMarked[kbID] {
			return 0, 0, nil // idempotent
		}
		seq, end, err := w.writeRecordLocked(recordTypeDeleteMark, []byte(kbID))
		if err != nil {
			return 0, 0, err
		}
		w.deleteMarked[kbID] = true
		return seq, end, nil
	}), "wal: WriteDeleteMark(%s): %w", kbID)
}

func (w *FileWAL) WriteDeleteComplete(_ context.Context, kbID string) error {
	return wrapWALErr(w.appendDurable(func() (uint64, int64, error) {
		if w.deleteCompleted[kbID] {
			return 0, 0, nil // idempotent
		}
		seq, end, err := w.writeRecordLocked(recordTypeDeleteComplete, []byte(kbID))
		if err != nil {
			return 0, 0, err
		}
		w.deleteCompleted[kbID] = true
		return seq, end, nil
	}), "wal: WriteDeleteComplete(%s): %w", kbID)
}

// WriteVersionDeleteMark records that a DeleteVersion flow has started.
// Payload: 8-byte versionID + kbID. Idempotent per versionID.
func (w *FileWAL) WriteVersionDeleteMark(_ context.Context, kbID string, versionID int64) error {
	// Built outside the closure: the payload is fixed for this call, and the closure
	// may run more than once in principle (it does not today, but nothing about the
	// helper depends on that).
	payload := versionDeletePayload(versionID, kbID)
	return wrapWALErr(w.appendDurable(func() (uint64, int64, error) {
		if _, done := w.versionDeleteMarked[versionID]; done {
			return 0, 0, nil // idempotent
		}
		seq, end, err := w.writeRecordLocked(recordTypeVersionDeleteMark, payload)
		if err != nil {
			return 0, 0, err
		}
		w.versionDeleteMarked[versionID] = kbID
		return seq, end, nil
	}), "wal: WriteVersionDeleteMark(%s,%d): %w", kbID, versionID)
}

// WriteVersionDeleteComplete records that the DeleteVersion cleanup for
// versionID has finished. Payload: 8-byte versionID + kbID. Idempotent per
// versionID.
func (w *FileWAL) WriteVersionDeleteComplete(_ context.Context, kbID string, versionID int64) error {
	payload := versionDeletePayload(versionID, kbID)
	return wrapWALErr(w.appendDurable(func() (uint64, int64, error) {
		if w.versionDeleteDone[versionID] {
			return 0, 0, nil // idempotent
		}
		seq, end, err := w.writeRecordLocked(recordTypeVersionDeleteComplete, payload)
		if err != nil {
			return 0, 0, err
		}
		w.versionDeleteDone[versionID] = true
		return seq, end, nil
	}), "wal: WriteVersionDeleteComplete(%s,%d): %w", kbID, versionID)
}

// versionDeletePayload encodes the version-delete record payload: 8-byte
// big-endian versionID followed by the raw kbID bytes.
func versionDeletePayload(versionID int64, kbID string) []byte {
	payload := make([]byte, 8+len(kbID))
	binary.BigEndian.PutUint64(payload[:8], uint64(versionID))
	copy(payload[8:], kbID)
	return payload
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
	return wrapWALErr(w.appendDurable(func() (uint64, int64, error) {
		if versionID <= w.cursors[kbID] {
			return 0, 0, nil // idempotent: the cursor is a scalar and only moves forward
		}
		seq, end, err := w.writeRecordLocked(recordTypeCursor, encodeCursorPayload(kbID, versionID))
		if err != nil {
			return 0, 0, err
		}
		w.cursors[kbID] = versionID
		return seq, end, nil
	}), "wal: WriteCursor(%s, %d): %w", kbID, versionID)
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
