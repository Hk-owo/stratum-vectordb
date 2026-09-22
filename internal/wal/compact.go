package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Compact rewrites the log, dropping the BEGIN records of versions that every
// replica already holds — the physical half of §7.5's reclaim step, whose
// judgement (all required replicas report a cursor reaching V) the control layer
// makes. keepThrough maps a knowledge base to the highest version safe to forget;
// a knowledge base absent from the map keeps everything.
//
// # What it never drops
//
// CURSOR records are not changes and are therefore outside the reclaim
// watermark entirely: the watermark says which CHANGES every replica already
// has, while a cursor is this node's own knowledge of how far its history
// reaches. Only the newest cursor per knowledge base is kept — the older ones
// say strictly less, since the value is a scalar — and that is the one record
// per knowledge base a restart reads back (Stratum_设计文档v13.md §7.8).
//
// A version is only dropped when it is BOTH at or below its knowledge base's
// watermark AND committed. An uncommitted flow is exactly what Recover exists to
// finish, so its BEGIN/VERSION_ID records are kept no matter how old they are;
// dropping them would turn a crash-recovery aid into a crash-recovery hole. The
// trailing BEGIN of an in-flight transaction is likewise kept.
//
// # Crash safety
//
// The rewritten log is built beside the original, synced, and swapped in with one
// atomic rename; only then is the in-memory index trimmed. Every interruption
// before the rename leaves the original log untouched (the partial rewrite is
// discarded), and every interruption after it leaves a complete new log — there is
// no window in which neither is readable. The records themselves are copied
// verbatim rather than re-encoded, so compaction cannot corrupt a payload it does
// not understand.
//
// # Effect on readers
//
// After compaction, ChangesFor/ChangesInRange simply have no record for a reclaimed
// version. That is the intended shape: a gap reads as a MISSING key, which is what
// tells a lagging peer to fall back to a full-state transfer instead of applying a
// range it never received (§7.5).
func (w *FileWAL) Compact(ctx context.Context, keepThrough map[string]int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if w.path == "" {
		return fmt.Errorf("wal: compact: the log was opened without a path")
	}

	tmpPath := w.path + ".compact"
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: compact: create %s: %w", tmpPath, err)
	}
	// Any failure below must leave the original log in place; only the temporary
	// file is discarded.
	committed := false
	defer func() {
		if !committed {
			out.Close()
			os.Remove(tmpPath)
		}
	}()

	// reclaimed records the versions whose BEGIN/VERSION_ID were left out, so the
	// in-memory index can be trimmed to match. It deliberately does NOT drive COMMIT
	// records: a COMMIT carries only a versionID, so a versionID-keyed set could
	// match a different knowledge base's version and drop a live record. A COMMIT is
	// nine bytes and Recover wants it, so keeping all of them is both simpler and
	// safer than encoding an assumption about versionID reuse.
	reclaimed := make(map[int64]bool)
	// The newest cursor per knowledge base, read in a pass of its own before the
	// rewrite. Compaction is a streaming rewrite and cannot know whether a cursor
	// it just read is the newest one until it has seen the whole log — so the
	// question is answered first, rather than by buffering records it may drop.
	maxCursor, err := w.maxCursorByKB()
	if err != nil {
		return fmt.Errorf("wal: compact: scan cursor records: %w", err)
	}
	buf := bufio.NewWriter(out)

	in, err := os.Open(w.path)
	if err != nil {
		return fmt.Errorf("wal: compact: open %s: %w", w.path, err)
	}
	defer in.Close()
	reader := bufio.NewReader(in)

	// pending is the BEGIN that has not yet met its VERSION_ID. A BEGIN carries no
	// versionID — the pairing is what identifies it — so the decision to keep or
	// drop it can only be made once its VERSION_ID arrives.
	var pending *rawRecord
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, raw, err := readRawRecord(reader)
		if err != nil {
			// A truncated or corrupt tail is where the log ends; everything read so
			// far is the valid log.
			break
		}

		switch rec.kind {
		case recordTypeBegin:
			// An unpaired BEGIN means the previous transaction never reached its
			// VERSION_ID: keep it, exactly as Recover would need it.
			if pending != nil {
				if _, err := buf.Write(pending.raw); err != nil {
					return fmt.Errorf("wal: compact: write %s: %w", tmpPath, err)
				}
			}
			pending = raw

		case recordTypeVersionID:
			if pending == nil {
				// A VERSION_ID with no BEGIN of its own (a follower replaying the
				// leader's log never wrote one). Nothing to pair, keep as is.
				if _, err := buf.Write(raw.raw); err != nil {
					return fmt.Errorf("wal: compact: write %s: %w", tmpPath, err)
				}
				break
			}
			if w.reclaimable(pending.kbID, rec.versionID, keepThrough) {
				reclaimed[rec.versionID] = true
			} else {
				if _, err := buf.Write(pending.raw); err != nil {
					return fmt.Errorf("wal: compact: write %s: %w", tmpPath, err)
				}
				if _, err := buf.Write(raw.raw); err != nil {
					return fmt.Errorf("wal: compact: write %s: %w", tmpPath, err)
				}
			}
			pending = nil

		case recordTypeCursor:
			// Cursors are not changes: the reclaim watermark does not apply to
			// them at all (see the doc comment). Only the newest value per
			// knowledge base survives; an older one is dropped because the
			// scalar it carries is already implied by the newer one.
			if rec.versionID == maxCursor[rec.kbID] {
				if _, err := buf.Write(raw.raw); err != nil {
					return fmt.Errorf("wal: compact: write %s: %w", tmpPath, err)
				}
			}

		case recordTypeCommit:
			// Kept unconditionally: see the note on reclaimed.
			if _, err := buf.Write(raw.raw); err != nil {
				return fmt.Errorf("wal: compact: write %s: %w", tmpPath, err)
			}

		default:
			if _, err := buf.Write(raw.raw); err != nil {
				return fmt.Errorf("wal: compact: write %s: %w", tmpPath, err)
			}
		}
	}
	// A BEGIN still waiting at the end of the log is an in-flight transaction: keep
	// it, or a crash would have nothing left to replay.
	if pending != nil {
		if _, err := buf.Write(pending.raw); err != nil {
			return fmt.Errorf("wal: compact: write %s: %w", tmpPath, err)
		}
	}

	if err := buf.Flush(); err != nil {
		return fmt.Errorf("wal: compact: flush %s: %w", tmpPath, err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("wal: compact: sync %s: %w", tmpPath, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("wal: compact: close %s: %w", tmpPath, err)
	}

	// The swap. Reopening the original handle first would only add a window; the
	// rename replaces the name atomically while the old file object keeps working
	// for anyone mid-read.
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: compact: close log: %w", err)
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		// The original is untouched; reopen it and report.
		if f, rerr := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR, 0o644); rerr == nil {
			w.file = f
		}
		return fmt.Errorf("wal: compact: swap in %s: %w", w.path, err)
	}
	committed = true

	// Make the rename itself durable, so a crash cannot resurrect the old log.
	if dir, err := os.Open(filepath.Dir(w.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("wal: compact: reopen %s: %w", w.path, err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return fmt.Errorf("wal: compact: seek %s: %w", w.path, err)
	}
	w.file = f

	// Trim the index to match what the log now holds: ChangesFor must report a
	// reclaimed version as MISSING, not as "here are the changes".
	for versionID := range reclaimed {
		delete(w.beginDataByVersion, versionID)
	}
	return nil
}

// DeleteByKB rewrites the log without kbID's records — the layer a knowledge-base
// deletion never used to touch.
//
// Why the WAL needs a step of its own: every other layer (documents, chunks, chunk→doc
// mappings, version doc lists, index and bloom files) is reclaimed by
// DeleteCoordinator, and the WAL is the one it left out. That is invisible while a
// knowledge base is alive, because its recorded changes are reclaimed by watermark —
// but the watermark is computed from replicas reporting a cursor, and a DELETED
// knowledge base has no replicas reporting one, so `ReclaimableChangesThrough` answers
// "unknown" forever and every BEGIN record it left behind (each carrying the full text
// of the documents it changed) stays on disk for good.
//
// Shape follows Compact: rewrite beside the original, sync, swap in with one atomic
// rename, then trim the in-memory index to match. The judgement differs — Compact drops
// what every replica already holds, this drops everything ONE knowledge base owns,
// regardless of watermarks. Like Compact it holds w.mu for the whole rewrite, so it
// costs one pass over the log; that is why it belongs to a deletion (a deliberate, rare
// operation) and not to any periodic path.
//
// A record is matched by knowledge base where it carries one (BEGIN, the delete
// markers, CURSOR), and by the version ids collected from BEGIN records otherwise —
// VERSION_ID and COMMIT carry only a version id. An orphan VERSION_ID, written by a node
// that applied the entry without running the transaction, belongs to no BEGIN and is
// left in place: it is one bool of idempotency state, and version ids are never reused,
// so keeping it cannot collide with anything.
func (w *FileWAL) DeleteByKB(ctx context.Context, kbID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if w.path == "" {
		return fmt.Errorf("wal: delete by knowledge base: the log was opened without a path")
	}

	// Collected BEFORE the rewrite: once the file has been rewritten there is nothing
	// left to tell which version ids belonged to this knowledge base.
	owned := make(map[int64]bool)
	for versionID, bd := range w.beginDataByVersion {
		if bd.kbID == kbID {
			owned[versionID] = true
		}
	}

	tmpPath := w.path + ".dropkb"
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: delete by knowledge base: create %s: %w", tmpPath, err)
	}
	// Any failure below must leave the original log in place; only the temporary file
	// is discarded.
	committed := false
	defer func() {
		if !committed {
			out.Close()
			os.Remove(tmpPath)
		}
	}()

	buf := bufio.NewWriter(out)
	in, err := os.Open(w.path)
	if err != nil {
		return fmt.Errorf("wal: delete by knowledge base: open %s: %w", w.path, err)
	}
	defer in.Close()
	reader := bufio.NewReader(in)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, raw, err := readRawRecord(reader)
		if err != nil {
			// A truncated or corrupt tail is where the log ends; everything read so
			// far is the valid log.
			break
		}
		// raw.kbID is filled for BEGIN (its payload carries one); rec.kbID for every
		// other type that has a knowledge base at all.
		if raw.kbID == kbID || rec.kbID == kbID {
			continue
		}
		if rec.versionID != 0 && owned[rec.versionID] {
			continue
		}
		if _, err := buf.Write(raw.raw); err != nil {
			return fmt.Errorf("wal: delete by knowledge base: write %s: %w", tmpPath, err)
		}
	}

	if err := buf.Flush(); err != nil {
		return fmt.Errorf("wal: delete by knowledge base: flush %s: %w", tmpPath, err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("wal: delete by knowledge base: sync %s: %w", tmpPath, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("wal: delete by knowledge base: close %s: %w", tmpPath, err)
	}

	// The swap. Reopening the original handle first would only add a window; the
	// rename replaces the name atomically while the old file object keeps working for
	// anyone mid-read.
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: delete by knowledge base: close log: %w", err)
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		// The original is untouched; reopen it and report.
		if f, rerr := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR, 0o644); rerr == nil {
			w.file = f
		}
		return fmt.Errorf("wal: delete by knowledge base: swap in %s: %w", w.path, err)
	}
	committed = true

	// Make the rename itself durable, so a crash cannot resurrect the old log.
	if dir, err := os.Open(filepath.Dir(w.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("wal: delete by knowledge base: reopen %s: %w", w.path, err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return fmt.Errorf("wal: delete by knowledge base: seek %s: %w", w.path, err)
	}
	w.file = f

	// Trim the in-memory index to what the file now holds, so the next Recover,
	// ChangesInRange and PendingRecords all agree with it.
	for versionID := range owned {
		delete(w.beginDataByVersion, versionID)
		delete(w.versionIDsWritten, versionID)
		delete(w.committedVersions, versionID)
		delete(w.versionDeleteMarked, versionID)
		delete(w.versionDeleteDone, versionID)
	}
	// A version flagged for deletion whose BEGIN never reached this node is not in
	// `owned`; its marker carries the knowledge base, so it can be matched directly.
	for versionID, kb := range w.versionDeleteMarked {
		if kb == kbID {
			delete(w.versionDeleteMarked, versionID)
		}
	}
	delete(w.deleteMarked, kbID)
	delete(w.deleteCompleted, kbID)
	delete(w.cursors, kbID)
	if w.pendingBegin != nil && w.pendingBegin.kbID == kbID {
		w.pendingBegin = nil
	}
	for key := range w.replayCounters {
		if key.kbID == kbID {
			delete(w.replayCounters, key)
		}
	}
	return nil
}

// reclaimable reports whether versionID's BEGIN record may be dropped for kbID.
// Every "no" answer is the safe direction, so the reasons are deliberately not
// distinguished.
func (w *FileWAL) reclaimable(kbID string, versionID int64, keepThrough map[string]int64) bool {
	watermark, ok := keepThrough[kbID]
	if !ok {
		return false
	}
	if versionID > watermark {
		return false
	}
	// Uncommitted means Recover still needs it: the log is the only place the
	// replay input exists.
	return w.committedVersions[versionID]
}

// rawRecord is a record plus the exact bytes it occupied, so compaction can copy it
// without re-encoding anything.
type rawRecord struct {
	raw  []byte
	kbID string
}

// readRawRecord reads one framed record and returns it decoded alongside its raw
// bytes. readRecord cannot be reused here because it discards the bytes, and
// compaction must not re-encode a payload it did not author.
func readRawRecord(r *bufio.Reader) (parsedRecord, *rawRecord, error) {
	header := make([]byte, recordHeaderLen)
	if _, err := io.ReadFull(r, header); err != nil {
		return parsedRecord{}, nil, err
	}
	payloadLen := binary.BigEndian.Uint32(header[1:5])
	if payloadLen > maxSanePayload {
		return parsedRecord{}, nil, fmt.Errorf("wal: implausible payload length %d", payloadLen)
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return parsedRecord{}, nil, err
	}
	crcBytes := make([]byte, recordCRCLen)
	if _, err := io.ReadFull(r, crcBytes); err != nil {
		return parsedRecord{}, nil, err
	}
	wantCRC := binary.BigEndian.Uint32(crcBytes)
	gotCRC := crc32.ChecksumIEEE(header)
	gotCRC = crc32.Update(gotCRC, crc32.IEEETable, payload)
	if gotCRC != wantCRC {
		return parsedRecord{}, nil, fmt.Errorf("wal: checksum mismatch")
	}

	rec := parsedRecord{kind: recordType(header[0])}
	raw := &rawRecord{raw: append(append(append([]byte{}, header...), payload...), crcBytes...)}
	switch rec.kind {
	case recordTypeBegin:
		bd, err := decodeBeginPayload(payload)
		if err != nil {
			return parsedRecord{}, nil, fmt.Errorf("wal: corrupt BEGIN payload: %w", err)
		}
		rec.begin = bd
		raw.kbID = bd.kbID
	case recordTypeVersionID, recordTypeCommit:
		if len(payload) != 8 {
			return parsedRecord{}, nil, fmt.Errorf("wal: malformed versionID payload length %d", len(payload))
		}
		rec.versionID = int64(binary.BigEndian.Uint64(payload))
	case recordTypeDeleteMark, recordTypeDeleteComplete:
		rec.kbID = string(payload)
	case recordTypeVersionDeleteMark, recordTypeVersionDeleteComplete:
		if len(payload) < 8 {
			return parsedRecord{}, nil, fmt.Errorf("wal: malformed version-delete payload length %d", len(payload))
		}
		rec.versionID = int64(binary.BigEndian.Uint64(payload[:8]))
		rec.kbID = string(payload[8:])
	case recordTypeCursor:
		kbID, versionID, err := decodeCursorPayload(payload)
		if err != nil {
			return parsedRecord{}, nil, err
		}
		rec.kbID = kbID
		rec.versionID = versionID
		raw.kbID = kbID
	default:
		return parsedRecord{}, nil, fmt.Errorf("wal: unknown record type 0x%02x", rec.kind)
	}
	return rec, raw, nil
}

// maxCursorByKB returns the highest CURSOR value recorded per knowledge base. It
// is the pre-scan Compact needs: the rewrite keeps the newest cursor record of
// each knowledge base and drops the rest (the value is a scalar, so an older
// record says nothing the newer one does not).
//
// A corrupt or truncated tail simply ends the scan, exactly as it ends every
// other reader of this log: everything before it is the valid log.
func (w *FileWAL) maxCursorByKB() (map[string]int64, error) {
	in, err := os.Open(w.path)
	if err != nil {
		return nil, err
	}
	defer in.Close()

	out := make(map[string]int64)
	reader := bufio.NewReader(in)
	for {
		rec, _, err := readRawRecord(reader)
		if err != nil {
			break
		}
		if rec.kind != recordTypeCursor {
			continue
		}
		if rec.versionID > out[rec.kbID] {
			out[rec.kbID] = rec.versionID
		}
	}
	return out, nil
}
