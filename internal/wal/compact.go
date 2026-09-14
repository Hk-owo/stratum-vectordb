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
	default:
		return parsedRecord{}, nil, fmt.Errorf("wal: unknown record type 0x%02x", rec.kind)
	}
	return rec, raw, nil
}
