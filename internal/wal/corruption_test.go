package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"testing"

	"stratum/internal/types"
)

// The log's framing is [type][len][payload][crc], with the CRC computed last and
// covering everything before it (file.go). Two very different situations can
// therefore both show up as "this record does not read back":
//
//   - A SHORT frame: the file ends in the middle of a record. This is what a
//     write that was cut short by a crash leaves behind, and truncating at that
//     point is exactly right — the record was never acknowledged to anyone.
//
//   - A COMPLETE frame that fails its checksum, or a length prefix that cannot
//     be trusted, with more records behind it. Nothing this process writes can
//     produce that: the checksum is appended last, so a torn write stops before
//     the checksum rather than producing a wrong one. What it means is that the
//     bytes on disk changed after they were written — a failing device, bit rot,
//     or another writer on the same path — and the records after the damage are
//     still perfectly good, often already acknowledged to their callers.
//
// The reader used to collapse both into "the log ends here" and truncate the
// file there, which permanently deleted every record after a damaged one. That
// is the read-side twin of H6 (docs/code-review-2026-10-18.md), which the write
// path already answers with rewind + fail-stop; these tests pin the same rule
// for the read path: damage in the MIDDLE refuses to open (and does not touch
// the file), while a damaged or short TAIL is still recoverable, because a crash
// mid-write must not turn into a node that will not start.

// recordFrame locates one record in a log file. It is parsed without validating
// the checksum on purpose: the tests have to find records in a file they have
// just damaged.
type recordFrame struct {
	start int64
	total int64
	kind  recordType
}

func scanRecordFrames(t *testing.T, path string) []recordFrame {
	t.Helper()
	data := mustReadLog(t, path)
	var frames []recordFrame
	for off := 0; off < len(data); {
		if off+recordHeaderLen > len(data) {
			break
		}
		payloadLen := int(binary.BigEndian.Uint32(data[off+1 : off+5]))
		total := recordHeaderLen + payloadLen + recordCRCLen
		if total < recordHeaderLen+recordCRCLen || off+total > len(data) {
			break
		}
		frames = append(frames, recordFrame{
			start: int64(off),
			total: int64(total),
			kind:  recordType(data[off]),
		})
		off += total
	}
	return frames
}

func mustReadLog(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

// flipPayloadByte changes one payload byte of frame in place, leaving the type
// tag, the length prefix and every record boundary intact — the shape damage
// takes when the framing survived but the contents did not.
func flipPayloadByte(t *testing.T, path string, frame recordFrame) {
	t.Helper()
	data := mustReadLog(t, path)
	off := frame.start + recordHeaderLen
	if off >= frame.start+frame.total-recordCRCLen {
		t.Fatalf("record at offset %d has an empty payload; nothing to flip", frame.start)
	}
	data[off] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// setPayloadLength rewrites the length prefix of frame, which is how a damaged
// record looks when the damage landed in the framing rather than the payload:
// reading it by its own length prefix does not land on the next record.
func setPayloadLength(t *testing.T, path string, frame recordFrame, payloadLen uint32) {
	t.Helper()
	data := mustReadLog(t, path)
	binary.BigEndian.PutUint32(data[frame.start+1:frame.start+5], payloadLen)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// seedLog writes two complete transactions so a damaged frame in the middle is
// unambiguously in the middle:
//
//	0 BEGIN(kb1, parent 0, add d1)  1 VERSION_ID(kb1, 1)  2 COMMIT(1)
//	3 BEGIN(kb1, parent 1, add d2)  4 VERSION_ID(kb1, 2)  5 COMMIT(2)
func seedLog(t *testing.T) (string, []recordFrame) {
	t.Helper()
	ctx := context.Background()
	path := tempWALPath(t)
	w := mustOpenFileWAL(t, path)
	for v := int64(1); v <= 2; v++ {
		changes := []types.DocChange{{
			Op:      types.ChangeOpAdd,
			DocID:   fmt.Sprintf("d%d", v),
			Content: fmt.Sprintf("c%d", v),
		}}
		if err := w.WriteBegin(ctx, "kb1", v-1, changes); err != nil {
			t.Fatalf("WriteBegin v%d: %v", v, err)
		}
		if err := w.WriteVersionID(ctx, "kb1", v); err != nil {
			t.Fatalf("WriteVersionID v%d: %v", v, err)
		}
		if err := w.WriteCommit(ctx, v); err != nil {
			t.Fatalf("WriteCommit v%d: %v", v, err)
		}
	}
	mustCloseFileWAL(t, w)

	frames := scanRecordFrames(t, path)
	if len(frames) != 6 {
		t.Fatalf("seed log has %d frames, want 6", len(frames))
	}
	return path, frames
}

// TestFileWAL_CorruptMiddleRecordRefusesToStart pins the main rule: a record
// whose frame is complete but whose checksum fails, with intact records behind
// it, is damage rather than the end of the log.
//
// Opening the log must fail, and it must fail without modifying the file. The
// alternative — truncating at the damage, which is what the reader used to do —
// silently deletes versions whose callers were already told the write succeeded,
// and whose changes the node may already have handed to its peers.
func TestFileWAL_CorruptMiddleRecordRefusesToStart(t *testing.T) {
	path, frames := seedLog(t)
	flipPayloadByte(t, path, frames[1]) // VERSION_ID(kb1, 1): five records around it

	before := mustReadLog(t, path)

	w, err := NewFileWAL(path)
	if err == nil {
		w.Close()
		t.Fatal("NewFileWAL accepted a log with a corrupt record in the middle; it must refuse to start")
	}
	if !errors.Is(err, ErrLogCorrupt) {
		t.Fatalf("error = %v, want errors.Is(err, ErrLogCorrupt) so an operator can tell settled damage from a transient I/O failure", err)
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("error %q does not name the log file; a refusal to start has to point at something", err)
	}

	if after := mustReadLog(t, path); !bytes.Equal(before, after) {
		t.Fatalf("a failed open modified the log (%d bytes before, %d after); refusing must not destroy the only copy of the evidence", len(before), len(after))
	}
}

// TestFileWAL_CorruptLengthPrefixRefusesToStart is the same damage, but in the
// framing: the length prefix of a middle record claims far more payload than the
// file holds. Reading by that prefix runs off the end of the file, which looks
// exactly like a short tail — so the reader may not decide "tail" from the read
// error alone. What tells the two apart is that valid records still follow.
func TestFileWAL_CorruptLengthPrefixRefusesToStart(t *testing.T) {
	path, frames := seedLog(t)
	setPayloadLength(t, path, frames[1], 1<<20) // five records still behind it

	before := mustReadLog(t, path)

	w, err := NewFileWAL(path)
	if err == nil {
		w.Close()
		t.Fatal("NewFileWAL accepted a log whose middle record has a corrupt length prefix; it must refuse to start")
	}
	if !errors.Is(err, ErrLogCorrupt) {
		t.Fatalf("error = %v, want errors.Is(err, ErrLogCorrupt)", err)
	}
	if after := mustReadLog(t, path); !bytes.Equal(before, after) {
		t.Fatal("a failed open modified the log")
	}
}

// TestFileWAL_DamagedLastRecordIsTreatedAsTail is the other half of the rule,
// and the reason the reader cannot simply refuse on any checksum failure: the
// same damage on the LAST record has no valid record behind it, which is
// indistinguishable from a power loss that flushed part of a page it had already
// extended the file for. Refusing to start there would make every unclean
// shutdown a manual intervention. The record is dropped and the node starts.
func TestFileWAL_DamagedLastRecordIsTreatedAsTail(t *testing.T) {
	path, frames := seedLog(t)
	last := frames[len(frames)-1] // COMMIT(2)
	flipPayloadByte(t, path, last)

	w := mustOpenFileWAL(t, path)
	defer mustCloseFileWAL(t, w)

	// Truncated back to the end of the last readable record, so appends start
	// from a clean boundary.
	if info, err := os.Stat(path); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	} else if info.Size() != last.start {
		t.Fatalf("log is %d bytes after recovery, want %d (truncated at the damaged record)", info.Size(), last.start)
	}

	// The dropped COMMIT is exactly what Recover exists to redo: version 2 was
	// written and is no longer committed.
	pending, err := w.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(pending) != 1 || pending[0].Type != types.PendingRecordTypeVersionWrite || pending[0].VersionID != 2 {
		t.Fatalf("Recover() = %+v, want one pending version write for version 2", pending)
	}
}

// TestFileWAL_ShortTailStillStarts keeps the previous behaviour honest for the
// case it was written for: a record cut short mid-write. This must stay a
// silent, successful start, because it is what every crash leaves behind.
func TestFileWAL_ShortTailStillStarts(t *testing.T) {
	path, frames := seedLog(t)
	appendPartialRecordBytes(t, path)

	w := mustOpenFileWAL(t, path)
	defer mustCloseFileWAL(t, w)

	want := frames[len(frames)-1].start + frames[len(frames)-1].total
	if info, err := os.Stat(path); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	} else if info.Size() != want {
		t.Fatalf("log is %d bytes after recovery, want %d (the partial record is cut away)", info.Size(), want)
	}

	pending, err := w.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("Recover() = %+v, want nothing pending: every seed transaction was committed", pending)
	}
}

// appendRecordOfType appends a record with a valid checksum but an arbitrary
// type tag, which is what a newer build's log looks like to this one.
func appendRecordOfType(t *testing.T, path string, kind byte, payload []byte) {
	t.Helper()
	header := make([]byte, recordHeaderLen)
	header[0] = kind
	binary.BigEndian.PutUint32(header[1:5], uint32(len(payload)))
	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, payload)
	crcBytes := make([]byte, recordCRCLen)
	binary.BigEndian.PutUint32(crcBytes, crc)

	frame := append(append(append([]byte{}, header...), payload...), crcBytes...)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open %s for append: %v", path, err)
	}
	defer f.Close()
	if _, err := f.Write(frame); err != nil {
		t.Fatalf("append record: %v", err)
	}
}

// TestFileWAL_UnknownRecordTypeRefusesToStart covers the case where the framing
// and the checksum are both fine and the log simply contains a record this build
// cannot interpret — a log written by a newer binary, say. Treating that as the
// end of the log and truncating it would destroy a log that is in perfect
// condition, so it is a refusal too, but a distinguishable one: the fix is to
// run the matching version, not to inspect the disk.
func TestFileWAL_UnknownRecordTypeRefusesToStart(t *testing.T) {
	path, _ := seedLog(t)
	appendRecordOfType(t, path, 0x7F, []byte("future"))

	w, err := NewFileWAL(path)
	if err == nil {
		w.Close()
		t.Fatal("NewFileWAL accepted a record type it does not understand; it must refuse rather than truncate a healthy log")
	}
	if !errors.Is(err, ErrUnknownRecord) {
		t.Fatalf("error = %v, want errors.Is(err, ErrUnknownRecord)", err)
	}
}

// TestFileWAL_ImplausibleLengthInTailIsTreatedAsTail covers the shape a stale
// sector takes at the end of a log: a plausible type tag followed by a length
// prefix nothing could satisfy. It cannot be read by its own framing, but nothing
// readable follows it either, so it is still the tail of the log and still
// recoverable — refusing here would turn a crash into a manual intervention.
func TestFileWAL_ImplausibleLengthInTailIsTreatedAsTail(t *testing.T) {
	path, frames := seedLog(t)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open %s for append: %v", path, err)
	}
	// VERSION_ID tag, then a length prefix claiming 4 GiB.
	if _, err := f.Write([]byte{0x02, 0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("append stale sector: %v", err)
	}
	f.Close()

	w := mustOpenFileWAL(t, path)
	defer mustCloseFileWAL(t, w)

	want := frames[len(frames)-1].start + frames[len(frames)-1].total
	if info, err := os.Stat(path); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	} else if info.Size() != want {
		t.Fatalf("log is %d bytes after recovery, want %d (the stale sector is cut away)", info.Size(), want)
	}
}

// TestFileWAL_CorruptFirstRecordRefusesToStart is the same damage at offset zero,
// where the scan has nowhere to start looking for survivors before the damage —
// the smallest case of "the log continues past this point".
func TestFileWAL_CorruptFirstRecordRefusesToStart(t *testing.T) {
	path, frames := seedLog(t)
	flipPayloadByte(t, path, frames[0]) // BEGIN, at offset 0

	before := mustReadLog(t, path)

	w, err := NewFileWAL(path)
	if err == nil {
		w.Close()
		t.Fatal("NewFileWAL accepted a log whose first record is corrupt; it must refuse to start")
	}
	if !errors.Is(err, ErrLogCorrupt) {
		t.Fatalf("error = %v, want errors.Is(err, ErrLogCorrupt)", err)
	}
	if after := mustReadLog(t, path); !bytes.Equal(before, after) {
		t.Fatal("a failed open modified the log")
	}
}

// TestFileWAL_CompactRefusesCorruptLog pins the same rule for the rewrite path.
// Compaction copies the records it reads into a fresh file and swaps it in with
// an atomic rename, so treating damage as the end of the log there is worse than
// at startup: the original is replaced, and the records after the damage are
// gone with no copy left to recover them from.
func TestFileWAL_CompactRefusesCorruptLog(t *testing.T) {
	path, frames := seedLog(t)
	w := mustOpenFileWAL(t, path)
	defer mustCloseFileWAL(t, w)

	flipPayloadByte(t, path, frames[1])
	before := mustReadLog(t, path)

	err := w.Compact(context.Background(), map[string]int64{"kb1": 1})
	if err == nil {
		t.Fatal("Compact accepted a log with a corrupt record in the middle")
	}
	if !errors.Is(err, ErrLogCorrupt) {
		t.Fatalf("error = %v, want errors.Is(err, ErrLogCorrupt)", err)
	}
	if after := mustReadLog(t, path); !bytes.Equal(before, after) {
		t.Fatal("Compact replaced the log it could not read; the damaged original is the only copy of the records after the damage")
	}
	if _, statErr := os.Stat(path + ".compact"); !os.IsNotExist(statErr) {
		t.Errorf("Compact left %s.compact behind", path)
	}
}

// TestFileWAL_DeleteByKBRefusesCorruptLog is Compact's case for the other
// rewrite path. The knowledge base being dropped is not the damaged one on
// purpose: the reason to refuse is that the rewritten log would be missing every
// record after the damage, regardless of which knowledge base it belongs to.
func TestFileWAL_DeleteByKBRefusesCorruptLog(t *testing.T) {
	path, frames := seedLog(t)
	w := mustOpenFileWAL(t, path)
	defer mustCloseFileWAL(t, w)

	flipPayloadByte(t, path, frames[1])
	before := mustReadLog(t, path)

	err := w.DeleteByKB(context.Background(), "kb2")
	if err == nil {
		t.Fatal("DeleteByKB accepted a log with a corrupt record in the middle")
	}
	if !errors.Is(err, ErrLogCorrupt) {
		t.Fatalf("error = %v, want errors.Is(err, ErrLogCorrupt)", err)
	}
	if after := mustReadLog(t, path); !bytes.Equal(before, after) {
		t.Fatal("DeleteByKB replaced the log it could not read")
	}
	if _, statErr := os.Stat(path + ".dropkb"); !os.IsNotExist(statErr) {
		t.Errorf("DeleteByKB left %s.dropkb behind", path)
	}
}
