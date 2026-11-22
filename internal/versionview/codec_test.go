package versionview

import (
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"
)

func sealTestFrame(payload []byte) []byte {
	crc := crc32.Checksum(payload, castagnoliTable)
	out := make([]byte, len(payload)+codecTrailerSize)
	copy(out, payload)
	binary.LittleEndian.PutUint32(out[len(payload):], crc)
	return out
}

func TestEncodeAndDecodeViewErrorPaths(t *testing.T) {
	if _, err := encodeView(nil); err == nil {
		t.Fatal("expected error encoding nil view")
	}
	if _, err := encodeView(&View{DocIDSetHash: strings.Repeat("x", 0x10000)}); err == nil {
		t.Fatal("expected error encoding oversized digest")
	}

	valid := &View{
		DocIDSetHash: ComputeDocIDSetHash([]string{"d1", "d2"}),
		DocIDs:       []string{"d1", "d2"},
		ChunkToDocs:  map[string][]uint32{"c1": {0, 1}},
	}
	raw, err := encodeView(valid)
	if err != nil {
		t.Fatalf("encodeView: %v", err)
	}

	// Corrupt CRC.
	badCRC := append([]byte(nil), raw...)
	badCRC[len(badCRC)-1] ^= 0xff
	if _, err := decodeView(badCRC); err == nil {
		t.Fatal("expected crc mismatch error")
	}

	// File too short.
	if _, err := decodeView([]byte("SVVW")); err == nil {
		t.Fatal("expected short file error")
	}

	// Bad magic.
	badMagic := append([]byte(nil), raw...)
	badMagic[0] = 'X'
	if _, err := decodeView(badMagic); err == nil {
		t.Fatal("expected bad magic error")
	}

	// Unsupported version.
	badVer := append([]byte(nil), raw...)
	badVer[4] = 99
	if _, err := decodeView(badVer); err == nil {
		t.Fatal("expected unsupported version error")
	}

	// Valid CRC over truncated/malformed payloads.
	hdr := []byte{'S', 'V', 'V', 'W', codecVersion, 0, 0, 0}

	makeBody := func(extra ...byte) []byte {
		p := append([]byte(nil), hdr...)
		return sealTestFrame(append(p, extra...))
	}

	cases := []struct {
		name string
		data []byte
	}{
		{"empty_body", makeBody()},
		{"truncated_digest", makeBody(10, 0, 'a')},
		{"missing_doc_count", makeBody(0, 0)},
		{"implausible_doc_count", func() []byte {
			b := append([]byte(nil), hdr...)
			b = binary.LittleEndian.AppendUint16(b, 0)
			b = binary.LittleEndian.AppendUint32(b, 100)
			return sealTestFrame(b)
		}()},
		{"unsorted_docs", func() []byte {
			b := append([]byte(nil), hdr...)
			b = binary.LittleEndian.AppendUint16(b, 0)
			b = binary.LittleEndian.AppendUint32(b, 2)
			b = binary.LittleEndian.AppendUint32(b, 1)
			b = append(b, 'b')
			b = binary.LittleEndian.AppendUint32(b, 1)
			b = append(b, 'a')
			return sealTestFrame(b)
		}()},
		{"implausible_chunk_count", func() []byte {
			b := append([]byte(nil), hdr...)
			b = binary.LittleEndian.AppendUint16(b, 0)
			b = binary.LittleEndian.AppendUint32(b, 0)
			b = binary.LittleEndian.AppendUint32(b, 50)
			return sealTestFrame(b)
		}()},
		{"out_of_range_doc_index", func() []byte {
			b := append([]byte(nil), hdr...)
			b = binary.LittleEndian.AppendUint16(b, 0)
			b = binary.LittleEndian.AppendUint32(b, 1) // 1 doc
			b = binary.LittleEndian.AppendUint32(b, 1)
			b = append(b, 'a')
			b = binary.LittleEndian.AppendUint32(b, 1) // 1 chunk
			b = binary.LittleEndian.AppendUint32(b, 1)
			b = append(b, 'c')
			b = binary.LittleEndian.AppendUint32(b, 1) // 1 posting
			b = binary.LittleEndian.AppendUint32(b, 5) // idx=5 >= 1
			return sealTestFrame(b)
		}()},
		{"non_ascending_postings", func() []byte {
			b := append([]byte(nil), hdr...)
			b = binary.LittleEndian.AppendUint16(b, 0)
			b = binary.LittleEndian.AppendUint32(b, 2)
			b = binary.LittleEndian.AppendUint32(b, 1)
			b = append(b, 'a')
			b = binary.LittleEndian.AppendUint32(b, 1)
			b = append(b, 'b')
			b = binary.LittleEndian.AppendUint32(b, 1) // 1 chunk
			b = binary.LittleEndian.AppendUint32(b, 1)
			b = append(b, 'c')
			b = binary.LittleEndian.AppendUint32(b, 2) // 2 postings
			b = binary.LittleEndian.AppendUint32(b, 1)
			b = binary.LittleEndian.AppendUint32(b, 1) // duplicate idx
			return sealTestFrame(b)
		}()},
		{"trailing_bytes", func() []byte {
			b := append([]byte(nil), hdr...)
			b = binary.LittleEndian.AppendUint16(b, 0)
			b = binary.LittleEndian.AppendUint32(b, 0)
			b = binary.LittleEndian.AppendUint32(b, 0)
			b = append(b, 0xaa)
			return sealTestFrame(b)
		}()},
		{"digest_mismatch", func() []byte {
			b := append([]byte(nil), hdr...)
			b = binary.LittleEndian.AppendUint16(b, 3)
			b = append(b, "bad"...)
			b = binary.LittleEndian.AppendUint32(b, 0)
			b = binary.LittleEndian.AppendUint32(b, 0)
			return sealTestFrame(b)
		}()},
	}

	for _, tc := range cases {
		if _, err := decodeView(tc.data); err == nil {
			t.Errorf("%s: expected decodeView error", tc.name)
		}
	}
}
