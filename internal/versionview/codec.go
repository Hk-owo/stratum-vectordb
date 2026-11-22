package versionview

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	codecMagic             = "SVVW"
	codecVersion     uint8 = 1
	codecHeaderSize        = 8
	codecTrailerSize       = 4
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// encodeView serializes v into the .vview binary format:
//
//	Magic("SVVW") | Version(1) | Flags(0) | Reserved(0) |
//	DocIDSetHash | DocCount + sorted DocIDs | ChunkCount + (ChunkID, []uint32) |
//	CRC-32C (Castagnoli)
func encodeView(v *View) ([]byte, error) {
	if v == nil {
		return nil, fmt.Errorf("versionview: encode nil view")
	}
	if len(v.DocIDSetHash) > 0xffff {
		return nil, fmt.Errorf("versionview: digest too long (%d bytes)", len(v.DocIDSetHash))
	}

	size := codecHeaderSize + 2 + len(v.DocIDSetHash) + 4 + 4 + codecTrailerSize
	for _, docID := range v.DocIDs {
		size += 4 + len(docID)
	}
	for chunkID, docs := range v.ChunkToDocs {
		size += 4 + len(chunkID) + 4 + 4*len(docs)
	}

	buf := make([]byte, 0, size)
	buf = append(buf, codecMagic...)
	buf = append(buf, codecVersion, 0, 0, 0)

	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(v.DocIDSetHash)))
	buf = append(buf, v.DocIDSetHash...)

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(v.DocIDs)))
	for _, docID := range v.DocIDs {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(docID)))
		buf = append(buf, docID...)
	}

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(v.ChunkToDocs)))
	for chunkID, docs := range v.ChunkToDocs {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(chunkID)))
		buf = append(buf, chunkID...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(docs)))
		for _, idx := range docs {
			buf = binary.LittleEndian.AppendUint32(buf, idx)
		}
	}

	crc := crc32.Checksum(buf, castagnoliTable)
	buf = binary.LittleEndian.AppendUint32(buf, crc)
	return buf, nil
}

func decodeView(data []byte) (*View, error) {
	if len(data) < codecHeaderSize+codecTrailerSize {
		return nil, fmt.Errorf("versionview: corrupt vview: file too short (%d bytes)", len(data))
	}
	if string(data[:4]) != codecMagic {
		return nil, fmt.Errorf("versionview: corrupt vview: invalid magic %q", data[:4])
	}
	if data[4] != codecVersion {
		return nil, fmt.Errorf("versionview: unsupported vview version %d", data[4])
	}

	payloadLen := len(data) - codecTrailerSize
	wantCRC := binary.LittleEndian.Uint32(data[payloadLen:])
	gotCRC := crc32.Checksum(data[:payloadLen], castagnoliTable)
	if wantCRC != gotCRC {
		return nil, fmt.Errorf("versionview: corrupt vview: crc32c mismatch (got %#x, want %#x)", gotCRC, wantCRC)
	}

	p := data[codecHeaderSize:payloadLen]
	readU16 := func() (uint16, error) {
		if len(p) < 2 {
			return 0, fmt.Errorf("versionview: corrupt vview: truncated u16")
		}
		v := binary.LittleEndian.Uint16(p[:2])
		p = p[2:]
		return v, nil
	}
	readU32 := func() (uint32, error) {
		if len(p) < 4 {
			return 0, fmt.Errorf("versionview: corrupt vview: truncated u32")
		}
		v := binary.LittleEndian.Uint32(p[:4])
		p = p[4:]
		return v, nil
	}
	readStr := func(n uint32) (string, error) {
		if uint64(n) > uint64(len(p)) {
			return "", fmt.Errorf("versionview: corrupt vview: string length %d exceeds remaining %d bytes", n, len(p))
		}
		s := string(p[:n])
		p = p[n:]
		return s, nil
	}

	hashLen, err := readU16()
	if err != nil {
		return nil, err
	}
	digest, err := readStr(uint32(hashLen))
	if err != nil {
		return nil, err
	}

	docCount, err := readU32()
	if err != nil {
		return nil, err
	}
	if uint64(docCount)*4 > uint64(len(p)) {
		return nil, fmt.Errorf("versionview: corrupt vview: implausible doc count %d", docCount)
	}
	docIDs := make([]string, docCount)
	for i := uint32(0); i < docCount; i++ {
		n, err := readU32()
		if err != nil {
			return nil, err
		}
		id, err := readStr(n)
		if err != nil {
			return nil, err
		}
		if i > 0 && id < docIDs[i-1] {
			return nil, fmt.Errorf("versionview: corrupt vview: docIDs not sorted")
		}
		docIDs[i] = id
	}

	chunkCount, err := readU32()
	if err != nil {
		return nil, err
	}
	if uint64(chunkCount)*8 > uint64(len(p)) {
		return nil, fmt.Errorf("versionview: corrupt vview: implausible chunk count %d", chunkCount)
	}
	chunkToDocs := make(map[string][]uint32, chunkCount)
	var mem int64 = 64
	for _, id := range docIDs {
		mem += int64(len(id) + 16)
	}
	for i := uint32(0); i < chunkCount; i++ {
		cn, err := readU32()
		if err != nil {
			return nil, err
		}
		chunkID, err := readStr(cn)
		if err != nil {
			return nil, err
		}
		pn, err := readU32()
		if err != nil {
			return nil, err
		}
		if uint64(pn)*4 > uint64(len(p)) {
			return nil, fmt.Errorf("versionview: corrupt vview: implausible posting count %d", pn)
		}
		docs := make([]uint32, pn)
		for j := uint32(0); j < pn; j++ {
			idx, err := readU32()
			if err != nil {
				return nil, err
			}
			if idx >= docCount {
				return nil, fmt.Errorf("versionview: corrupt vview: doc index %d out of range (%d)", idx, docCount)
			}
			if j > 0 && idx <= docs[j-1] {
				return nil, fmt.Errorf("versionview: corrupt vview: posting list not strictly ascending")
			}
			docs[j] = idx
		}
		chunkToDocs[chunkID] = docs
		mem += int64(len(chunkID) + 32 + 4*len(docs))
	}

	if len(p) != 0 {
		return nil, fmt.Errorf("versionview: corrupt vview: %d trailing bytes", len(p))
	}
	if ComputeDocIDSetHash(docIDs) != digest {
		return nil, fmt.Errorf("versionview: corrupt vview: internal digest mismatch")
	}

	return &View{
		DocIDSetHash: digest,
		DocIDs:       docIDs,
		ChunkToDocs:  chunkToDocs,
		MemBytes:     mem,
	}, nil
}
