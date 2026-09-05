// key_codec_test.cpp — pins the C++ (kb_id, chunk_id) → chunk-store key
// encoding to the Go-side rule in internal/chunkstore/grpc_client.go
// (encodeKey / encodeKBPrefix), so the two-stage search's rerank reads
// the exact keys the Go write path used. Expected bytes below are derived
// by hand from that rule: a 4-byte big-endian kb_id length prefix followed
// by kb_id's raw bytes, then chunk_id's raw bytes.
#include "vecstore/include/key_codec.h"

#include <string>

#include "gtest/gtest.h"

namespace stratum {
namespace vecstore {
namespace {

TEST(KeyCodecTest, EncodeKeyMatchesGoRule) {
  // kb_id "kb1" (length 3): 00 00 00 03 'k' 'b' '1', then chunk_id "c1"
  // (2 bytes): 9 bytes total.
  EXPECT_EQ(EncodeKey("kb1", "c1"), std::string("\x00\x00\x00\x03kb1c1", 9));
}

TEST(KeyCodecTest, EncodeKeyWithTypicalChunkId) {
  // kb_id "kb-abc" (length 6): 00 00 00 06 'k''b''-''a''b''c' (10 bytes)
  // + chunk_id "chunk-0" (7 bytes): 17 bytes total.
  EXPECT_EQ(EncodeKey("kb-abc", "chunk-0"),
            std::string("\x00\x00\x00\x06kb-abcchunk-0", 17));
}

TEST(KeyCodecTest, EncodeKBPrefixIsKeyPrefixAndLengthPrefixed) {
  const std::string prefix = EncodeKBPrefix("kb1");
  EXPECT_EQ(prefix, std::string("\x00\x00\x00\x03kb1", 7));
  // EncodeKey(kb, chunk) must start with EncodeKBPrefix(kb) and end with
  // the raw chunk_id.
  const std::string key = EncodeKey("kb1", "c1");
  EXPECT_EQ(key.substr(0, prefix.size()), prefix);
  EXPECT_EQ(key.substr(prefix.size()), std::string("c1"));
}

TEST(KeyCodecTest, EncodeKeyPrefixPreventsPrefixCollision) {
  // DeleteByPrefix(encodeKBPrefix("kb")) must never match a different,
  // longer kb_id that happens to start with the same characters: the
  // 4-byte length prefix differs ("kb" → length 2, "kb-longer" → 9).
  EXPECT_EQ(EncodeKBPrefix("kb"), std::string("\x00\x00\x00\x02kb", 6));
  EXPECT_EQ(EncodeKBPrefix("kb-longer").substr(0, 4),
            std::string("\x00\x00\x00\x09", 4));
  EXPECT_NE(EncodeKey("kb-longer", "1").substr(0, 7),
            std::string("\x00\x00\x00\x02kb", 6));
}

}  // namespace
}  // namespace vecstore
}  // namespace stratum
