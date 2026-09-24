#include "vecstore/include/key_codec.h"

#include <cstddef>

namespace stratum {
namespace vecstore {

namespace {

// BigEndianU32Bytes appends the 4 big-endian bytes of n to out. Mirrors
// the Go-side hand-rolled encoding in
// internal/chunkstore/grpc_client.go encodeKBPrefix.
void AppendBigEndianU32(std::string* out, size_t n) {
  out->push_back(static_cast<char>((n >> 24) & 0xFF));
  out->push_back(static_cast<char>((n >> 16) & 0xFF));
  out->push_back(static_cast<char>((n >> 8) & 0xFF));
  out->push_back(static_cast<char>(n & 0xFF));
}

}  // namespace

std::string EncodeKBPrefix(const std::string& kb_id) {
  std::string out;
  out.reserve(4 + kb_id.size());
  AppendBigEndianU32(&out, kb_id.size());
  out.append(kb_id);
  return out;
}

std::string EncodeKey(const std::string& kb_id, const std::string& chunk_id) {
  return EncodeKBPrefix(kb_id) + chunk_id;
}

bool IsKBPrefix(const std::string& prefix) {
  // Too short to hold the 4-byte length prefix, let alone a kb_id: nothing
  // is named, so the scan would run unqualified.
  if (prefix.size() <= 4) {
    return false;
  }
  const size_t declared =
      (static_cast<size_t>(static_cast<unsigned char>(prefix[0])) << 24) |
      (static_cast<size_t>(static_cast<unsigned char>(prefix[1])) << 16) |
      (static_cast<size_t>(static_cast<unsigned char>(prefix[2])) << 8) |
      static_cast<size_t>(static_cast<unsigned char>(prefix[3]));
  // The declared length must account for the rest of the prefix exactly. A
  // shorter one would leave the upper bound computed from a truncated kb_id
  // (so the scan does not stop at this knowledge base's keys), and a longer
  // one is a prefix the Go side never writes.
  return declared == prefix.size() - 4;
}

}  // namespace vecstore
}  // namespace stratum
