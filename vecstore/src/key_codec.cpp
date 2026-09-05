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

}  // namespace vecstore
}  // namespace stratum
