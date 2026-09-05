// key_codec.h — vecstore-side encoding of (kb_id, chunk_id) into the
// opaque RocksDB chunk-store key, mirroring the Go-side encoding in
// internal/chunkstore/grpc_client.go (encodeKey / encodeKBPrefix) byte for
// byte.
//
// Why this exists: the two-stage search (quantized coarse pass + exact
// rerank over full-precision vectors) resolves candidates to chunk_ids on
// the C++ side, but the full-precision vectors live in the chunk store
// under keys constructed by the Go side as kb_id + chunk_id. Search must
// rebuild those keys locally, so the encoding rule is duplicated here.
// The cross-language consistency test (test/key_codec_test.cpp) pins the
// two implementations together.
#ifndef STRATUM_VECSTORE_INCLUDE_KEY_CODEC_H_
#define STRATUM_VECSTORE_INCLUDE_KEY_CODEC_H_

#include <string>

namespace stratum {
namespace vecstore {

// EncodeKBPrefix returns the length-prefixed encoding of kb_id alone: a
// 4-byte big-endian length prefix followed by kb_id's raw bytes. Used
// both as the leading portion of EncodeKey and as an exact,
// collision-free DeleteByPrefix argument (a longer kb_id sharing the
// same prefix characters can never be matched).
std::string EncodeKBPrefix(const std::string& kb_id);

// EncodeKey returns the chunk-store key for (kb_id, chunk_id):
// EncodeKBPrefix(kb_id) followed by chunk_id's raw bytes. Byte-for-byte
// identical to Go's internal/chunkstore/grpc_client.go encodeKey.
std::string EncodeKey(const std::string& kb_id, const std::string& chunk_id);

}  // namespace vecstore
}  // namespace stratum

#endif  // STRATUM_VECSTORE_INCLUDE_KEY_CODEC_H_
