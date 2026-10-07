#pragma once
#include <cstddef>

// Independently bounded engine-owned exact-file metadata; transfer windows and
// TLS scratch are admitted separately. Include the transient scan allowlists,
// filename aliases and retained file-handle paths, not just S3Object fields.
namespace grv_s3_limits {
constexpr std::size_t METADATA_BYTES = 2 * 1024 * 1024;
constexpr std::size_t MAX_OBJECTS = 4096;
constexpr std::size_t MAX_URI_BYTES = 8192;
constexpr std::size_t MAX_VALIDATOR_BYTES = 1024;
constexpr std::size_t MAX_VERSION_BYTES = 4096;
inline bool Admit(std::size_t used, std::size_t objects, std::size_t uri_bytes,
                  std::size_t validator_bytes, std::size_t &cost) {
    // At most twelve live coordinate/path copies, each charged twice its byte
    // length for string capacity. Four validator capacities cover fixed and
    // transient copies. The fixed allowance covers the maximum object version,
    // actual ETag, SHA, S3Object/control blocks, map/set nodes and hash buckets.
    if (used > METADATA_BYTES || objects >= MAX_OBJECTS || uri_bytes == 0 ||
        uri_bytes > MAX_URI_BYTES || validator_bytes == 0 ||
        validator_bytes > MAX_VALIDATOR_BYTES) { return false; }
    cost = 24 * uri_bytes + 4 * validator_bytes + 8192;
    return cost <= METADATA_BYTES - used;
}
}
