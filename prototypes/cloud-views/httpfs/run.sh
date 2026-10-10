#!/usr/bin/env bash
# No downloads/install: verify the pinned artifact before native LOAD.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
native="${GRV_NATIVE_DIR:-/tmp/grv-duckdb-native}"
source="${GRV_DUCKDB_SOURCE:-$native/duckdb-1.5.6}"
lib="$native/cmake/libgrv_duckdb_guard.dylib"
# Must match the signed artifact's pinned source; never fetch or build an extension.
httpfs_source="${GRV_HTTPFS_SOURCE:-/tmp/grv-httpfs-source/duckdb-httpfs-4bc690dba4496c765777a0269d48fdbaff7cdc11}"
[[ -f "$httpfs_source/src/include/http_state.hpp" ]] || { printf 'Missing pinned HTTPFS headers\n' >&2; exit 1; }
# Internal counter layout is version-specific, not a supported DuckDB public API.
"${PYTHON:-python3.13}" - "$httpfs_source/src/include/http_state.hpp" <<'PY'
import hashlib, pathlib, sys
expected = '3c4f0c6a7bc43d26b519fad356b489eb1a03e1a57cd5788e46854fb0e4c80146'
if hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest() != expected:
    raise SystemExit('HTTPFS metrics header does not match pinned source')
PY
[[ -f "$lib" && -f "$source/src/include/duckdb.hpp" ]] || { printf 'Missing pinned native build\n' >&2; exit 1; }
work="$(mktemp -d /tmp/grv-httpfs-build-XXXXXX)"
trap 'rm -rf "$work"' EXIT
"${CXX:-c++}" -std=c++17 -O1 -Wall -Wextra -isystem "$source/src/include" -isystem "$httpfs_source/src/include" \
  "$here/harness.cpp" "$lib" -Wl,-rpath,"$native/cmake" -o "$work/harness"
"${PYTHON:-python3.13}" "$here/offline.py" --binary "$work/harness" "$@"
