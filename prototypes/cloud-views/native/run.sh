#!/usr/bin/env bash
# Reuse the existing patched native build; never download or rebuild DuckDB.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
native="${GRV_NATIVE_DIR:-/tmp/grv-duckdb-native}"
source="${GRV_DUCKDB_SOURCE:-$native/duckdb-1.5.6}"
build="${GRV_NATIVE_BUILD:-$native/cmake}"
lib="$build/libgrv_duckdb_guard.dylib"
[[ -f "$lib" && -f "$source/src/include/duckdb.hpp" ]] || {
  printf 'Missing existing native build/headers: %s / %s\n' "$lib" "$source" >&2; exit 1;
}
work="$(mktemp -d /tmp/grv-native-prototype-build-XXXXXX)"
trap 'rm -rf "$work"' EXIT
"${CXX:-c++}" -std=c++17 -O1 -Wall -Wextra \
  -isystem "$source/src/include" -isystem "$source/third_party/parquet" -isystem "$source/third_party/thrift" \
  "$here/prototype.cpp" "$lib" -Wl,-rpath,"$build" -o "$work/prototype"
"$work/prototype" "$@"
