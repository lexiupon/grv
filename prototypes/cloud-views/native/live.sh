#!/usr/bin/env bash
# Opt-in experiment: existing pinned native build only; no credential discovery.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
native="${GRV_NATIVE_DIR:-/tmp/grv-duckdb-native}"
source="${GRV_DUCKDB_SOURCE:-$native/duckdb-1.5.6}"
build="${GRV_NATIVE_BUILD:-$native/cmake}"
lib="$build/libgrv_duckdb_guard.dylib"
[[ -f "$lib" && -f "$source/src/include/duckdb.hpp" ]] || {
  printf 'Missing existing native build/headers: %s / %s\n' "$lib" "$source" >&2; exit 1;
}
work="$(mktemp -d /tmp/grv-native-live-build-XXXXXX)"
trap 'rm -rf "$work"' EXIT
"${CXX:-c++}" -std=c++17 -O1 -Wall -Wextra -Wno-unused-function \
  -isystem "$source/src/include" -isystem "$source/third_party/parquet" -isystem "$source/third_party/thrift" \
  "$here/live.cpp" "$lib" -lcurl -Wl,-rpath,"$build" -o "$work/live"
args=("$@")
if [[ "${1:-}" == scan ]]; then
  for ((i=1; i<${#args[@]}; i++)); do
    if [[ "${args[i]}" == --manifest ]]; then
      [[ $((i+1)) -lt ${#args[@]} ]] || { printf 'Missing manifest\n' >&2; exit 1; }
      python3 "$here/manifest.py" "${args[i+1]}" "$work/manifest.tsv"
      args[i+1]="$work/manifest.tsv"
      break
    fi
  done
fi
"$work/live" "${args[@]}"
