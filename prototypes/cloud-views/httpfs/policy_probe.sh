#!/usr/bin/env bash
# Offline only; signed artifact verified by Python before native LOAD.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
native="${GRV_NATIVE_DIR:-/tmp/grv-duckdb-native}"
source="${GRV_DUCKDB_SOURCE:-$native/duckdb-1.5.6}"
httpfs="${GRV_HTTPFS_SOURCE:-/tmp/grv-httpfs-source/duckdb-httpfs-4bc690dba4496c765777a0269d48fdbaff7cdc11}"
work="$(mktemp -d /tmp/grv-gcs-policy-XXXXXX)"
trap 'rm -rf "$work"' EXIT
"${CXX:-c++}" -std=c++17 -O1 -Wall -Wextra -isystem "$source/src/include" -isystem "$httpfs/src/include" \
  "$here/policy_probe.cpp" "$native/cmake/libgrv_duckdb_guard.dylib" -Wl,-rpath,"$native/cmake" -o "$work/probe"
"${PYTHON:-python3.13}" "$here/policy_probe.py" --binary "$work/probe"
