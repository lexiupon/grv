# Developing GRV

This guide is for people working on the GRV codebase. To _use_ GRV, see the
[README](../README.md); to build a bundle from source, see
[building.md](building.md).

## Architecture

```text
 source system (DuckDB, Salesforce, …)
        │  ▲
        ▼  │        push: source → GRV          pull: GRV → destination
  adapter process (separate process, registered capabilities)
        │  ▲
        ▼  │
  grv CLI — commands, orchestration, transactions, publication
        │
        ▼
  GRV store — local filesystem, S3, or GCS (one layout for all)
```

The CLI and adapters run as separate processes. Adapters send Arrow data to
the host, and the CLI owns publication to the GRV store. Local, S3 and GCS
storage share the same conditional operations and outcome proofs.

| Crate                                                         | Role                                                                |
| ------------------------------------------------------------- | ------------------------------------------------------------------- |
| [`grv`](../crates/grv/)                                       | The CLI: commands, orchestration, the JSON result envelope, skills  |
| [`grv-core`](../crates/grv-core/)                             | Core: journaling, locks, capture, publication, builds, recovery     |
| [`grv-storage`](../crates/grv-storage/)                       | The storage v2 layout on local, S3 and GCS backends                 |
| [`grv-types`](../crates/grv-types/)                           | Shared canonical/logical types and result shapes                    |
| [`grv-adapter-api`](../crates/grv-adapter-api/)               | The adapter interface shared by host and adapters                   |
| [`grv-adapter-wire`](../crates/grv-adapter-wire/)             | Frame grammar for the adapter process channel                       |
| [`grv-adapter-host`](../crates/grv-adapter-host/)             | Host side: discovery, installation, process supervision, data plane |
| [`grv-adapter-sdk`](../crates/grv-adapter-sdk/)               | SDK for writing new adapters                                        |
| [`grv-adapter-duckdb`](../crates/grv-adapter-duckdb/)         | DuckDB adapter and its pinned native guard                          |
| [`grv-adapter-salesforce`](../crates/grv-adapter-salesforce/) | Salesforce adapter (push only)                                      |
| [`grv-conformance`](../crates/grv-conformance/)               | Conformance harness and named lifecycle gates                       |

The normative contracts are in [spec/](../spec/README.md): the storage
layout, the client surface, execution semantics and the adapter process
protocol.

## Toolchain

All workspace targets compile with Rust 1.89 (the workspace MSRV). CMake and
Python 3.11 or newer are needed only for the DuckDB native guard. Runtime
conformance evidence currently runs with Rust 1.99 on macOS ARM64.

Arrow and Parquet are pinned to 58.3.0 and DuckDB to 1.5.6. Changing an
upstream version requires a reviewed guard revision.

## Tests

```console
cargo test --workspace --offline
GRV_DUCKDB_NATIVE_LIB_DIR=/path/to/native/cmake cargo test -p grv-adapter-duckdb --features native --offline
GRV_DUCKDB_NATIVE_LIB_DIR=/path/to/native/cmake cargo test -p grv-conformance --features native-duckdb --offline
```

Run subprocess conformance builds one at a time within a Cargo target
directory, because a concurrent build can replace a binary between subprocess
invocations. Separate target directories can run independently.

## Native guard and bundles

- Native guard build instructions and guarantees:
  [BUILDING.txt](../crates/grv-adapter-duckdb/native/BUILDING.txt).
  Signed extension fetching:
  [EXTENSIONS.txt](../crates/grv-adapter-duckdb/native/EXTENSIONS.txt).
  The extensions root contains one subdirectory per fetched platform.
- `scripts/package.py` builds a relocatable bundle (executables, hashed
  adapter manifests, native library, signed extensions, notices, capability
  inventory). `scripts/verify-bundle.py` checks a relocated copy.
- The [CI workflow](../.github/workflows/conformance.yml) runs the native
  tests, strict lint and bundle relocation with Rust 1.89 on Linux and macOS,
  on x86-64 and ARM64.

## Agent skills

Skills that teach AI coding agents to use GRV live in [skills/](../skills/).
Each one is a directory with a `SKILL.md` and optional `references/`. They are
compiled into the `grv` binary and installed with `grv skills install`. When
you change CLI behaviour, update the matching skill in the same change; the
`grv` crate tests check that every bundled skill has valid frontmatter.

## Build-time and internal environment variables

- `GRV_DUCKDB_NATIVE_LIB_DIR`: location of the built native guard.
- `GRV_DUCKDB_BUNDLE_ONLY_RPATH`: bundle-only rpath.
- `GRV_DUCKDB_EXTENSIONS_DIR` and `GRV_DUCKDB_WORKSPACE_LOCK_FD` are set by
  the CLI host for adapter child processes, not by users.
- Other `GRV_*` variables in the source are test-only fixtures.

## Releases

Release validation, gates and evidence handling are described in
[spec/release-validation/](../spec/release-validation/README.md). Publishing
always needs separate approval.
