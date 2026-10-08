# GRV (Golden Record Vault)

GRV moves declared datasets between external systems and a versioned Parquet
store, and keeps that store auditable. You describe a transfer in a YAML
declaration — which adapter reads or writes, which tables, which columns, what
to check — and the GRV CLI handles acquisition, staging, transactions,
retries, and publication.

- **Push** moves data from an adapter-backed source into GRV. A push
  succeeds when a new GRV revision is committed.
- **Pull** moves data from GRV into an adapter-backed destination. A pull
  succeeds when its destination operation is known to have completed.

The store is a directory of Parquet data and JSON metadata that works
identically on a local filesystem, S3, or GCS. Dataset state advances only
through explicit **revisions**, so every past state of a dataset remains
resolvable, can be **pinned** against garbage collection, and carries
provenance that is auditable from the data directory alone.

The Rust CLI and its process adapters implement the staged client v1 /
storage v2 contracts defined in [spec/](spec/README.md). Full v1 delivery is
still in progress; see [Status](#status).

## Getting started

No standalone GRV release exists yet (see [Status](#status)); until the
initial release ships, building from source is the only way to obtain the
tool, so the steps below are what every current user and developer does.

Requirements: Rust 1.89 or newer. The DuckDB adapter additionally needs
CMake and Python 3.11 or newer (native guard and bundle packaging).

### Try the store (no adapters required)

The store commands — `init`, `ls`, `show`, `pin`, `gc`, and the rest — need
only the CLI:

```console
cargo build --workspace
./target/debug/grv init --grv ./grv
./target/debug/grv ls --grv ./grv
```

Data movement (`push`/`pull`) additionally requires an installed adapter,
below.

### Move data with the DuckDB adapter

1. Build the pinned DuckDB native guard (DuckDB 1.5.6; see
   [the native build instructions](crates/grv-adapter-duckdb/native/BUILDING.txt)):

   ```console
   curl -fL https://github.com/duckdb/duckdb/archive/refs/tags/v1.5.6.tar.gz \
     --output /tmp/duckdb-1.5.6.tar.gz
   python3 crates/grv-adapter-duckdb/native/build.py \
     --archive /tmp/duckdb-1.5.6.tar.gz --build /tmp/grv-duckdb-native
   ```

2. Fetch the pinned, signed DuckDB extensions for your platform
   (`osx_arm64`, `osx_amd64`, `linux_amd64`, `linux_arm64`; see
   [the extension instructions](crates/grv-adapter-duckdb/native/EXTENSIONS.txt)):

   ```console
   python3 scripts/fetch-duckdb-extensions.py \
     --output /protected/path/extensions --platform osx_arm64
   ```

3. Package a local development bundle — the three executables, hashed adapter
   manifests, the pinned native library, the signed extensions, and an
   observed capability inventory:

   ```console
   python3 scripts/package.py \
     --native-dir /tmp/grv-duckdb-native/cmake \
     --extensions-dir /protected/path/extensions \
     --profile dev --output /protected/path/grv-bundle
   ```

4. Install the adapter and run transfers:

   ```console
   GRV_ADAPTERS_DIR=/protected/path/grv-bundle/adapters \
     /protected/path/grv-bundle/bin/grv adapter install /protected/path/grv-bundle/adapters/duckdb
   /protected/path/grv-bundle/bin/grv init --grv /protected/path/store
   /protected/path/grv-bundle/bin/grv push --grv /protected/path/store \
     --decl spec/examples/duckdb-build-push.yml
   ```

The bundle records `complete_client_v1: false`, and its DuckDB adapter loads
the packaged guard library beside the executable. Adapter installation and
discovery require [protected directories](#concepts) — in the commands above,
`/protected/path` must be a path whose ancestors are all owned by you (or
root), not group- or other-writable, and free of symlinks.

A declaration looks like this (more runnable examples live in
[spec/examples/](spec/examples/); relative SQL and column-file references
resolve from the declaration file):

```yaml
declaration_version: 1
kind: push
dataset: month_totals
adapter: duckdb
connection:
  database: ../../workspace.duckdb
build:
  inputs:
    - { table: raw.mt_month_stats, as: month_stats }
tables:
  - name: totals
    source:
      sql: |
        SELECT period, SUM(spend)::DECIMAL(38,4) AS total_spend, _period_
        FROM grv_input.month_stats
        GROUP BY period, _period_
    columns:
      - { name: period, type: date32 }
      - { name: total_spend, type: "decimal128(38,4)" }
      - { name: _period_, type: utf8 }
    partition_keys:
      - period
checks:
  - { table: totals, not_null: [period, total_spend, _period_] }
```

## Concepts

The full definitions live in the [specification guide](spec/README.md); these
are the terms you will meet most often.

- **dataset** — a collection of **tables**; the unit of revision. A table is
  versioned and may be partitioned; multiple versions coexist and nothing is
  overwritten in place.
- **revision** — a complete, named snapshot of a dataset's state (which
  tables are present, at which version). Revisions advance only through an
  explicit publish step; concurrent publications that change the same
  (table, partition) are detected, never silently overwritten.
- **run** — one execution that produced a version. Which run produced a
  version, and which revisions of other datasets it was derived from, is
  auditable from the data directory alone.
- **pin / hold** — explicit retention protection on a revision. Merely
  choosing a revision does not protect it; `gc` never collects pinned or
  held versions.
- **attempt** — one transfer request, identified by a UUID. Retrying with the
  same UUID resumes or replays that request; it never starts different work.
- **capture** — the staged Parquet output of a complete extraction, sealed by
  a receipt (schemas, counts, hashes, source identity).
- **receipt** — the immutable record of a successful pull (or accepted
  capture), retained by the destination.
- **adapter** — a separate process that knows how to talk to one kind of
  external system (DuckDB, Salesforce, …). The CLI supervises adapters over a
  local process channel; adapters report only the lifecycles whose
  conformance gates pass.
- **protected directory** — a path whose every ancestor is owned by the
  current user or root, is not group- or other-writable, and contains no
  symlinks. Adapter installation, adapter discovery, and bundle packaging
  require protected ancestors.

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

The CLI and adapters are separate processes: adapters send Arrow data to the
host, and the CLI owns GRV publication. Local, S3, and GCS storage share the
same conditional operations and outcome proofs.

| Crate                                                      | Role                                                                |
| ---------------------------------------------------------- | ------------------------------------------------------------------- |
| [`grv`](crates/grv/)                                       | The CLI: commands, orchestration, and the JSON result envelope      |
| [`grv-core`](crates/grv-core/)                             | Core: journaling, locks, capture, publication, builds, recovery     |
| [`grv-storage`](crates/grv-storage/)                       | The storage v2 layout on local, S3, and GCS backends                |
| [`grv-types`](crates/grv-types/)                           | Shared canonical/logical types and result shapes                    |
| [`grv-adapter-api`](crates/grv-adapter-api/)               | The adapter interface shared by host and adapters                   |
| [`grv-adapter-wire`](crates/grv-adapter-wire/)             | Frame grammar for the adapter process channel                       |
| [`grv-adapter-host`](crates/grv-adapter-host/)             | Host side: discovery, installation, process supervision, data plane |
| [`grv-adapter-sdk`](crates/grv-adapter-sdk/)               | SDK for writing new adapters                                        |
| [`grv-adapter-duckdb`](crates/grv-adapter-duckdb/)         | DuckDB adapter and its pinned native guard                          |
| [`grv-adapter-salesforce`](crates/grv-adapter-salesforce/) | Salesforce adapter (push only)                                      |
| [`grv-conformance`](crates/grv-conformance/)               | Conformance harness and named lifecycle gates                       |

The normative contracts are in [spec/](spec/README.md): the storage layout,
the client surface, execution semantics, and the adapter process protocol.

## Usage

```console
grv init --grv <root>
grv push --decl <yaml> --grv <root>
grv pull --decl <yaml> --grv <root> [--revision <N|latest>]
grv ls --grv <root> [<dataset>] [--table <table>] [--revision N]
grv show <dataset> --grv <root> [--revision N] [--retention]
grv status <dataset> --grv <root> [--decl <yaml>]
grv log <dataset> --grv <root> [--limit N]
grv diff <dataset> --grv <root> --from N --to <N|latest>
grv verify <dataset> --grv <root> [--revision N] [--full]
grv pin <dataset> --grv <root> --revision N --reason <text>
grv unpin <dataset> --grv <root> --revision N --pin <id>
grv gc <dataset> --grv <root> [--dry-run | --apply]
grv recover <dataset> --grv <root> [--run <run-id>] [--dry-run]
grv adapter list
grv adapter install <path|tarball> [--replace]
grv adapter <name> capabilities
```

- `--grv` selects one root: a local path, `s3://bucket/prefix`, or
  `gs://bucket/prefix`.
- `--decl` selects one YAML declaration; connection, SQL, and column-file
  paths inside it resolve from the declaration file.
- `--revision` overrides the declaration's pull selector (source state only).
- `--json` is supported by every command: progress goes to stderr, stdout
  carries the versioned result envelope
  ([schema](spec/grv-client-v1-command-output.schema.json)).
- Exit statuses: `0` success; `2` invalid request; `3` conflict, busy, or
  ownership loss; `4` unavailable or not found; `5` integrity or protocol
  failure; `6` adapter, backend, or engine failure, or unknown outcome.

Read-only commands never establish bindings, create pins, renew leases, or
repair state. The full command and result contracts, including retry and
recovery semantics, are defined in the
[client v1 spec](spec/grv-client-v1.md) and its
[execution companion](spec/grv-client-v1-execution.md).

## Storage backends

| Backend          | Root scheme          | Notes                                                                       |
| ---------------- | -------------------- | --------------------------------------------------------------------------- |
| Local filesystem | a path               | The layout's reference backend                                              |
| S3               | `s3://bucket/prefix` | Multipart streaming by default; a limited single-put mode exists (below)    |
| GCS              | `gs://bucket/prefix` | Full-object reads; GCS views are not yet advertised (see [Status](#status)) |

All three backends share the same layout, conditional operations, and
outcome proofs — there are no backend-specific layout variants.

S3 writers that only have `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`,
and `s3:DeleteObject` can use the limited **single-put** mode
(`GRV_S3_UPLOAD_MODE=single-put`): one atomic conditional PUT per object, no
multipart APIs, with a per-object in-memory buffer that defaults to 1 GiB.
See [docs/cloud-storage.md](docs/cloud-storage.md) for the buffer limit,
memory guidance, and the cloud read-timeout setting.

## Adapters

| Adapter           | Push       | Pull       | Initial scope                                                                         |
| ----------------- | ---------- | ---------- | ------------------------------------------------------------------------------------- |
| `duckdb`          | Yes        | Yes        | Local snapshot extraction; managed SQL builds; transactional replace and append pulls |
| `salesforce`      | Yes        | No         | Filtered objects with mapped columns; full extractions                                |
| Installed adapter | Capability | Capability | Its registered source or destination binding and commands                             |

The **DuckDB** adapter runs against the pinned native guard (DuckDB 1.5.6)
and advertises snapshot extraction, transactional local and S3-view pulls,
managed and external builds, and read-only inspection after their
conformance gates. Its signed `httpfs`/`aws` extension pair is fetched with
`scripts/fetch-duckdb-extensions.py`; no network install or extension
auto-loading happens in a packaged adapter.

The **Salesforce** adapter uses existing local CLI authentication, API v66.0,
and REST-first `auto` transport. See
[its helper and credential requirements](crates/grv-adapter-salesforce/README.md);
credentials stay in local configuration and private helper pipes.

## Environment variables

User-facing settings, read by the CLI process that owns the operation:

| Variable                                           | Purpose                                                                        |
| -------------------------------------------------- | ------------------------------------------------------------------------------ |
| `GRV_ADAPTERS_DIR`                                 | Adapter discovery root; overrides the default user-config location             |
| `GRV_S3_UPLOAD_MODE`                               | `single-put` enables the limited-permission S3 upload mode                     |
| `GRV_S3_SINGLE_PUT_MAX_BYTES`                      | Per-object buffer limit for single-put mode (default 1 GiB; max 5,000,000,000) |
| `GRV_CLOUD_READ_TIMEOUT_SECONDS`                   | Cloud download total timeout, 1–3600 (default 600)                             |
| `GRV_GCS_ACCOUNT`, `GRV_GCS_PROJECT`               | Explicit GCS account/project                                                   |
| `AWS_PROFILE`, `AWS_REGION` / `AWS_DEFAULT_REGION` | Standard AWS environment for S3                                                |

Build-time variables: `GRV_DUCKDB_NATIVE_LIB_DIR` (location of the built
native guard) and `GRV_DUCKDB_BUNDLE_ONLY_RPATH` (bundle-only rpath).
`GRV_DUCKDB_EXTENSIONS_DIR` and `GRV_DUCKDB_WORKSPACE_LOCK_FD` are set by
the CLI host for adapter child processes, not by users. The remaining
`GRV_*` variables in the source are test-only fixtures.

## Development

This section is for people working on the codebase. To _use_ GRV, the
[Getting started](#getting-started) and [Usage](#usage) sections are all you
need.

- **Toolchain.** All workspace targets compile with Rust 1.89 (the workspace
  MSRV). CMake and Python 3.11 or newer are needed only for the DuckDB
  native guard. Runtime conformance evidence currently runs with Rust 1.99 on
  macOS ARM64 (see [Status](#status)).
- **Pinned dependencies.** Arrow and Parquet are pinned to 58.3.0; DuckDB to
  1.5.6. An upstream change requires a reviewed guard revision.
- **Tests.**

  ```console
  cargo test --workspace --offline
  GRV_DUCKDB_NATIVE_LIB_DIR=/path/to/native/cmake cargo test -p grv-adapter-duckdb --features native --offline
  GRV_DUCKDB_NATIVE_LIB_DIR=/path/to/native/cmake cargo test -p grv-conformance --features native-duckdb --offline
  ```

  Run subprocess conformance builds serially within one Cargo target
  directory: a concurrent build can replace a binary between subprocess
  invocations. Separate target directories support independent test runs.

- **Native guard.** Build instructions and the guard's guarantees are in
  [BUILDING.txt](crates/grv-adapter-duckdb/native/BUILDING.txt); signed
  extension fetching is in
  [EXTENSIONS.txt](crates/grv-adapter-duckdb/native/EXTENSIONS.txt). The
  extensions root contains one subdirectory per fetched platform.
- **Bundles.** `scripts/package.py` assembles a relocatable bundle
  (executables, hashed adapter manifests, native library, signed extensions,
  notices, capability inventory) and `scripts/verify-bundle.py` verifies a
  relocated copy. The [CI workflow](.github/workflows/conformance.yml) runs
  the native tests, strict lint, and bundle relocation with Rust 1.89 on
  Linux and macOS, each on x86-64 and ARM64.

## Status

Full v1 delivery is in progress; the bundle records
`complete_client_v1: false`, and adapter capabilities report only lifecycles
whose conformance gates pass.

- The [first release validation plan](spec/grv-v1-release-validation.md)
  defines the full acceptance matrix. The
  [validation operations index](spec/release-validation/README.md) tracks
  the scoped initial macOS ARM64 gates, current status, the reusable test
  catalog, immutable evidence references, and the change-based rerun policy.
  Development passes do not qualify later candidate binaries.
- Current target: a macOS ARM64 0.1.0 initial scoped release; macOS x86-64
  and Linux x86-64/ARM64 are future qualification cells. Those platform runs
  remain release gates until their execution evidence is available. Initial
  scope and notice policy are recorded in
  [0.1.0-SCOPE.md](spec/release-validation/0.1.0-SCOPE.md).
- Local and production GCS lifecycles passed on the previous clean
  candidate. GCS views are not advertised: verified files materialize
  locally.
- Production S3 multipart and single-PUT lifecycles passed on follow-up
  bytes; the configurable 1 GiB default and the final candidate still require
  qualification. S3 fixed-file views and external builds are distinct named
  gates, not implied by publication or local-pull evidence.
- Source acquisition is initially nonresumable; accepted captures and
  terminal outcomes can be retried without contacting their source.
- The current macOS ARM64 native library requires macOS 26.0 or newer
  (tested host 26.7.1); do not infer macOS 11 support from the CLI or
  extension minimum alone.

## License

[Apache License 2.0](LICENSE).
