# GRV Rust client

This workspace implements the staged client-v1/storage-v2 contracts in [spec](spec/README.md). Full v1 delivery is still in progress. Adapter capabilities report only lifecycles whose conformance gates pass.

The CLI and adapters are separate processes. Local, S3 and GCS storage share conditional operations and outcome proofs. Source acquisition is initially nonresumable; accepted captures and terminal outcomes can be retried without contacting their source.

Use Rust 1.89 or newer with Cargo, CMake and Python 3.11 or newer. All workspace targets compile with Rust 1.89; runtime conformance currently runs with Rust 1.99 on macOS ARM64. The pinned DuckDB native guard must be built before enabling the native adapter; follow [the native build instructions](crates/grv-adapter-duckdb/native/BUILDING.txt). Arrow and Parquet are pinned to 58.3.0 and DuckDB to 1.5.6.

```console
cargo test --workspace --offline
GRV_DUCKDB_NATIVE_LIB_DIR=/path/to/native/cmake cargo test -p grv-adapter-duckdb --features native --offline
GRV_DUCKDB_NATIVE_LIB_DIR=/path/to/native/cmake cargo test -p grv-conformance --features native-duckdb --offline
```

Run subprocess conformance builds serially within one Cargo target directory. A concurrent build can replace a binary between subprocess invocations. Separate target directories support independent test runs.

Create a local development bundle with the three executables, hashed adapter manifests, the pinned native library, signed S3 extensions and an observed capability inventory:

```console
python3 scripts/package.py --native-dir /path/to/native/cmake --extensions-dir /protected/path/extensions --profile dev --output /protected/path/grv-bundle
```

The bundle records `complete_client_v1: false`. Its DuckDB adapter loads the packaged guard library beside the executable. Installation and adapter discovery require protected directory ancestors.

The [CI workflow](.github/workflows/conformance.yml) runs native tests, strict lint and bundle relocation with Rust 1.89 on Linux and macOS, each on x86-64 and ARM64. Those platform runs remain release gates until their execution evidence is available. Runner labels follow [GitHub's supported runner reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners). Fetch and verify the required signed S3 extension packaging inputs using [these instructions](crates/grv-adapter-duckdb/native/EXTENSIONS.txt). The extensions root contains one subdirectory per fetched platform.

```console
GRV_ADAPTERS_DIR=/protected/path/grv-bundle/adapters /protected/path/grv-bundle/bin/grv --json adapter list
/protected/path/grv-bundle/bin/grv --json adapter install /protected/path/grv-bundle/adapters/duckdb
grv --json init --grv /protected/path/store
grv --json push --grv /protected/path/store --decl push.yml
grv --json pull --grv /protected/path/store --decl pull.yml --revision latest
grv --json pin data --grv /protected/path/store --revision 1 --reason 'retain input'
grv --json unpin data --grv /protected/path/store --revision 1 --pin <pin-uuid>
grv --json gc data --grv /protected/path/store --dry-run
grv --json gc data --grv /protected/path/store --apply
```

Salesforce uses existing local CLI authentication, API v66.0 and REST-first `auto`. See [its helper and credential requirements](crates/grv-adapter-salesforce/README.md). Credentials stay in local configuration and private helper pipes.

Native builds advertise snapshot extraction, transactional local and S3-view pulls, managed and external builds, and read-only inspection after their conformance gates. External preparation fixes held S3 input views independently of later tracking refreshes. Accepted exports and terminal outcomes replay without their source; a pending post-publication acknowledgement retries only its fixed idempotent hook. Platform execution and complete native dependency notices remain release gates. Live GCS primitives and fixture-CLI publication/replay have passed in owned writable prefixes; production GCS integration qualification is still incomplete.

The [first release validation plan](spec/grv-v1-release-validation.md) defines the full acceptance matrix. The [validation operations index](spec/release-validation/README.md) tracks the scoped initial macOS ARM64 gates, current status, reusable test catalog, immutable evidence references and change-based rerun policy. Development passes do not qualify later candidate binaries. The current macOS ARM64 native library requires macOS 26.0 or newer (tested host 26.7.1); do not infer macOS 11 support from the CLI or extension minimum alone. Initial scope and notice policy are recorded in [0.1.0-SCOPE.md](spec/release-validation/0.1.0-SCOPE.md).
