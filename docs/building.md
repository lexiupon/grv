# Building GRV from source

Use this guide if no prebuilt package exists for your platform yet, or if you
want to run a local development bundle. Most users should install with
Homebrew instead (see the [README](../README.md#install)).

## Requirements

- Rust 1.89 or newer
- For the DuckDB adapter: CMake and Python 3.11 or newer

## Store commands only

The store commands (`init`, `ls`, `show`, `log`, `diff`, `verify`, `pin`,
`unpin`, `gc`, `recover`) and `grv skills` need only the CLI:

```console
cargo build --workspace --release
./target/release/grv init --grv ./grv
./target/release/grv ls --grv ./grv
```

`push` and `pull` also need an installed adapter, described next.

## A local bundle with the DuckDB adapter

1. Build the pinned DuckDB native guard (DuckDB 1.5.6; see
   [BUILDING.txt](../crates/grv-adapter-duckdb/native/BUILDING.txt)):

   ```console
   curl -fL https://github.com/duckdb/duckdb/archive/refs/tags/v1.5.6.tar.gz \
     --output /tmp/duckdb-1.5.6.tar.gz
   python3 crates/grv-adapter-duckdb/native/build.py \
     --archive /tmp/duckdb-1.5.6.tar.gz --build /tmp/grv-duckdb-native
   ```

2. Fetch the pinned, signed DuckDB extensions for your platform
   (`osx_arm64`, `osx_amd64`, `linux_amd64`, `linux_arm64`; see
   [EXTENSIONS.txt](../crates/grv-adapter-duckdb/native/EXTENSIONS.txt)):

   ```console
   python3 scripts/fetch-duckdb-extensions.py \
     --output /protected/path/extensions --platform osx_arm64
   ```

3. Package a development bundle. It contains the three executables, hashed
   adapter manifests, the pinned native library, the signed extensions and an
   observed capability inventory:

   ```console
   python3 scripts/package.py \
     --native-dir /tmp/grv-duckdb-native/cmake \
     --extensions-dir /protected/path/extensions \
     --profile dev --output /protected/path/grv-bundle
   ```

4. Install the adapter and run a transfer:

   ```console
   export PATH=/protected/path/grv-bundle/bin:$PATH
   GRV_ADAPTERS_DIR=/protected/path/grv-bundle/adapters \
     grv adapter install /protected/path/grv-bundle/adapters/duckdb
   grv init --grv /protected/path/store
   grv push --grv /protected/path/store --decl spec/examples/duckdb-build-push.yml
   ```

`/protected/path` must be a **protected directory**: every ancestor is owned
by you (or root), none is group- or other-writable, and the path contains no
symlinks. Adapter installation and discovery refuse anything else.

The bundle records `complete_client_v1: false`. Its DuckDB adapter loads the
packaged guard library that sits beside the executable.
