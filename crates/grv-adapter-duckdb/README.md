# DuckDB native adapter

The native feature requires the patched DuckDB 1.5.6 shim. A stock DuckDB
library cannot satisfy its guarded binding ABI. The source archive SHA-256 is
`1fadcbe9e69e1470f9093b6bcde08daf477d729c449e59a807f45c346622099b`.

```sh
python3 crates/grv-adapter-duckdb/native/build.py --archive /tmp/grv-duckdb-v1.5.6.tar.gz --build /tmp/grv-duckdb-native --jobs 4
GRV_DUCKDB_NATIVE_LIB_DIR=/tmp/grv-duckdb-native/cmake cargo test -p grv-adapter-duckdb --features native --offline
GRV_DUCKDB_NATIVE_LIB_DIR=/tmp/grv-duckdb-native/cmake cargo clippy -p grv-adapter-duckdb --features native --all-targets --offline --no-deps -- -D warnings
```

The native library owns guarded initial/nested binding and automatic rebind,
including bind-time custom casts and template callbacks. User SQL only resolves
declared logical input bindings and trusted built-in callbacks. Internal table
access uses separately authorized private relations. The engine has a 512-MiB
memory bound and a 16-GiB temporary staging quota, separate from the 32-MiB
source and scratch bounds and four 8-MiB transfer credits.

## Read-only inspection

Native builds advertise read-only connection inspection after the process and
CLI conformance gate. The inspection runtime owns a read-only transaction and workspace lock. It
reads recognized local metadata, checks immutable pull receipts, and reports
materialization mappings, successful imports and durable build-session facts.
It does not execute declaration SQL or read source rows. The parent projects
materialization state and observed GRV run facts separately. Foreign roots,
unknown metadata, missing immutable receipts and lost build history are errors;
inspection never repairs them. The native metadata unit is 2 MiB and inspection
requires at least 16 MiB of scratch space before opening the engine. Oversized
summaries are refused instead of truncated.

A protected lock-file witness records prior managed initialization and build
history before their first commit. Proved pre-commit rollback restores the exact
previous witness. A successful or uncertain commit preserves it, so lost engine
metadata cannot authorize a fresh workspace or an empty session observation.

## External driver library seam

`driver::ExternalInvocation` supervises an external engine command against an
already prepared `BuildSession`. `start` consumes `BuildStore`, persists a unique
invocation before spawn, closes native connections, and retains the canonical
workspace lock through an inherited descriptor. Failed or ambiguous spawn does
not authorize another invocation of that session.

The child receives `GRV_DUCKDB_WORKSPACE_LOCK_FD`. Every cooperating writer must
retain that descriptor, including descendants, until its engine work has
stopped. The driver holds workspace ownership only; backend renewal can take
the independent `grv_adapter_host::session_lock::SessionMutationLock`. CLI
mutations take session ownership before workspace ownership.

`complete` polls a caller-provided run-ownership callback. The driver supplies
actual completed output results and the completion kind. A failed invocation,
lost ownership, or unjoined descendant cannot produce completion. Cancellation,
`abort`, and `Drop` stop the process group, escalate ignored SIGTERM, and wait
for inherited ownership to stop. The leader is observed without reaping until
all group signals are finished, preventing process-ID reuse during signaling.
Escaped writers retaining ownership leave the workspace busy and the session
unresolved. Arbitrary external processes remain the driver's responsibility;
the CLI cannot infer invocation success from prepared tables or stop unknown
writers by stealing ownership.

Before completion publication, the helper independently reacquires workspace
ownership and the native database file lock, verifies the exact fixed session
and output base tables, and closes every native connection. Workspace ownership
remains held while the completion file becomes durable. `completion::read` and
`completion::publish` enforce owner-only regular files, protected ancestors,
no symlink/hardlink aliases, a 2-MiB bound, strict JSON, fixed identities and
selected-output mappings. Canonical publication is atomic and never replaces a
different established completion. Unresolved sessions cannot be cleaned; abort
and confirmed terminal outcomes preserve their evidence after table cleanup.

These are reusable library helpers, with no additional public commands.
Native builds advertise managed and external builds after parent preparation,
publication, recovery and CLI conformance gates. The live external S3 gate
checks held private views through tracking refresh, explicit empty outputs,
stopped writers, and accepted export replay with reader configuration removed.

An independent driver linked against the pinned library's C API calls
`native::load_static_extensions` on its exclusively owned live `duckdb_database`
before reading Parquet. This registers the compiled Parquet extension without
executing SQL or changing guards. S3 readers additionally load the pinned signed
httpfs/aws pair and configure their separate read-only named profile; exact S3
filenames use URL compatibility mode. The protected reader configuration and
canonical-URI conversion are documented in [BUILDING.txt](native/BUILDING.txt).
