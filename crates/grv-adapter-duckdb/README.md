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
access uses separately authorized private relations. The engine has a configured 512-MiB
DuckDB memory_limit and a 16-GiB temporary staging quota, separate from source/
scratch budgets and four 8-MiB transfer credits. DuckDB's setting does **not**
cap every engine/extension allocation or total RSS; transfer buffering has its
own bounds.

## Keyless GCS implementation foundation (not advertised)

GCS now executes in-process within this adapter, matching local/S3. The
additional GCS native-owner executable and IPC/package/override requirements
have been removed; historical source is archived under prototypes. Requalification
of the new path is still in progress. No hard native stop/join/rollback
at lease expiry or total engine/RSS cap is promised. See the
[decision and explicit limitations](../../spec/gcs-in-process-execution-decision.md).
Helper-only allocation-policy experiments will not be adopted as production
requirements. Transfer-buffer bounds and fail-closed recovery remain required.

`gcs_config` loads the owner-only engine-adjacent `.grv-gcs-read.json` selector
without auth/network, requires explicit gcloud account/project and canonical
scope, and maps exact gs coordinates to fixed-host HTTPS filenames. `gcs_token`
uses projected gcloud config-helper JSON for matching token/expiry, rejects auth
overrides, zeroizes bounded private buffers and exposes at most a 300-second
monotonic lease with a 120-second expiry margin. It does not guess lifetime from
`print-access-token`. Local gcloud login storage is trusted operator state;
impersonation/credential overrides are not automatic providers.

The native `gcs_http_policy.hpp` parameter wrapper retains signed stock HTTPFS
transport while disabling redirects/proxies and enforcing TLS/timeouts/retries.
Its synthetic two-origin proof runs with
`bash prototypes/cloud-views/httpfs/policy_probe.sh`. The redirect target must
receive no request under policy; the unhardened stock control follows it.

Native guard revision6 retains exact read-only HTTPS admission, per-handle
epochs, request/content checks and bounded metadata streaming. Discovery BEGIN
and later GCS setup remain on the same connection; no snapshot handoff, helper
IPC or descriptor remapping. Verified local inputs use the existing retained
read-only descriptor scanner directly, without reopening source paths.
GCS still admits **32 MiB scratch** and **2 MiB source unit** before execution/
authentication; this conservative reserve bounds transfer/metadata handling,
not total engine allocation or RSS. Decoded metadata has an8MiB logical storage
cap, command strings an aggregate2MiB cap, encoded results at most2MiB. Local/S3
admission is unchanged. The in-process reserve/high-water qualification remains
open; no helper allocator policy is installed.

A joined per-call watcher requests supported DuckDB interruption at the original
monotonic lease; it cannot forcibly stop stock synchronous HTTP. Expired/cancelled
calls refuse late success. ROLLBACK remains available after the native call
returns. Unproved setup cleanup quarantines the connection (no process _exit),
with synchronous owner-thread teardown/reopen under retained workspace ownership
before recovery. Close/reopen alone is not an immutable-outcome resolution.
Revision 3's
engine request hook was bypassed by signed HTTPFS's private methods; its mock
proof does not qualify that shipping boundary. Revision 4 binds pinned
HTTPFSParams to a virtual policy delegate forwarding to signed stock **httplib**.
Stock curl has no hard transfer deadline and is not admitted for bounded GCS mode.
The delegate removes cached clients, bounds remaining timeout and checks GET
content delivery. Signed-extension direct/redirect/stalled-HEAD/trickling-GET
proofs pass, but continuously trickling HEAD headers exceed the lease: stock
httplib timeouts are not absolute either. Hard in-flight owner stop/join is now
an explicitly accepted limitation, not a passing proof. New in-process path,
request isolation, failed-setup cleanup and honest cancellation/recovery remain
release qualification work.
A ten-trial shipping-scanner GCS slice passed with local oracles and query body
projection/selective-wide savings of **99.43% / 96.69%** on its fixed fixture;
preparation was not measured and still downloads/verifies complete objects.
Earlier live pull and adapter-level self-preparation passed, including rollback
and source-free replay. Independent final review, actual held lifecycle closure,
complete shipping matrices, fault/resource/cancellation qualification and
packaging remain pending: `gcs-view` is **not advertised**.
No token is persisted in the view; ordinary DuckDB users need independent
short-lived HTTPS HTTP secrets and are not protected by the GRV guard. See the
[implementation plan](../../spec/grv-cloud-views-pushdown-v1-plan.md).

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
