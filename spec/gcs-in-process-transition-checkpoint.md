# GCS in-process transition checkpoint

Status: implementation, offline slices and authorized live pull/self-preparation
pass; actual held lifecycle passed on prebuilt unchanged artifacts. Remaining
memory/fault/package qualification is open. Not release acceptance. Approved direction:
[gcs-in-process-execution-decision.md](gcs-in-process-execution-decision.md).

## Implemented

- NativeEngine directly owns the GCS DuckDB connection, as for local/S3. Removed
  active `ProcessOwner`, helper entrypoint/Cargo binary, private JSON/raw-frame
  IPC, SCM_RIGHTS remapping, process supervisor/self-exit and helper override.
- Packaging no longer copies/relocates the extra binary; host no longer forwards
  its override. Historical source moved unchanged to
  `prototypes/cloud-views/isolated-owner-archive/`, outside Cargo/package paths.
  Archived tests remain historical, not current passing-test counts.
- Native guard revision6 is coordinated in native/Rust/Cargo. No public wire,
  schema, binding or durable version bump. Added native bounded-metadata activation
  separately from cloud configuration/authority; source-free GCS metadata can
  stream before credentials, with no new scanner authority.
- Discovery starts its original lease and BEGIN on the same native connection.
  Later OAuth configuration never closes/hands off that connection or replenishes
  the original deadline. Fresh configurations require a fresh invocation owner.
- Cooperative per-call watcher invokes supported DuckDB Interrupt after expiry
  until the synchronous call returns, then joins. It cannot forcibly cancel stock
  synchronous HTTP. Native work is never detached or concurrently destroyed.
  Preexisting interrupt clones keep the same native control/cancellation latch.
- Remote scan checks cancellation before dispatch and after return. Expired/late
  results refuse. Explicit ROLLBACK is available once the synchronous operation
  has returned; it is not a stopped-writer/immutable-outcome attestation.
- Unproved setup revocation/restoration (`-2` or unexpected status) poisons Rust
  owner reuse, without `_exit` of an embedding process. Operational FFI entry
  points refuse until owner-thread synchronous close/reopen under workspace lock.
  Read-only mode survives close/reopen; failed reopen leaves owner unusable.
- Keep direct verified read-only descriptor scanner, signed stock transport,
  exact file/epoch guards, full preparation, local/S3 behavior and GCS-only
  bounded arguments/decoded metadata/durable CASE error projection. No production
  allocator policy installed.32MiB scratch admission remains conservative, not a
  proved total-engine/RSS bound; partial512MiB engine setting remains separate.

## Validation

- Native library unit tests:90passed/13ignored. Reduction from prior counts is
  removal of helper IPC/recovery test modules from production compilation, not
  a claim that those tests now exercise in-process execution.
- Six explicit signed-extension synthetic slices pass: configure after discovery
  BEGIN preserves snapshot and original interrupt clone; cooperative compute
  expiry/rollback; ordinary failure rollback/commit/reopen; bounded streamed
  metadata; concurrent-owner cancellation/isolation; durable projection bounds.
- Normal suite includes source-free preallocation argument refusal, unlinked
  descriptor reads, malformed durable receipt OutcomeUnknown with DB/witness
  bytes unchanged, setup quarantine without process exit, original lease
  shortening, and read-only reopen preservation.
- Twelve native ctest proofs and signed post-secret setup fault proof pass.
- Stale-registry CLI conformance passes. Strict native/conformance Clippy,
  rustfmt and diff checks pass. Packaging removal agent reports49script tests
  passed; actual removal checked by parent. No helper references remain in active
  adapter source/Cargo/package/host allowlist.
- Independent read-only lifecycle review found two concrete gaps: pre-dispatch
  cancellation and read-only reopen mode. Both fixed with regression assertions.
  Review was scoped, not universal security approval.

## Resolved authentication blocker and remaining acceptance

The retained failed live driver attempt `~/grv-shipping-gcs-f3r0h_88` stopped while
acquiring the selected account's token before bucket creation/creation intent.
No test cloud object or bucket was created by that attempt. Absence was not
independently queried because authentication failed; do not invent404 evidence.
Private diagnostic classification indicated operator reauthentication required
for `jiacao@on.sinch.com` / `entit-finance-prod`; no identity fallback or token
output. Operator subsequently restored its trusted login; explicitly selected
config-helper refresh matched account/project without printing credentials.

Revision6 live rerun `~/grv-shipping-gcs-296qgt4m` passed both guarded pull/refresh/
source-free replay and nonempty external self-preparation/reopen/cleanup tests.
Owned cleanup deleted one generation, verified empty all-version inventory and
bucket404. Prior failed attempt is retained unchanged; this success does not
complete the whole cloud, memory or fault matrix.

Actual parent-held lifecycle `~/grv-held-gcs-dw_ypv61` passed:1test,0failures,
1962.69seconds. The prebuilt conformance test, CLI, GCS fixture, native library
and signed extension have matching pre/post SHA256 hashes. The direct driver
ran without Cargo/rebuilding during execution. Owned all-version inventory empty
and bucket404 verified. Zero final-cleanup generation deletions after test-prefix
cleanup is not a total deletion count. Successful published dependency holds
remain; this is not abort/prune/release/expiry evidence.

Later Rust generated-SQL memory admission edits are a new stage, not silently
qualified by that held pass. Native library unchanged; those edits have separate
unit/mixed-local/live regression validation.

Prior livepair `73ki_c3w` and held lifecycle `9zqrg2qn` belong to earlier helper/
library stages. They do not qualify revision6 in-process code. Finish actual
unchanged-artifact held lifecycle (pre/post hashes), failed-setup/recovery and
installed package paths before closing task45. The current live pair satisfies
only the pull/self-preparation slice; the new held evidence covers the named
parent lifecycle with its scoped limitations.

Transfer/metadata/caller-document lifetime accounting remains task37; hard native
stop/join and total-engine/RSS are expressly outside the accepted contract, not
hidden passing tests. Credential-copy/security acceptance and broader recovery
matrices remain separate open gates. The user's pause-after-memory-completion
instruction remains in force. No commit/push/publication/advertisement, no new
credentials or IAM changes. The authorized actual held lifecycle has ended and
owned cloud cleanup is verified; no GCS helper process was involved.
