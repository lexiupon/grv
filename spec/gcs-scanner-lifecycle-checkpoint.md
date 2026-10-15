# GCS scanner rebind, cache, epoch and failure isolation (package 32)

**Status: package32 complete after parent corrections, independent offline
validation, frozen actual shipping-FFI/HTTPS execution and owned cleanup.**
Package25 is the next separately scoped package; broader package45 is not closed. No commit, publication, release-register refresh, capability
advertising, default change, helper restoration or allocator change is approved.

## Baseline and development history

The original baseline is `/tmp/grv-task32/baseline/` (tracked diff, status,
72 preexisting touched-file hashes, source/artifact hashes). The second handover
is `/tmp/grv-task32/handover-2/`. The original timed-out owned local fixture
`/tmp/grv-gcs-scanner-lifecycle-gmu3SG` remains historical evidence, not a cloud
resource. Do not infer a passing case or shipping defect from that timeout.

The first Qwen/xhigh agent produced a partial source-including proof and an
unrun setup case/synthetic test. The second verified the defects but made no
edits before its turn limit. The parent repaired and independently exercised
these paths. Development failures are preserved:

- Original signed `LOAD` directory/file argument error and 300-second timeout:
  original transcript/build evidence; no invented historical log hash.
- `/tmp/grv-task32/parent-proof-run-1.txt` and `-2.txt`: after removing the mock
  deadlock, direct opener-filesystem access outside a transaction exposed
  `ActiveTransaction called without active transaction` in the test harness.
- `parent-proof-run-3.txt`: the context's opener filesystem already supplies its
  opener; explicitly passing another opener is invalid. The test now opens
  direct handles within `RunFunctionInTransaction`, without a second opener.
- `parent-ctest.txt`: 14/15 passed, scanner configuration failed because CMake
  supplied an extension **file** to the scanner proof's **directory** argument.
  The test registration was corrected; final 15/15 pass.
- `parent-ctest-owner-records.txt`: a newly strengthened timeout assertion
  incorrectly required a nonzero microsecond component. Stock timeouts represent
  whole seconds plus a fractional microsecond remainder, so exactly30s has
  remainder0. The corrected assertion checks a positive combined timeout at
  most30s and a remainder below1,000,000; it preserves the bound rather than
  weakening it. Final `parent-ctest-owner-records-final.txt`: **15/15 pass**,
  with per-owner bearer/project/scope assertions also enforced.
- `parent-identical-synthetic.txt`: incorrect `native::tests` selector ran
  **zero tests**, not a qualification pass. `parent-identical-synthetic-exact.txt`
  runs the actual `native::descriptor_tests` symbol: one test passes, 4.68s.

No production execution behavior changed. `gcs.inc` corrections describe
original-access-mode recovery, not universally read-only recovery.
`gcs_setup_proof.cpp`, the standalone scanner target/CMake registration and
ignored Rust tests are qualification changes. Shipping
`libgrv_duckdb_guard.dylib` remains SHA256
`2709cc619eb125d1ecdc48377303019ea20bfe32f20c4e964c298b2b66abdd5f`;
it was not rebuilt during this stage.

## What the offline proof actually runs

`native/gcs_scanner_lifecycle_proof.cpp` includes `shim.cpp` and links the pinned
DuckDB/Parquet code. It loads the official pinned signed HTTPFS extension and
uses the actual guard, Parquet reader/binder, exact filesystem/handles,
`GCSScanScope` epochs and `GCSHTTPPolicy::SendRequest` implementation.

The stock transport delegate is replaced by a **mock recorder**, not a real
HTTPS socket. Parameter construction is a **model**: the real signed parameter
resolver is called outside queries and its concrete fields are captured; each
mock initializer returns a fresh copy. This avoids the original mock's
`RunFunctionInTransaction` re-entry into a nonrecursive query-held context lock.
It does not substitute a production locking change, nor qualify every signed
initializer overload in its genuine query context.

The recorder implements checked single-byte ranges, 206/Content-Range/length,
response callback before body callback, split-body delivery, failures and a
cooperative synthetic gate. Every recorded physical request must reach the
actual wrapper delegate (not the direct-client/base registry bypass). Every
used owner must create and dispose nonzero clients. The known base
`HTTPUtil::Request` registry bypass remains a separate signed-extension finding.

This source-including executable calls the `grv_native_*` implementation compiled
**into the proof**; it is not a shipping-dylib FFI test or source-to-signed-binary
correspondence proof. Native Rust synthetic qualification loads the shipping
dylib, but performs no provider acquisition or real cloud scan.

## Finite offline matrix — observed, not hypothetical

| Dimension                   | Parent-validated assertion                                                                                                                                                                                                                                                                  | Layer / limitation                                                                                                                |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Automatic rebind            | Prepared view/wrapper initially reads physical value111; replacing the view with a second admitted physical fixture returns222 and records that URL. Rebinding to an unadmitted fixture refuses before transport.                                                                           | Actual binder/Parquet/guard, mock delegate.                                                                                       |
| Handle epochs               | Same-epoch direct reads succeed. ScopeEnd and re-admission of the **same URI** cannot revive the old handle; fresh handles work.                                                                                                                                                            | Actual exact filesystem. Direct open has an explicit test transaction.                                                            |
| Retained prepared data      | An executed prepared plan may open a fresh admitted handle in a later epoch and return222. Re-executing while its URL is unadmitted refuses without a dispatch.                                                                                                                             | No unsupported blanket claim that all retained prepared plans must fail.                                                          |
| Fixed SQL semantics         | Explicit BIGINT cast, typed NULL and positional UNION ALL over differently named columns return correct complete row oracles111/444. Invalid explicit VARCHAR-to-BIGINT conversion refuses.                                                                                                 | Ordinary SQL unions may coerce; this is not an invented universal type-mismatch bind refusal.                                     |
| Copied bind / warm scan     | Copied/shared bind executes correctly. Cold/warm data agree; observed warm deltas HEAD1/GET2 in this fixture. Shipping cache settings are checked.                                                                                                                                          | No guaranteed request pattern, forced redispatch, universal cache absence or pruning claim; adaptive handle buffering can remain. |
| Typed parameters            | All three **wrapper** initialization entrypoints return private concrete modeled copies seeded by actual signed resolution. Original parameters remain unchanged across dispatch.                                                                                                           | Not three genuine signed resolver executions.                                                                                     |
| Mixed local                 | A verified, read-only, unlinked Parquet descriptor returns37. An existing valid raw local Parquet path, writable descriptor and directory descriptor refuse.                                                                                                                                | RAII descriptor ownership; not a missing-file-only denial.                                                                        |
| Failure restoration         | Separate fresh-owner HEAD exception, GET exception and body rejection exercise real native metadata cleanup; paths/scanners restore, admission ends, client disposal is nonvacuous, failed cloud authority cannot be reused.                                                                | Expected interrupt cleanup is not universally poison; these outcomes are asserted in their actual paths.                          |
| Parallel isolation          | Joined independent owners: one HEAD failure cannot change the sibling's value777, bearer/project or authority. Malformed admission preserves a healthy owner without dispatch.                                                                                                              | Two owners, no shared-connection concurrent query claim.                                                                          |
| Identical values            | Independent **synthetic** invocations may carry the same token string while preserving owner/project isolation. Exact Rust shipping-FFI synthetic test also passes.                                                                                                                         | No provider invocation; actual fresh/concurrent acquisition is package23 evidence.                                                |
| Cooperative cancel / reopen | A genuine configured owner enters the body gate; supported interruption returns an error, then the worker is joined before close. Durable witness991 survives an explicitly read-only reopen; mode is checked and a write refuses.                                                          | C++ proof ownership, not SDK Stopped/WorkspaceLock/immutable-outcome proof. Default recovery preserves the original mode.         |
| Setup failure stages        | Standalone `grv_gcs_setup_proof` exercises existing stage faults and the new failed-owner reconfiguration refusal: -1 with proved revocation/restoration, -2 where proof is unavailable.                                                                                                    | Explicit separate run using the signed **file** argument; not registered as an extra CTest.                                       |
| Closed artifacts / decoder  | All owners close before required regular-file inventory and complete streaming canary scans; strict inventory/read errors refuse. Owned local cleanup ends in verified ENOENT. Decoder rejects short/impossible dimensions/overflow/trailing bytes and consumes complete multi-row packets. | No universal heap erasure or adversarial race-safe opener guarantee.                                                              |

The deadline/gate uses test rendezvous and an external process watchdog.
`JoinedWorker` releases/interrupts and joins on failing assertions, propagating
worker exceptions. Completion timing is captured **before** joining so a late
join cannot fabricate a within-bound observation. This is a harness observation,
not a shipping hard-stop, finite native join, finite HTTP overrun or rollback-at-
expiry guarantee; a hung test process may still require its test watchdog.

## Parent validation stage (before the pending live-test addition)

Environment: target `/Users/jiacao/.grv-native-work/owner-validation-target`,
lib dir `/Users/jiacao/.grv-native-work/cmake`, signed extension dir
`/Users/jiacao/.grv-httpfs-prototype-extensions/osx_arm64`.

- `parent-proof-final.txt`: standalone source-including scanner proof passes.
- `parent-setup-final.txt`: standalone setup proof passes separately.
- `parent-ctest-final.txt`: **15/15** configured CTests pass;
  `parent-ctest-owner-records-final.txt` independently passes **15/15** after
  stronger per-owner bearer/project/scope and combined-timeout assertions.
- `parent-native.txt`: **117 passed /15 ignored**, plus **48 integration**.
- `parent-identical-synthetic-exact.txt`: exactly **1 passed**, real shipping FFI,
  synthetic identical token values, no cloud or provider acquisition.
- `parent-conformance.txt`: **22 host +12 peer CLI** pass (not a full new
  conformance/API/wire/SDK rerun).
- `parent-clippy-native.txt`, `parent-clippy-nonnative.txt`,
  `parent-fmt-check.txt`, `parent-diff-check.txt`: strict checks pass.

These are stage-scoped results. A later ignored live-test addition changes the
library test artifact and must be built/checked before its frozen live stage;
these counts/hashes are not silently promoted to a later candidate.

## Actual frozen real-route completion and carried limitations

The additional Qwen/xhigh real-route draft also reached its turn limit. The
parent reviewed actual files and repaired the incomplete Rust test/driver/plan:
actual descriptor metadata instead of placeholders, protected repo-local native
tempdir, strict artifact inventory, fixed named binaries/extensions, exact one-
test launches, automatic finally cleanup, generation/history-aware paginated
inventory, no invented inventory on404, and explicit limits on credential claims.
See `spec/gcs-scanner-lifecycle-live-plan.md` for the **reviewed plan frozen
before** acquisition; it remains immutable stage correspondence, not a claim
that planning itself passed the gate.

Final pre-live checks: `parent-live-offline-native.txt` **117/16ignored +48
integration**, strict native/non-native Clippy, fmt/diff, and
`parent-driver-offline-test.txt` **10 offline driver tests**. No ignored cloud
matrix ran with Cargo. Ten named artifacts were frozen in
`/tmp/grv-task32/live-freeze/artifact-hashes.json` before the lifecycle; no edit,
build or native/conformance run overlapped it.

Actual evidence: `/Users/jiacao/grv-task32-real-route-p44w44j_`:

- `shipping-route.txt`: exact
  `native::descriptor_tests::real_shipping_gcs_route_repeated_reads_and_unadmitted_url_refusal`,
  **1 passed /4.73s**. Real independently selected force-refresh token, unchanged
  shipping dylib, pinned signed HTTPFS, real fixed-host HTTPS. Same owner/original
  lease, positive A/B/A111/222/111, final unadmitted-B refusal while onlyA admitted,
  no reuse/retry, closed workspace/lock scan. No public request counter: zero
  rejected-request dispatch is source/proof-backed, not counted anew here.
- `full-preparation.txt`: exact
  `build::tests::live_nonempty_gcs_external_self_prepare_reopen_and_cleanup`,
  **1 passed /10.80s**, separately on owned id42. Actual external/self preparation
  and source-free reopen. The existing local preparation-fault and
  abort/outcome/cleanup branches are explicitly included, not a new cloud
  replacement/missing-object or published-hold matrix.
- `summary.json`: both exact-one-test exits0; no execution/cleanup error.
- `cleanup.json`: **3 generation-qualified deletes**, observed complete empty
  all-version inventory and bucket404; no keys/existing IAM/ACL/policy/versioning
  changes. Bucket `grv-httpfs-oauth-ba877aeb0a2c4505b5a2a9ed` is absent.
- Pre/post/current **all10 artifact hashes equal**, independently checked by the
  parent after lifecycle completion. Prior source-including proof/held evidence
  is not silently promoted to the new library-test artifact.

Baseline preservation: `parent-baseline-preservation.json` checks **68/72**
preexisting touched-file hashes unchanged; the four changes are scoped
`CMakeLists.txt`, `gcs.inc` comments, `native.rs` tests and preexisting untracked
`gcs_setup_proof.cpp`. No baseline file was deleted/reset; new scanner/plan
files are distinguished from the preexisting setup proof.

The real-route evidence separates repeated shipping-FFI/stock-HTTPS query
oracles from the source-including auto-rebind/cache matrix. Mandatory full
SHA/size/schema/row preparation remains unchanged and separately qualified;
below-SDK proof is not a waiver. No Task25 replacement/missing-object faults,
Task27 held lifecycle or Task26 S3 matrix is part of this gate.

No universal credential heap erasure, aggregate RSS bound, escaped arbitrary
prepared-owner lifetime proof, stock per-range generation pinning or protection
against privileged replacement is claimed. Source lifetime review and bounded
engine-owned close/reopen do not prove arbitrary escaped host objects vanished.
Original OAuth, exact fixed-host read-only authority, no redirects/proxy/retry,
original lease/no renewal and cooperative deadline limitations remain in force.
