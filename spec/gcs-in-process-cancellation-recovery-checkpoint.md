# GCS in-process cancellation and uncertain-outcome recovery checkpoint

Status: **targeted in-process cancellation/recovery qualification closed for the
five contracted cases** under the accepted
[execution decision](gcs-in-process-execution-decision.md). This is a new Rust
stage on top of the prior held-lifecycle artifacts; it does not promote or
re-qualify that held lifecycle, and the memory-scope
[pause](gcs-memory-qualification-completion.md) remains in force. No
publication, commit, IAM, credential, or release-register change is made here.

## Contract restated (qualified, not strengthened or waived)

- Execution lease is an admission/authority budget of at most 300 seconds,
  anchored monotonically before credential acquisition; later configuration can
  only shorten it. Actual token expiry must exceed the admitted budget plus at
  least 120 seconds. A fresh token/connection is required between executions.
- The lease is NOT a hard in-flight native stop/join guarantee. Stock
  synchronous HTTP cannot reliably be stopped: the retained synthetic
  header-trickle counterexample (250 ms lease returned after 3792 ms) remains
  the counterexample. No finite worst-case overrun, hard stop, or rollback-at-
  deadline claim is made or tested here.
- Supported cooperative interrupt (20 ms watchdog) plus RAII join is the only
  stop mechanism. The native owner/workspace lock is retained while the actual
  call is in flight; there is no detach, unsafe thread cancellation, or
  concurrent destruction. A stopped acknowledgement follows only actual
  stop/join evidence.
- Setup `-2`/unexpected status poisons operational reuse; only synchronous
  owner-thread teardown/reopen under retained ownership recovers. Teardown,
  process exit, interrupt, or Close alone is not immutable rollback/outcome
  proof.
- OutcomeUnknown is quarantined until actual stopped-writer/lock evidence and
  immutable durable resolution; never a false NotCommitted or a blind effects
  retry.

## Parent verification and scope

Requested subagent: `remote-qwen3.8-27b`, `xhigh`; resumed to complete missing
regressions and correct weak gate assertions. Parent inspected the actual changes,
not just the report, and reran the affected tests. Baseline diff comparison shows
only the five tracked files listed below changed; no preexisting diff blocks
were removed. Production Rust execution behavior is unchanged (new fields/hooks
are `cfg(test)`); only the standalone proof target was rebuilt, not the shipping
native library.

Parent added an explicit **GcsView** variant to the isolated pre/post-COMMIT crash
receipt test: `gs://` root, source-free empty fixed view, actual GCS metadata/lease
mode on child and recovery owner, real process exit and workspace lock. Immutable
same-request replay after advancing the checkpoint does not rewind/reapply.
No authentication, source request or cloud resource is used. The real SDK
cancel/EOF boundary test remains a local-root shared worker/transaction-path
proof; it is not newly claimed as a stock-HTTP GCS cancellation experiment.

Parent also hardened the test-only bridge decoder's length/integer checks and
made proof cleanup use `lstat` (no symlink traversal). The final proof passed
again. A formatting-only failure after adding the GCS test variant is preserved
in `/tmp/grv-task34-parent-fmt-development.txt`; the two line wraps were fixed,
and final fmt/Clippy/tests pass. No product behavior was weakened to pass.

## Qualification matrix

Case 1 — cancel before dispatch / original-lease shortening / fresh invocation.

| Evidence                                                                                                                 | Kind                                                                 | Result      |
| ------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------- | ----------- |
| `pull_worker::predispatch_tests::cancel_before_dispatch_joins_owner_and_refuses_without_effects` (new)                   | actual, offline                                                      | pass        |
| `pull::tests::native_pull_apply_cancel_before_dispatch_leaves_no_effects_and_no_false_not_committed` (new, fixture plan) | actual, offline                                                      | pass        |
| `build_worker::predispatch_tests::cancel_before_dispatch_joins_owner_and_never_runs_the_job` (new, counter closure)      | actual, offline                                                      | pass        |
| `native::descriptor_tests::in_process_original_lease_only_shortens_and_discovery_uses_same_connection`                   | actual, offline                                                      | pass        |
| `native::descriptor_tests::close_invocation_preserves_readonly_authority`                                                | actual, offline                                                      | pass        |
| `native::descriptor_tests::gcs_owner_normal_failure_rolls_back_and_committed_state_reopens`                              | actual, exact opt-in signed-extension test; synthetic token/no cloud | parent pass |

The new tests set the stop signal before any command reaches the owner and
prove: the command/closure never executes (counter stays 0; a lookup that had
run would have returned an ordinary reply), the owner handle and command
channel are empty after the join, the real `WorkspaceLock` is acquirable
again, a second dispatch is refused the same way (no reuse of a stopped
owner), and a fresh source-free resolution reports the true state
(`Uninitialized`) rather than a false `NotCommitted`, with no effects retry.

Case 2 — genuinely in-flight controlled blocked native operation retains
worker/workspace exclusion, with no stopped acknowledgement or reuse until
unblock plus join.

| Evidence                                                                                         | Kind                                                                     | Result |
| ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------ | ------ |
| ctest `duckdb_in_process_bounded_gate_fence` via `native/in_process_gate_proof.cpp` (new)        | synthetic gate on an actual WAL FileSync at COMMIT; manual flock sidecar | pass   |
| `tests/native.rs::interrupt_remains_safe_during_query_and_after_owner_shutdown`                  | actual, offline                                                          | pass   |
| `pull::tests::native_pull_owner_cancellation_joins_and_fences_before_outcome_resolution`         | actual, offline                                                          | pass   |
| `commit_boundary_tests::sdk_native_cancel_and_eof_at_commit_boundary_fence_and_immutable_replay` | actual SDK child, real workspace lock                                    | pass   |

The new proof is a bounded synthetic gate (a real `FileSync` at COMMIT held
behind a 10-second-bounded gate) and proves, while the operation is in
flight: the owner retains exclusion (a second `flock` fails), a real
`grv_native_interrupt` request does not stop the in-flight operation (the
gate is still held and the call has not returned 100 ms later), and only
after the gate releases and the owner closes does the lock free and a fresh
invocation get admitted. **Labelled limitation:** this harness opens the
engine directly and takes a manual flock; it is not the Rust `WorkspaceLock`
and not the SDK stopped-acknowledgement path, and `grv_native_close` there is
synchronous return plus destruction, not a thread join. The actual SDK
stop/ownership/join evidence for the same contract is the commit-boundary
test above (real child, real lock, no terminal frame while the owner is
held, immutable replay after recovery). Scenario 1 of the proof records the
observed post-interrupt commit state without claiming rollback or commit.

Case 3 — cancellation/expiry crossing commit/result publication boundaries
reports unresolved or an immutable confirmed result truthfully.

| Evidence                                                                                                        | Kind                                                                              | Result      |
| --------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- | ----------- |
| `native::descriptor_tests::in_process_lease_expiry_refuses_at_publication_boundary_and_reopens_immutably` (new) | actual COMMIT + reopen; synthetic boundary placement                              | pass        |
| `pull::tests::native_precommit_and_postcommit_process_exit_resolve_without_reapplying`                          | actual isolated crash child, Local/S3View/GcsView pre/post COMMIT                 | parent pass |
| ctest `duckdb_gcs_actual_wal_commit_failure_fence` (`native/transaction_fault_proof.cpp`)                       | actual WAL fault; synthetic fault injection                                       | pass        |
| `native::descriptor_tests::gcs_deadline_watchdog_interrupts_without_network_or_commit`                          | actual cooperative compute/watchdog; exact opt-in signed-extension test, no cloud | parent pass |

The new test runs under GCS metadata mode (`begin_gcs_execution`, no
authentication or HTTP) and uses a `cfg(test)`-only, per-engine, per-call
injection placed immediately after the successful native FFI return and
before the final publication deadline check: an actual `COMMIT` succeeds
natively, the original lease is then expired, and the result is refused at
the Rust publication boundary with a deadline error (the native outcome is
uncertain to the caller and is not rollback proof). After the synchronous
return, `close_invocation` retains actual workspace ownership and a fresh
immutable read confirms the committed row exactly once with no effects retry.
The same test also proves: an expired call is refused before dispatch with no
effect, an explicit `ROLLBACK` remains supported once control returns, and a
fresh invocation resets the lease and interrupt latch. The boundary placement
is synthetic; the COMMIT and reopen inspection are actual. This is not a
wall-clock timing or hard-HTTP proof.

Case 4 — setup quarantine with safe owner-thread reopen.

| Evidence                                                                                                   | Kind                                              | Result |
| ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------- | ------ |
| `native::descriptor_tests::in_process_failed_setup_quarantines_without_exiting_and_closes_before_recovery` | actual, offline                                   | pass   |
| ctest `duckdb_gcs_setup_fault_fence` (`native/gcs_setup_proof.cpp`)                                        | synthetic setup faults on actual secret lifecycle | pass   |

Unproved setup revocation (`-2`) or unexpected status poisons owner reuse
without exiting the embedding process; metadata and `ROLLBACK` are refused;
owner-thread `close_invocation` recovers; the transient setup test table is
absent and subsequent reads work. This observation and the separate actual
transaction/receipt recovery tests are not a general rollback attestation
inferred from close/reopen alone.

Case 5 — source-free stopped-writer plus lock recovery of a durable receipt
with an uncertain or lost reply.

| Evidence                                                                                                 | Kind                                                                  | Result      |
| -------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | ----------- |
| `pull::tests::native_identity_transaction_replays_without_files_and_refuses_lost_receipts`               | actual, offline                                                       | pass        |
| `pull::tests::native_precommit_and_postcommit_process_exit_resolve_without_reapplying`                   | actual isolated crash child; parent-added source-free GcsView variant | parent pass |
| `commit_boundary_tests::sdk_native_cancel_and_eof_at_commit_boundary_fence_and_immutable_replay`         | actual SDK child, real lock                                           | pass        |
| `pull::tests::native_prior_contracts_require_matching_scope_and_immutable_receipt_without_source_access` | actual, offline                                                       | pass        |

A committed receipt replays without source access and never re-applies; a
lost or erased receipt is `OutcomeUnknown`, never absence or repair; an
abrupt process exit at the commit boundary (isolated offline child, owned
temp workspace and journal, no host kill) resolves to the immutable outcome on
a fresh owner under the real workspace lock, with the same request admitted
exactly once. No universal abrupt-crash/WAL claim is made beyond these
receipt assertions.

## Validation (this stage)

Environment: `CARGO_TARGET_DIR=/Users/jiacao/.grv-native-work/owner-
validation-target`, `GRV_DUCKDB_NATIVE_LIB_DIR=/Users/jiacao/.grv-native-
work/cmake`, `GRV_DUCKDB_EXTENSIONS_DIR=/Users/jiacao/.grv-httpfs-
prototype-extensions/osx_arm64`, `GRV_GCS_STOCK_PROOF_BIN=/Users/jiacao/.grv-
native-work/cmake/grv_gcs_stock_request_proof`. Native/conformance targets run
serially (`--test-threads=1`). Subagent ran no ignored/live tests. Parent ran
exactly the two named ignored **synthetic signed-extension/no-cloud** tests,
not the live tests or the full ignored list. No live/held lifecycle or cloud
operation was started; nothing is running.

- `cargo test -p grv-adapter-duckdb --features native --lib -- --test-threads=1`
  → **109 passed / 0 failed / 12 ignored** (baseline 105 + 4 new); log
  `/tmp/grv-task34-lib-after.txt` (baseline `/tmp/grv-task34-baseline-lib.txt`).
- `cargo test -p grv-adapter-duckdb --features native --tests -- --test-threads=1`
  → **48 passed / 0 failed** (build 22, build_process 2, extraction 9,
  native 4, ownership 7, process 1, pull 2, pull_host 1); log
  `/tmp/grv-task34-tests-after.txt`.
- `cargo test -p grv-adapter-sdk --lib -- --test-threads=1` → **18 passed /
  0 failed**; log `/tmp/grv-task34-sdk-after.txt`.
- `ctest` in `/Users/jiacao/.grv-native-work/cmake` → **13/13 passed**
  (12 prior + 1 new gate proof); log `/tmp/grv-task34-ctest-after.txt`.
- `cargo clippy -p grv-adapter-duckdb --features native --tests -- -D
warnings` and `-p grv-adapter-sdk --tests -- -D warnings` → clean; logs
  `/tmp/grv-task34-clippy.txt`, `/tmp/grv-task34-clippy-sdk.txt`.
- `cargo fmt -p grv-adapter-duckdb -p grv-adapter-sdk --check` → clean; log
  `/tmp/grv-task34-fmt.txt`.
- New gate proof build: `cmake --build . --target grv_in_process_gate_proof`
  (log `/tmp/grv-task34-gate-build.txt`); ctest property `TIMEOUT 120` is a
  backup, not the proof. The proof removes its owned `/tmp` workspace on exit.
- Shipping native dylib re-hashed after all work: unchanged at
  `2709cc619eb125d1ecdc48377303019ea20bfe32f20c4e964c298b2b66abdd5f`. The new
  proof links the pinned static libraries only; the prior held-lifecycle
  artifacts are not promoted to this new Rust stage.

### Final parent validation

- `cargo test -p grv-adapter-duckdb --features native --tests -- --test-threads=1`:
  **109 library passed/12 ignored +48 integration passed**, after the parent
  GcsView crash variant; `/tmp/grv-task34-parent-final-tests.txt`.
- Two exact signed synthetic tests selected with `--lib <full-name> -- --exact
--ignored --test-threads=1`: **1 pass each**, watchdog4.03seconds and normal
  failure/fresh owner1.54seconds. Logs `/tmp/grv-task34-parent-watchdog.txt` and
  `/tmp/grv-task34-parent-fresh-owner.txt`; no credentials/cloud requests.
- `cargo test -p grv-adapter-sdk --tests -- --test-threads=1`: **18 library +3
  runtime passed**, including terminal-refusal cancellation behavior;
  `/tmp/grv-task34-parent-sdk.txt`.
- Standalone proof rebuilt with decoder/cleanup corrections, **13/13 ctest
  passed**: `/tmp/grv-task34-parent-gate-build.txt`,
  `/tmp/grv-task34-parent-ctest.txt`.
- Strict native Clippy and final fmt/diff clean:
  `/tmp/grv-task34-parent-clippy.txt`, `/tmp/grv-task34-parent-fmt-final.txt`.
- Evidence checker: catalog42/records94/digests matched; release readiness not
  assessed. Source-change traceability gate is not silently refreshed. Saved
  source/proof/native/log SHA256 are in
  `release-validation/gcs-in-process-cancellation-recovery-checkpoint.json`.

## Files changed in this stage

- `crates/grv-adapter-duckdb/src/pull_worker.rs` — new `predispatch_tests`
  module (lookup pre-dispatch refusal, join, lock release, no reuse, true
  absence).
- `crates/grv-adapter-duckdb/src/build_worker.rs` — new `predispatch_tests`
  module (counter closure proves no dispatch, join, lock release, no reuse).
- `crates/grv-adapter-duckdb/src/pull.rs` — new fixture-plan apply
  pre-dispatch regression in the existing test module; parent added GcsView
  metadata-mode variant to the pre/post-COMMIT crash receipt test.
- `crates/grv-adapter-duckdb/src/native.rs` — `cfg(test)`-only
  `test_expire_lease_after_ffi` field plus post-FFI/pre-publication-check
  injection in `internal_query` (production behavior unchanged) and the new
  lease-expiry publication-boundary test.
- `crates/grv-adapter-duckdb/native/in_process_gate_proof.cpp` (new) and
  `crates/grv-adapter-duckdb/native/CMakeLists.txt` (new target + ctest
  registration with `TIMEOUT 120`).

## Open cases (not closed by this stage)

- The 250 ms → 3792 ms header-trickle counterexample stands: no finite
  worst-case overrun, hard stop, or rollback-at-deadline guarantee exists or
  is claimed for stock synchronous HTTP.
- The two signed-extension synthetic tests were independently rerun by the
  parent; neither qualifies in-flight stock-HTTP interruption. Real HTTP
  overrun and broader cloud fault coverage remain separate; no hard-stop
  promise was introduced.
- The gate proof is a native-boundary model (manual flock, direct engine
  open); the Rust `WorkspaceLock` and SDK stopped-ack evidence remain the
  commit-boundary test's domain.
- Broader setup/security, scanner/rebind/cache isolation, held terminal
  cleanup, S3 matrix, install/platform/package/MSRV and release gates remain
  open per the transition checkpoint; the memory-scope pause is unchanged.
