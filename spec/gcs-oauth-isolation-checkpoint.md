# GCS OAuth isolation and failed-setup cleanup checkpoint (package #23)

Status: **package #23 complete within the accepted OAuth-isolation/cleanup
contract**, after parent review/adjustments and successful selected real OAuth
retry. Broader scanner/cloud/lifecycle/install/release gates remain open. This
checkpoint records the OAuth-isolation and failed-setup-cleanup qualification
for the current revision6 in-process architecture, under
[gcs-in-process-execution-decision.md](gcs-in-process-execution-decision.md),
[gcs-in-process-transition-checkpoint.md](gcs-in-process-transition-checkpoint.md),
[gcs-setup-revocation-checkpoint.md](gcs-setup-revocation-checkpoint.md)
(revision5/helper sections historical),
[gcs-memory-qualification-completion.md](gcs-memory-qualification-completion.md)
and
[gcs-in-process-cancellation-recovery-checkpoint.md](gcs-in-process-cancellation-recovery-checkpoint.md).
It is not release acceptance, not publication, and it does not close the
broader transition, packaging or release gates. The release traceability
register is not refreshed here. The first parent OAuth gate failed at `~/grv-gcs-oauth-private-zlq4s1fn`;
concurrent acquisition was not launched on that attempt. Failed evidence remains.
After the user reported restored access, the explicitly authorized retry at
`~/grv-gcs-oauth-private-5lu6gd8k` passed both gates with three pre/post hashes
unchanged. Operator blocker52 resolved; #24 may start separately.

## Parent final review and adjustments

Parent inspected the actual code, not just the subagent report, and corrected:

- Child probes were still unguarded ordinary library tests and could mutate the
  default test process environment. Each now requires the exact parent-set child
  selector; only exact child probes create fixtures. The parent compares all
  environment keys **and values**, sorted, without rendering private values.
- The native proof returned before its claimed close/flush/cleanup. Actual
  owner/workspace RAII now runs on all exits; both owners close **before** the
  second scan. Required DB absence now fails, only optional WAL ENOENT skips,
  and incomplete reads fail. No symlink traversal in owned-file cleanup.
- Concurrent proof calls now use joined futures with a bounded rendezvous and
  exception propagation. Tracked clients demonstrate wrapper destruction, not
  a vacuous initially-null client check. Original timeout/headers/canary are
  unchanged despite shorter per-request leases.
- The unexecuted `-1` branch incorrectly expected Ok. It now expects Err with no
  Rust owner poisoning; explicit mapping assertion exercises that branch even
  where actual missing-LOAD returns `-2`. Native observed-absence evidence remains
  in the separate setup proof.
- The plan-shape test did not create a receipt. A new exact signed **shipping FFI**
  test installs a synthetic canary, creates an empty GcsView and actual receipt,
  closes/flushes, independently resolves the immutable receipt, and scans all
  present owned regular files recursively (unexpected symlink/read errors fail).
  This is not an export or nonempty cloud-data lifecycle.
- Real concurrent acquisition uses scoped threads; both launched threads join
  before either result is examined, including unexpected failure paths.

These are test/proof corrections only; production Rust behavior and the shipping
native dylib remain unchanged. Subagent stage logs/reports retain their actual
scope and are not silently promoted to the corrected parent stage.

## Scope and authority

Package #23 covers only:

- Explicit gcloud acquisition: ambient-override refusal before any effect,
  no auth fallback, bounded private helper channel, and synthetic bearer
  canaries across subprocess error/timeout/partial-output paths.
- Post-secret setup failure: per-stage `-1`/`-2` semantics, observed secret
  absence, guard/allowed-path/external-access restoration, failed-state
  quarantine, no operational reuse on uncertainty, no false effect absence.
- Credential isolation: canary secrets unreachable from guarded SQL, catalog,
  profiling, settings, view definitions and error text; no synthetic bearer in
  durable files, views, receipts, logs or output bytes.
- Concurrent independent native owners: distinct synthetic canaries, exact
  per-scope routing, no cross-scope or wrong-token requests.
- Lease integrity: configuration/acquisition never renews the original
  execution lease.

Out of scope here (separate packages/gates): full retained-handle isolation
matrix (#32), broader real cloud lifecycle coverage, memory-scope work (paused per
[gcs-memory-qualification-completion.md](gcs-memory-qualification-completion.md)),
and all release/packaging gates. No new transport, key, credential, IAM,
wire, schema or durable version change is introduced or implied.

## Baseline (before this package)

- Working tree: preexisting dirty work preserved; baseline captured in
  `/tmp/grv-task23-baseline-status.txt`,
  `/tmp/grv-task23-baseline-diffstat.txt`,
  `/tmp/grv-task23-baseline-diff.txt` (37 modified files, 4543+/222- at
  capture). Parent final baseline-diff comparison checks the precise changed
  blocks rather than claiming an identical dirty-file count.
- Source hashes (before): `/tmp/grv-task23-baseline-source-hashes.txt`;
  artifact hashes (before): `/tmp/grv-task23-baseline-artifact-hashes.txt`.
- Shipping native library:
  `2709cc619eb125d1ecdc48377303019ea20bfe32f20c4e964c298b2b66abdd5f`
  (`~/.grv-native-work/cmake/libgrv_duckdb_guard.dylib`).
- Signed extensions: `httpfs.duckdb_extension`
  `0cde1f5acd1970bfd414b11f5e945a1eddd8425e96e273f6411538d8fe3d951d`,
  `aws.duckdb_extension` `911b68461fdd989e0c9e74926b197c8a05727bfafc05df38baa47ece42174f1c`.
- Baseline suites (serial, `--test-threads=1`), logs
  `/tmp/grv-task23-baseline-cargo.txt`, `/tmp/grv-task23-baseline-ctest.txt`:
  - `grv-adapter-duckdb` native lib: **109 passed / 0 failed / 12 ignored**.
  - `grv-adapter-duckdb` integration: **48 passed / 0 failed** (build 22,
    build_process 2, extraction 9, native 4, ownership 7, process 1, pull 2,
    pull_host 1).
  - `grv-adapter-sdk` lib: **18 passed / 0 failed**; runtime tests **3 passed**.
  - ctest: **13/13 passed**.

## Files owned by this package

Own files (new or edited in this pass; all other dirty-tree files are
preexisting and were not modified beyond their baseline state):

- `crates/grv-adapter-duckdb/src/gcs_token.rs` — additive only:
  `AccessToken::synthetic_with_canary_for_test` (cfg test+native; production
  non-Clone/non-Serialize/zeroized/redacted-Debug unchanged), the
  parent-spawned ambient child-probe design (parent test + two child tests +
  private `AmbientFixture` RAII fixture), the reformatted canary test, and
  the new ignored real fresh/concurrent gate test.
- `crates/grv-adapter-duckdb/src/native.rs` — additive only: new
  `configure_with_longer_token_deadline_never_renews_original_lease`, new
  `in_process_proved_setup_failure_keeps_owner_usable_for_local_work`, and
  the rewritten ignored `concurrent_gcs_owners_cancel_and_reconfigure...`
  (distinct canaries, cross-scope refusal, full owned-artifact scan).
- `crates/grv-adapter-duckdb/src/pull.rs` — additive only: new
  `gcs_view_sql_and_receipt_contain_exact_urls_not_credentials`.
- `crates/grv-adapter-duckdb/native/gcs_oauth_isolation_proof.cpp` — new.
- `crates/grv-adapter-duckdb/native/CMakeLists.txt` — additive only: new
  proof target + ctest registration; `GRV_DUCKDB_EXTENSIONS_DIR` is now a
  cache variable with an **empty default** (no hardcoded user path); the new
  ctest is registered only when the variable is set (this pass configured
  with `-DGRV_DUCKDB_EXTENSIONS_DIR=.../osx_arm64`), with `TIMEOUT 120`.
- `spec/gcs-oauth-isolation-checkpoint.md` — this file (new).

No production execution behavior was changed: Rust changes are confined to
`cfg(test)` helpers/tests. No shipping-library rebuild was required/performed.

## Qualification matrix (current revision6)

Kind legend: **source** = source-including proof (test-only seams, mock or
bounded transport; not the shipping binary); **shipping** = assertion against
the shipped library/extension artifacts; **actual** = real code path executed
offline (no cloud); **live** = requires the real gcloud identity (parent gate).

| #   | Case                                                                                                                                                                                                                                                                                                                                                                                      | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Kind                                                   | Result                                             |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ | -------------------------------------------------- |
| 1a  | Ambient `CLOUDSDK_AUTH_*` override refused before any helper invocation (no auth fallback); parent process environment never mutated                                                                                                                                                                                                                                                      | `gcs_token::tests::ambient_auth_override_refuses_before_any_helper_invocation` (new): parent spawns the lib test binary as a child with every ambient key `env_remove`d from the child only; child control proves the fake helper is invoked exactly when the environment is clean; child probe proves refusal **before** any helper invocation under override; parent asserts its inherited ambient state byte-for-byte unchanged                                                     | actual, offline                                        | **pass**                                           |
| 1b  | Helper subprocess failure/timeout/partial-output never carries a token canary into public error evidence                                                                                                                                                                                                                                                                                  | `gcs_token::tests::subprocess_token_canary_never_enters_public_evidence` (new): canary-bearing helper output in fail/timeout/partial-output/stderr paths; every public error is the fixed generic diagnostic                                                                                                                                                                                                                                                                           | actual, offline                                        | **pass**                                           |
| 1c  | Selectors, duplicate JSON, escaped token, expiry, process bound, private error                                                                                                                                                                                                                                                                                                            | existing `gcs_token::tests` (selectors/duplicateJSON/escapedtoken/expiry/processbound/privateerror)                                                                                                                                                                                                                                                                                                                                                                                    | actual, offline                                        | pass (baseline)                                    |
| 1d  | Config-file credential fields, aliases, defaults refused; error text canary-free                                                                                                                                                                                                                                                                                                          | existing `gcs_config::tests::closed_configuration_refuses_duplicates_secrets_aliases_and_defaults`                                                                                                                                                                                                                                                                                                                                                                                     | actual, offline                                        | pass (baseline)                                    |
| 2a  | Canary secret not visible via tested secret/settings introspection and secret-reading view definitions on a configured owner                                                                                                                                                                                                                                                              | new native proof (guarded-SQL section) + updated `concurrent_gcs_owners...` assertions                                                                                                                                                                                                                                                                                                                                                                                                 | source proof + shipping FFI signed synthetic, offline  | **parent pass**                                    |
| 2b  | No logger/profiling path attached to GCS requests; logging disabled at setup                                                                                                                                                                                                                                                                                                              | new native proof (profiling section: `EnableLogging::GetSetting` false, no logger in resolved params, profiling disabled on both fresh contexts)                                                                                                                                                                                                                                                                                                                                       | source (signed synthetic), offline                     | **pass**                                           |
| 2c  | No canary in any native error string across all refused paths                                                                                                                                                                                                                                                                                                                             | new native proof (error-scan section) + existing setup proof                                                                                                                                                                                                                                                                                                                                                                                                                           | source (signed synthetic), offline                     | **pass**                                           |
| 3a  | No synthetic bearer in durable DB/WAL bytes after configure + guarded queries                                                                                                                                                                                                                                                                                                             | updated concurrent-owner root-file scan + native proof required DB/optional WAL before/after-close scan + parent configured-receipt recursive scan of present owned files. No export was performed; do not infer absent artifacts were exercised                                                                                                                                                                                                                                       | source + shipping FFI, signed synthetic/offline        | **parent pass**                                    |
| 3b  | Generated projection/plan preserve exact URLs; actual configured empty view/receipt and closed artifacts exclude canary                                                                                                                                                                                                                                                                   | `gcs_view_sql_and_receipt_contain_exact_urls_not_credentials` is a pure **plan** shape test despite its historical name; parent `configured_gcs_empty_view_receipt_and_closed_artifacts_are_canary_free` installs the canary via shipping FFI, commits an actual empty view/receipt, independently resolves it after close and scans present owned artifacts                                                                                                                           | actual + shipping FFI, exact signed synthetic/no cloud | **parent pass**                                    |
| 4a  | Two independent owners hold distinct canaries in their own memory secret storage (no cross-copy)                                                                                                                                                                                                                                                                                          | new native proof (secret-value section: each owner's `memory`-storage secret value is exactly its own canary)                                                                                                                                                                                                                                                                                                                                                                          | source (signed synthetic), offline                     | **pass**                                           |
| 4b  | Stock secret resolution routes each in-scope URL to exactly its owner's canary; `x-goog-user-project` routing per owner                                                                                                                                                                                                                                                                   | new native proof (parameter-resolution section through real `InitializeParameters` in a transaction: in-scope URL resolves to the owner's canary + project header; cross-owner resolution resolves to no bearer)                                                                                                                                                                                                                                                                       | source (signed synthetic), offline                     | **pass**                                           |
| 4c  | Cross-scope request from owner B to owner A's scope is refused at the request boundary before any dispatch; no wrong-token request; error canary-free                                                                                                                                                                                                                                     | new native proof (transport-boundary section: in-scope dispatch per owner; cross-scope both directions, non-admitted, write, and expired all refused **before** dispatch with canary-free errors) + Rust cross-scope refusal in updated `concurrent_gcs_owners...`                                                                                                                                                                                                                     | source + shipping FFI signed synthetic, offline        | **parent pass**                                    |
| 4d  | Concurrent dispatch through the actual shipping wrapper boundary: two threads, barrier-synchronized, each exclusively owning its own params/state/mock/wrapper; the `grv::GCSHTTPPolicy::SendRequest` private-param-copy (zeroizing) boundary is exercised per owner; the original shared params are unchanged after both dispatches; no concurrent mutation of the global guard registry | new native proof (policy-wrapper section; the supplementary mock-stock/guard-registry section is labelled as a known revision-3 bypass, not a waiver)                                                                                                                                                                                                                                                                                                                                  | source (signed synthetic), offline                     | **pass**                                           |
| 5a  | Failure immediately after secret installation: `-1`, observed absence, guard/path/external-access restored, failed state, read admission refused, local BEGIN/ROLLBACK recovery                                                                                                                                                                                                           | existing ctest `duckdb_gcs_setup_fault_fence` (`native/gcs_setup_proof.cpp`) stage 1                                                                                                                                                                                                                                                                                                                                                                                                   | source (signed synthetic), offline                     | pass (baseline)                                    |
| 5b  | Failure after HTTP request policy installation: same proof at stage 2                                                                                                                                                                                                                                                                                                                     | existing setup proof stage 2                                                                                                                                                                                                                                                                                                                                                                                                                                                           | source (signed synthetic), offline                     | pass (baseline)                                    |
| 5c  | Failure after exact filesystem installation: same proof at stage 3                                                                                                                                                                                                                                                                                                                        | existing setup proof stage 3                                                                                                                                                                                                                                                                                                                                                                                                                                                           | source (signed synthetic), offline                     | pass (baseline)                                    |
| 5d  | Forced DROP failure keeps the secret and yields `-2` (unproved revocation), not reusable `-1`; inspection/preamble uncertainty also `-2`                                                                                                                                                                                                                                                  | existing setup proof drop/inspection/preamble scenarios                                                                                                                                                                                                                                                                                                                                                                                                                                | source (signed synthetic), offline                     | pass (baseline)                                    |
| 5e  | `-1` vs `-2` Rust-side semantics: `gcs_setup_outcome(-1)` returns `Err` **without** setting `owner_unusable` (proved absence and restoration: local work continues; only cloud reuse is denied by the failed state); only `-2`/unexpected status poisons operational reuse (metadata and ROLLBACK refused) until synchronous owner-thread close/reopen under retained ownership           | new `native::descriptor_tests::in_process_proved_setup_failure_keeps_owner_usable_for_local_work` (branches on the actual FFI status; in this environment a missing signed extension yields `-2` because the pre-`LOAD` guard removal cannot be re-proved, so the poisoning branch executed and close/reopen recovery was asserted) + existing `in_process_failed_setup_quarantines...` (explicit `-2` poisoning) + the ctest setup proof (native-level `-1` proven-absence semantics) | actual, offline                                        | **pass**                                           |
| 5f  | No false effect absence: failed setup never reports a committed/absent effect; recovery only after close/reopen                                                                                                                                                                                                                                                                           | existing setup proof (BEGIN/ROLLBACK recovery) + existing crash-receipt tests                                                                                                                                                                                                                                                                                                                                                                                                          | source/actual, offline                                 | pass (baseline)                                    |
| 6a  | Configuration/acquisition never renews the original lease: `min(original, token)` only shortens; a later, longer token deadline cannot extend it; second configure on a failed owner is refused                                                                                                                                                                                           | new `native::descriptor_tests::configure_with_longer_token_deadline_never_renews_original_lease` (deadline arithmetic through the real `configure_gcs_reader` path; original interrupt clone preserved; fresh invocation resets) + existing `in_process_original_lease_only_shortens_and_discovery_uses_same_connection`                                                                                                                                                               | actual, offline                                        | **pass**                                           |
| 7   | Real fresh/concurrent OAuth with the selected identity                                                                                                                                                                                                                                                                                                                                    | new ignored `gcs_token::tests::explicitly_selected_fresh_and_concurrent_acquisition_is_independent_per_invocation` (two sequential + two concurrent `acquire` of the explicitly selected account/project; asserts `ensure_live()` and non-empty bearer only; no token value/digest/Debug output; no automatic retry) + existing ignored `explicitly_selected_gcloud_returns_private_token_with_expiry`; driver below                                                                   | actual selected provider, parent direct prebuilt       | **retry: both gates pass; prior failure retained** |

## Source-proof vs shipping-binary proof

- The new native proof (`native/gcs_oauth_isolation_proof.cpp`, target
  `grv_gcs_oauth_isolation_proof`) **source-includes** `shim.cpp`
  (guard/engine/FFI definitions) and links the pinned static libraries. It is
  a **static/source proof of the revision6 code as written**, run against the
  signed stock HTTPFS extension for real secret resolution. It is not the
  shipping dylib and does not re-qualify
  `libgrv_duckdb_guard.dylib` (`2709cc61...`), which remains byte-identical
  after this package (re-hashed below).
- The policy-wrapper section of the proof exercises the **actual shipping
  wrapper** `grv::GCSHTTPPolicy::SendRequest` private-parameter-copy
  boundary (the zeroizing `PrivateParams` copy that the patched stock
  `HTTPUtil::Request` hands to the client) for both owners, in-scope and
  cross-scope. The mock-stock/guard-registry section is retained and
  labelled **supplementary** (known revision-3 bypass of the wrapper); it is
  not the basis of the 4d pass.
- The signed-extension Rust tests (ignored, parent-runnable) exercise the
  same setup path through the **shipping library** FFI; they are the
  shipping-binary proof class for the cases they cover; parent independently ran the distinct-owner and actual
  configured receipt tests on the final prebuilt library-test artifact.
- The mock-transport section uses a recording client; the signed stock
  client's own `SendRequest` internals are not exercised there. That boundary
  is labelled, not waived: it matches the existing
  `duckdb_gcs_request_deadline_exact_read_fence` proof pattern.

## Canary and secret-copy limitations

- Canary proofs bound **newly observable** copies: helper stdout capture,
  error strings, view/receipt/durable bytes, secret storage values, request
  parameters and the request boundary. They do not prove erasure of every
  heap/stock allocation: DuckDB's secret storage, the pinned HTTPFS
  `HTTPFSParams::bearer_token` copies, TLS/httplib internal buffers and
  already-dispatched request state may retain bytes past the lease. Accepted
  best-effort wipes (`GCSPrivateString`, `PrivateParams`, `Zeroizing`
  buffers) and the temporary-secret lifetime bound the _new_ exposure, not
  historical memory.
- The post-close owned-artifact scans (native proof and the Rust
  concurrent-owner test) cover every file in the owned workspace after both
  owners close/flush. No preconfiguration file scan or export is inferred from
  source/artifact baseline hashes. These are byte-level scans of present durable
  files only; no heap-wipe claim is made, and
  WAL files that were never created (ENOENT) are the only permitted absence.
  Any other stat/read failure on an owned artifact fails the proof.
- The temporary secret is dropped and its absence positively observed in
  failed-setup paths; a forced DROP failure yields `-2` (owner fence), never
  a reusable `-1`. No universal credential-copy erasure is claimed.
- No token hash, digest or durable bearer copy is created by any test in this
  package; the real-token driver below prints nothing.

## Actual-auth gate: failure preserved, authorized retry passed

Parent ran the single-acquisition test directly against the reviewed prebuilt
binary with explicit account `jiacao@on.sinch.com` / project `entit-finance-prod`.
`~/grv-gcs-oauth-private-zlq4s1fn` contains private result/summary and three
matching pre/post artifact hashes. Exit101, generic explicit OAuth acquisition
or lifetime validation failure. **Concurrent test not launched.** No token values,
digests, raw helper output, cloud objects/buckets or IAM changes were recorded.

The exact read-only config preflight subsequently succeeded and found an auth
object with no prohibited nonempty selectors. Existing-log classification did not
identify a cause; do not label reauthentication as proven. Operator task52 must
resolve/confirm this selected provider state, then authorize an explicit retry.
No automatic acquisition retry or alternative identity was used on failure.

After the user reported restored access, parent retried explicitly using the
same selected account/project and direct prebuilt test binary. Both gates pass
at `~/grv-gcs-oauth-private-5lu6gd8k`: one single acquisition, followed by two
sequential fresh and two concurrent acquisitions, each via the existing private
bounded provider. Provider may return the same live token; distinct values are
not asserted or recorded. Three pre/post artifact hashes match. No token value,
digest, raw helper output, cloud object/bucket or IAM change is recorded. This
success resolves task52 and the required real gate; it does not retrospectively
classify the earlier failure. Package23 is complete; next package is separately
launched with one subagent at a time.

Subagent performed no actual cloud/provider acquisition. Two minimal existing drivers are
ignored tests in `src/gcs_token.rs`, using the existing `acquire` provider
only:

1. `gcs_token::tests::explicitly_selected_gcloud_returns_private_token_with_expiry`
   (single fresh acquisition), and
2. `gcs_token::tests::explicitly_selected_fresh_and_concurrent_acquisition_is_independent_per_invocation`
   (two sequential + two concurrent acquisitions of the same selected
   identity, each in its own private bounded helper process; no comparison of
   token values, since the provider may legitimately return the same live
   token; a reauthentication mismatch stops and never auto-retries).

```bash
cd /Users/jiacao/projects/grv
export CARGO_TARGET_DIR=/Users/jiacao/.grv-native-work/owner-validation-target
export GRV_GCS_ACCOUNT=jiacao@on.sinch.com
export GRV_GCS_PROJECT=entit-finance-prod   # project 828148855924
cargo test -p grv-adapter-duckdb --features native --lib \
  gcs_token::tests::explicitly_selected_gcloud_returns_private_token_with_expiry \
  -- --exact --ignored --test-threads=1
cargo test -p grv-adapter-duckdb --features native --lib \
  gcs_token::tests::explicitly_selected_fresh_and_concurrent_acquisition_is_independent_per_invocation \
  -- --exact --ignored --test-threads=1
```

Guards: the tests assert only `ensure_live()` and a non-empty bearer; the
`AccessToken` Debug is redacted, the helper runs in a private process group
with null stderr and bounded stdout, no token argv/env, and no durable copy.
A real token refresh is existing-provider use, not a grant for broader cloud
testing (no bucket, object, read, fallback or new credential). Parent stopped on the first failure, then ran the authorized retry only after
user-restored access; row7 now passes.

## Validation (this package)

- Environment: `CARGO_TARGET_DIR=/Users/jiacao/.grv-native-work/owner-validation-target`,
  `GRV_DUCKDB_NATIVE_LIB_DIR=/Users/jiacao/.grv-native-work/cmake`,
  `GRV_DUCKDB_EXTENSIONS_DIR=/Users/jiacao/.grv-httpfs-prototype-extensions/osx_arm64`
  (also passed to the cmake configure as `-DGRV_DUCKDB_EXTENSIONS_DIR=...`
  for ctest registration; the CMake default is empty),
  `GRV_GCS_STOCK_PROOF_BIN=/Users/jiacao/.grv-native-work/cmake/grv_gcs_stock_request_proof`.
  Native/conformance targets run serially; default-ignored live tests are
  never run as a list; exact ignored signed synthetic tests only individually
  after reading what they do.
- Baseline logs: `/tmp/grv-task23-baseline-*.txt` (counts above).
- After-logs (this pass):
  - `/tmp/grv-task23-stage2-lib-final.txt` — native lib
    **116 passed / 0 failed / 13 ignored** (baseline 109/0/12; +7 passing,
    +1 ignored).
  - `/tmp/grv-task23-stage2-integration.txt` — integration **48 passed /
    0 failed** (unchanged from baseline).
  - `/tmp/grv-task23-stage2-sdk.txt` — SDK lib **18 passed / 0 failed**,
    runtime **3 passed / 0 failed** (unchanged).
  - `/tmp/grv-task23-stage2-ctest.txt` — ctest **14/14 passed** (13
    baseline + new `duckdb_gcs_oauth_isolation_fence`, `TIMEOUT 120`).
  - `/tmp/grv-task23-stage2-lint.txt` — strict Clippy `-D warnings`
    (native and non-native, `--all-targets`) **clean**; `cargo fmt --check`
    **clean** on `grv-adapter-duckdb` and `grv-adapter-sdk`.
- Shipping dylib re-hashed after all work:
  `2709cc619eb125d1ecdc48377303019ea20bfe32f20c4e964c298b2b66abdd5f` —
  **unchanged**; no shipping-library rebuild occurred.
- Parent final stage: **116 native library/14 ignored +48 integration**,
  **18 SDK library +3 runtime**, **14/14 ctest**, strict native all-targets
  Clippy and fmt/diff pass. Logs `/tmp/grv-task23-parent-final-tests.txt`,
  `/tmp/grv-task23-parent-sdk.txt`, `/tmp/grv-task23-parent-ctest.txt`,
  `/tmp/grv-task23-parent-clippy.txt`, `/tmp/grv-task23-parent-fmt.txt`.
  Two exact signed no-cloud tests independently pass again on the final artifact:
  configured receipt1.76seconds and distinct-owner isolation4.56seconds;
  `/tmp/grv-task23-parent-final-receipt.txt`,
  `/tmp/grv-task23-parent-final-owners.txt`. Earlier1.59/4.62second logs retained.
- No dirty-file-count equivalence or purely additive assertion is inferred:
  the concurrent-owner test was rewritten. Parent baseline-diff comparison
  identifies the exact scoped code blocks changed and verifies none were
  removed outside this package. Production execution is unchanged.
- Subagent stage1 build/run failures were reported during development; some
  run log paths were reused by the subagent. The agent transcript retains those
  tool results; do not invent separate preserved-file hashes for overwritten
  failed logs. Parent corrections are recorded above, not silently credited to
  the earlier stage.

## Open items (precise)

1. **Row7 resolved.** Parent-authorized retry after user-restored access passes
   both gates with stable artifacts. Prior failure cause remains unclassified;
   no fallback, automatic retry or broader cloud authority. Package23 is closed
   on the documented source/mock/shipping-FFI/real-provider evidence scopes.
2. Real in-flight stock-HTTP overrun, broader cloud fault coverage and the
   retained-handle full isolation matrix (#32) remain separate gates; nothing
   here claims them.
3. The 250 ms → 3792 ms header-trickle counterexample and the accepted
   cooperative-cancellation limitation stand unchanged.
4. No release traceability register refresh; no publication; no commit.
