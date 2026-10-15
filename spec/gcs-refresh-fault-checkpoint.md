# GCS refresh, source faults and nonempty COMMIT recovery — package25

**Status: complete within the finite parent-reviewed matrix below.** This does
not close broader package45, held lifecycle27, remaining S326, aggregate/install/
platform/release qualification or publication. `gcs-view` remains unadvertised.

## Reviewed changes and unchanged production

Tests added only in `crates/grv-adapter-duckdb/src/pull.rs`; `src/native.rs`
adds a cfg(test)-only bridge to the existing publication-expiry hook. No
production execution, protocol/default/capability, allocator/helper/transport
change. Shipping native guard unchanged SHA256
`2709cc619eb125d1ecdc48377303019ea20bfe32f20c4e964c298b2b66abdd5f`.

Parent read actual source and corrected partial/unapplied Qwen/xhigh outputs;
reports were not acceptance. One agent at a time. Baseline
`/tmp/grv-task25/baseline/` preserved. `parent-preservation.json`:79/81 existing
touched files unchanged; only scoped native.rs/pull.rs changed. No reset,
unrelated reformat, commit, push, publication or release-register refresh.

Parent corrections include eight initial compile/move/shadowing errors, invalid
hex digest, placeholder generations, private/tempdir/fixture gates, unintended
reader configuration on source-free replay, receipt field assertion, atomic
ready-marker publication, non-vacuous source-loss relation admission and
child-guard try_wait/timeout/reap semantics. Existing exact-value assertion
checks representability, **not declared value equality or remote integrity**.
Expiry hook fires on the first successful metadata-FFI result in resolve, not
after file1 or view creation. These corrected boundaries are asserted honestly.

## Final finite acceptance matrix

| Dimension                      | Actual acceptance                                                                                                                                                                                                                                                                       | Boundary                                                                                                                         |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Multi-file pre-dispatch cancel | Two-file GcsView plan validates; stopped owner never dispatches; actual joined WorkspaceLock release; second dispatch refuses; fresh resolution is genuine Uninitialized.                                                                                                               | Offline actual Rust worker/native workspace, no provider.                                                                        |
| Publication expiry             | Committed empty GcsView baseline, new nonempty multi-file attempt: first metadata result refused at expiry before BEGIN/source dispatch. Close/fresh activation preserves prior receipt and empty view; initialized attempted identity is legitimately NotCommitted.                    | Test-only existing expiry hook, not native rollback-at-expiry or source1 expiry.                                                 |
| Fixture/audit admission        | Private owner/mode/regular NOFOLLOW bounded stable input; closed fields/selectors/root/roles/generations, duplicate/malformed/unknown/link/permission/absence refusal; typed stage acknowledgements.                                                                                    | Offline negative tests; not adversarial race-safe whole-host guarantee.                                                          |
| Harness child ownership        | Normal exit86 propagated; try_wait errors refuse; timeout stays error; owned child killed/waited by RAII; reaped child not killed again.                                                                                                                                                | Offline test child only, not shipping hard native stop.                                                                          |
| Nonempty refresh               | Actual two-file111/222 apply and new-attempt refresh, count2 receipts; older receipt replay is immutable and does not rewind.                                                                                                                                                           | Shipping FFI/signed HTTPFS/real HTTPS, below full CLI preparation.                                                               |
| Evolution/special coordinates  | Version1 shorter file + version2 id/extra yield `(111,NULL),(333,'new')`; special key space/plus/percent/Unicode/apostrophe executes; canonical gs spelling translated once. Same-version schema inconsistency refuses preflight.                                                       | Exact casts/null padding/positional UNION, no ordinary UNION coercion myth.                                                      |
| Source-free historical replay  | Fresh owner with no reader/provider resolves/replays all earlier receipts; latest view metadata persists.                                                                                                                                                                               | No new source query/auth on the lookup owner.                                                                                    |
| Nonempty pre/post COMMIT exit  | Two isolated actual GCS applies reach existing exit86 hooks before/after COMMIT. Fresh source-free outcomes distinguish NotCommitted/Committed; aftercommit count2 and authenticated111/222 oracle; later empty checkpoint advances while historical receipt stays immutable/no rewind. | Specific process-exit boundary, not universal abrupt kill/WAL/receipt-race proof. Fresh inspection relation explicitly admitted. |
| Missing primary                | Exact original generation deleted only after native owner close/lock release; fresh configured attempt fails; original receipt remains Committed and new failed identity legitimately NotCommitted.                                                                                     | Not merely late missing second-target historical evidence.                                                                       |
| Same-key replacement           | Replacement84 new generation after delete. Stale original size/SHA/generation descriptor applies and sees84; new known descriptor/request commits new receipt; original replay does not rewind84.                                                                                       | **Observable privileged replacement limitation, not prevention or per-range generation pinning.**                                |
| Source loss inspection         | Delete all known generations, complete empty inventory ack; fresh configured and relation-admitted view read fails, not silent empty. Source-free immutable receipts still replay; new attempt fails/NotCommitted without repair/filtering.                                             | Fresh failed owners are closed, not operationally reused; preexisting unadmitted guard cannot make negative assertion vacuous.   |

Prior23 OAuth,24 malformed/stale history,32 scanner epoch/retained handles and34
cooperative cancel/uncertain COMMIT proof remain separate accepted dimensions.
Their scopes are reused by reference, not falsely declared new/unimplemented.
Full SHA/size/schema/row preparation remains mandatory upstream. Holds/prepared
integrity cannot prevent privileged later replacement; stock requests are not
per-range generation-pinned. Synthetic or source-including evidence is not a
substitute for this actual HTTPS stage, nor does it imply full CLI held coverage.

## Independent final offline validation

Logs `/tmp/grv-task25/`:

- `parent-final-native.txt`, after admission correction `parent-admission-native.txt`:
  **121 passed /20 ignored +48 integration**.
- `parent-final-conformance.txt`: **22 host +12 peer CLI**.
- `parent-final-ctest.txt`: **15/15**; no shipping guard rebuild.
- Strict native/non-native Clippy, fmt and diff checks pass; admission correction
  reruns native Clippy and fmt/diff without assertion weakening.
- Driver `parent-final-driver-canonical.txt`: **13 offline tests** (initial12
  separately retained), Python compile and default offline-plan pass.

No ignored cloud matrix was run by Cargo. No build/edit or native/conformance
run overlapped a live lifecycle. Each stage used a new protected freeze and
exclusively owned bucket; package32 hashes were not silently reused.

## Actual final live execution and cleanup

Reviewed plan frozen before acquisition: `spec/gcs-refresh-fault-live-plan.md`.
Driver `/tmp/grv-task25-live-driver.py`, freeze
`/tmp/grv-task25/live-freeze-admission/artifact-hashes.json`.

Final evidence `/Users/jiacao/grv-task25-live-47dmnvxc`:

- Exact `native_live_gcs_multifile_evolution_special_coordinate_refresh_and_replay`:
  **1 passed /14.91s**.
- Exact `native_live_gcs_nonempty_precommit_and_postcommit_exit_recover_immutable_receipts`:
  **1 passed /14.11s**, includes both owned child exit86 observations.
- Exact `native_live_gcs_missing_replaced_source_loss_preserves_immutable_receipts`:
  **1 passed /32.30s**, all three typed destructive audit barriers.
- All **11 frozen named artifact hashes pre/post/current equal**, independently
  verified by parent. Includes current binary, unchanged guard, both pinned
  extensions, manifest, driver/plan and relevant sources.
- Audit strictly contains six ready/ack records outside immutable fixture inputs;
  fixture digest preserved across stages. Atomic flush/publication before reads.
- Bucket `grv-gcs-pull-1754ee453cbe4f04afbb725d`: **5** known generation-qualified
  deletions (four initial objects plus replacement), complete observed empty
  all-version inventory, bucket deletion and observed404. No keys or existing
  IAM/ACL/policy/versioning changes; ownership project828148855924 verified.
- `summary.json`: all three exact-one-test passes, no execution/cleanup error.

Credentials explicitly selected `jiacao@on.sinch.com` / `entit-finance-prod`,
fresh independent acquisition per owner execution; token memory-only, no argv/
env/raw helper/provider output/digest. REST fixed verified-TLS host with no
proxy/redirect/retry; shipping exact read-only reader unchanged. Initial new-
bucket-only scaffold settings disclosed in frozen plan, no production reads.

## Retained development stages (not erased or promoted)

- Agent compile failures `parent-phaseA-first-build.txt` (8 errors), parent wrong
  receipt-field compile `parent-phaseA-native.txt`, private test directory failure
  `parent-phaseA-native-final.txt`, Clippy character-iteration failure
  `parent-phaseA-clippy-native.txt`, phaseB missing Path import
  `parent-phaseB-first.txt`; fixes/actual passing reruns separately logged.
- First live `/Users/jiacao/grv-task25-live-_chfl9_q`: canonical fixture refusal
  before reader/native apply due driver generic `%2B/%27` encoding (GRV requires
  raw printable plus/apostrophe). No crash/fault launch. Four conditional deletes,
  observed empty inventory/404;11 artifact pre/post equal.
- Second live `/Users/jiacao/grv-task25-live-h87rl16n`: positive test1pass15.37s;
  crash test reached both exit86 boundaries/receipt recovery but authenticated
  reopened-view oracle failed due missing **test** guard relation admission.
  Fault not launched. Fixed explicit relation admission, not guard weakening.
  Four deletes, empty inventory/404;11 pre/post equal.
- Final corrected new fixture/stage passes above. No effects retry on old
  resources, invented failure explanation, historical digest or no-effect claim.

## Carried limits / next packages

Cooperative supported interrupt/pre-dispatch/late-publication refusal only;
no hard native stop/join/rollback-at-expiry or finite stock HTTP overrun.
Test-process watchdog is harness containment, not shipping cancellation.
No universal credential heap erasure, aggregateRSS, arbitrary escaped-owner
lifetime/source-binary correspondence or universal crash-race proof. Privileged
replacement is explicitly observed and remains possible. Published held terminal
cleanup/expiry27 and S326 remain open. Evidence catalog matching is not release
readiness; source-change refusal remains and publication needs separate approval.
