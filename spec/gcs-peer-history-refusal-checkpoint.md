# GCS peer history refusal checkpoint (task 24)

Status: package 24 completed after parent code review, corrections and
independent validation (2026-10-13). This is finite, scoped end-to-end
stale-peer/history acceptance for pull, with the existing closed build-record
and wire/host slices retained; not exhaustive peer/security or release approval.
No wire or binding version bump, no migration, no reset,
no dual-version negotiation, no new capability, no new default, and no
harder contract were added. The shipping dylib
`2709cc619eb125d1ecdc48377303019ea20bfe32f20c4e964c298b2b66abdd5f` remains
unchanged. No production fix was required or made.

## Scope

- The actual CLI (`grv pull`) keeps its intentional order: offline
  `locate_connection` -> `bind_connection` -> `pull::lookup` run before any
  current-defaults/registry validation or destination authentication. A
  found receipt replays source-free; only an absent trusted history falls
  through to current validation. This order is preserved, not reordered.
- New coverage extends the existing bounded
  `fixture-stale-pull` offline fixture with private, per-process, file-backed
  fixture state (no public wire flag, no production default change):
  prior-bound identity/workspace, a genuine recorded committed receipt,
  unknown durable history, and terminal-journal evidence.
- Modeled-fixture cases (this file, CLI layer) are paired with
  actual-native evidence (DuckDB `PullStore` against a real local database
  holding a GCS root) for the same history shapes. The two are labeled
  separately in the matrix below.
- Closed peer identity/wire refusal (host handshake, wire inline/chunked
  access decoding, API materialization decoding) is re-verified by rerunning
  the existing typed slices; no new wire fields or versions were invented.

## Finite matrix

| #   | Case (layer)                                                                                                                  | Expected outcome                                                                                                 |
| --- | ----------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| 1   | Stale options enum (local/S3), Uninitialized identity, gcs-view declaration (CLI, existing)                                   | INVALID_DECLARATION before auth; no mutation                                                                     |
| 2   | Stale options enum, prior-bound identity+workspace, gcs-view declaration (CLI, new)                                           | INVALID_DECLARATION before auth/prepare/apply/journal; probe markers present; owned tree bytes/entries unchanged |
| 3   | Valid matching committed historical GCS receipt, current options stale, gs:// root (CLI, new)                                 | success `replayed: true`, source-free; no auth/prepare/apply/journal; no cloud contact                           |
| 4   | Terminal journal with stored receipt, destination lookup NotCommitted (CLI, new)                                              | OUTCOME_UNKNOWN before auth/effects                                                                              |
| 5   | Terminal journal with stored receipt, destination lookup Unknown (CLI, new)                                                   | OUTCOME_UNKNOWN before auth/effects                                                                              |
| 6   | Prior-bound journal intent with different root (CLI, new)                                                                     | REQUEST_MISMATCH before session spawn/auth                                                                       |
| 7   | Prior-bound journal intent, matching root, different workspace/identity (CLI, new)                                            | REQUEST_MISMATCH before compare/auth/effects                                                                     |
| 8   | Unknown durable history from destination (CLI, new)                                                                           | OUTCOME_UNKNOWN before auth/effects                                                                              |
| 9   | Actual native: GCS stored receipt under offline local database, receipt emptied (native, new; NOT NULL schema)                | OutcomeUnknown; no repair/auth; database+witness unchanged                                                       |
| 10  | Actual native: stored GCS binding root/workspace mismatched vs query (native, new)                                            | StateConflict; no repair/auth; database+witness unchanged                                                        |
| 11  | Host manifest/handshake identity, interface array, binding schema, no-common-offer (host, existing rerun)                     | refusal before Ready/dispatch                                                                                    |
| 12  | Wire inline + chunked unknown materialization/access values (wire, existing rerun)                                            | Codec/typed refusal before dispatch                                                                              |
| 13  | API materialization decode known/unknown values (api, existing rerun)                                                         | typed decode refusal                                                                                             |
| 14  | Native incompatible GCS durable receipt/header (wrong mode/field/header0/ownership0/package/binding) (native, existing rerun) | OutcomeUnknown/RequestMismatch; no repair/auth                                                                   |

Local/S3 valid current cases remain covered by the existing passing slices;
no S3/cloud matrix was started.

## Results

All matrix rows pass. Modeled-fixture rows (2-8c) run the actual CLI binary
against the bounded offline fixture; actual-native rows (9-10, 14) run the
real DuckDB `PullStore` against a real local database holding a GCS root.
The two are labeled separately; the fixture models wire-level history while
the native rows prove the same history shapes on closed typed storage.

| #   | Test                                                                                                                                                                                                                                      | Result                                                                                                                                                                                                                                                                                   |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `agreeing_peer_stale_gcs_options_refuse_after_offline_probe_before_auth_or_mutation` (existing; Uninitialized identity, gcs-view, stale options enum)                                                                                     | pass: INVALID_DECLARATION before auth; no forbidden marker; owned tree bytes/entries unchanged; direct host authenticate control fires the marker                                                                                                                                        |
| 2   | `prior_bound_stale_gcs_options_refuse_before_auth_or_mutation` (new; prior-bound identity+workspace)                                                                                                                                      | pass: INVALID_DECLARATION; audit dir holds exactly `probe-lookup` (probe ran, compare/prepare/apply never ran); no forbidden marker; owned tree and private inputs unchanged; no journal created                                                                                         |
| 3   | `committed_historical_gcs_receipt_replays_source_free_despite_stale_current_options` (new; genuine recorded request+receipt built with the full registry and production digests; gs:// root)                                              | pass: success with `replayed: true`, `committed_revision "1"`, recorded workspace; gs root parsed lazily without storage auth; no journal, no auth/prepare/apply marker; audit holds exactly `probe-lookup`+`probe-compare`; owned tree and private inputs unchanged                     |
| 3b  | `committed_historical_gcs_receipt_replays_with_recorded_journal_terminal_completion` (new; same replay with a pre-existing recorded journal)                                                                                              | pass: success `replayed: true`; the only owned-tree change is the journal file completing its terminal (generation 2, intent immutable); a second replay advances the existing journal CAS to generation 3 with identical intent/terminal, no destination effects; no auth/effect marker |
| 4   | `terminal_journal_without_stored_receipt_is_outcome_unknown` (new)                                                                                                                                                                        | pass: OUTCOME_UNKNOWN before auth/effects; audit exactly `probe-lookup`; journal immutable                                                                                                                                                                                               |
| 5   | `terminal_journal_with_unknown_lookup_is_outcome_unknown` (new)                                                                                                                                                                           | pass: OUTCOME_UNKNOWN; audit exactly `probe-lookup`; journal immutable                                                                                                                                                                                                                   |
| 6   | `prior_bound_journal_root_mismatch_refuses_before_session_spawn` (new)                                                                                                                                                                    | pass: REQUEST_MISMATCH at the journal identity check before session spawn; audit empty (no probe); a separately positive-tested executable startup marker remains absent, proving the peer was not launched; no forbidden marker                                                         |
| 7a  | `prior_bound_journal_workspace_mismatch_refuses_at_bind` (new)                                                                                                                                                                            | pass: REQUEST_MISMATCH at the host bind check (expected workspace vs bound workspace); audit empty; no forbidden marker                                                                                                                                                                  |
| 7b  | `prior_bound_journal_identity_mismatch_refuses_before_compare` (new; recorded identity differs, current materialization passes the stale enum)                                                                                            | pass: REQUEST_MISMATCH at the fixed-request comparison after the lookup probe and before compare/auth; audit exactly `probe-lookup`                                                                                                                                                      |
| 8   | `unknown_history_is_outcome_unknown_before_auth` (new)                                                                                                                                                                                    | pass: OUTCOME_UNKNOWN; audit exactly `probe-lookup`; no forbidden marker                                                                                                                                                                                                                 |
| 8b  | `malformed_stored_receipt_is_outcome_unknown_not_absence` (new; parent final three sub-cases: unknown top-level field, corrupted request digest, unknown result materialization)                                                          | pass: all three sub-cases OUTCOME_UNKNOWN before auth; present malformed history is never reinterpreted as absence; audit exactly `probe-lookup` each run                                                                                                                                |
| 8c  | `invalid_private_history_entries_refuse_instead_of_clean_absence` (parent; nine owned-fixture faults: missing/unknown binding, bad workspace, missing identity, dangling binding/receipt/audit, directory receipt, invalid UTF-8 receipt) | pass: OUTCOME_UNKNOWN, no auth/effects, protected inputs unchanged; links inventoried without traversal                                                                                                                                                                                  |
| 9   | `gcs_stored_history_loss_or_mismatch_refuses_without_repair_or_auth` (new native; genuine GCS receipt under offline local database, then receipt slot erased)                                                                             | pass: OutcomeUnknown; database+witness bytes unchanged; no GCS read configuration created; no token                                                                                                                                                                                      |
| 10  | same native test, binding-root and binding-workspace faults                                                                                                                                                                               | pass: StateConflict; database+witness bytes unchanged; no GCS read configuration                                                                                                                                                                                                         |
| 11  | host manifest/handshake/offer/schema slices (existing rerun)                                                                                                                                                                              | pass: 22/22 in `tests/host.rs`                                                                                                                                                                                                                                                           |
| 12  | wire inline + chunked unknown materialization/access (existing rerun)                                                                                                                                                                     | pass: 20/20 protocol + 5/5 wire lib                                                                                                                                                                                                                                                      |
| 13  | API materialization decode (existing rerun)                                                                                                                                                                                               | pass: 10/10 build + 7/7 api lib                                                                                                                                                                                                                                                          |
| 14  | native incompatible GCS durable receipt/header (existing rerun, inside the 117)                                                                                                                                                           | pass                                                                                                                                                                                                                                                                                     |

Fixture semantics added for these rows (no public wire flag, no production
default change): a directory coordinate containing any controlled state file
enters private fixture state; the `binding` marker must be a closed value and
a `bound` state requires a valid workspace UUID; present-but-unreadable or
invalid state fails closed as OutcomeUnknown, never Uninitialized or absence.
A present `receipt.json` that is unreadable or not a valid closed typed
record (including a registry that fails closed checks or an adapter result
failing the recorded registry's `pull_result` point) is Unknown history.
Parent review replaced `exists()`/`is_file()` absence checks with checked
`symlink_metadata`: only genuine ENOENT is absence; links, nonregular entries
and inventory/read errors refuse. This is owned-fixture validation, not an
adversarial race-safe opener. Every new CLI invocation now snapshots private
history inputs as well as package/root/state/canary files. Snapshot entries
retain file/directory/link type, with link destinations recorded but not read;
audit enumeration failures cannot become a vacuous empty set.

Probe evidence is written only to a dedicated audit directory selected by an
owned fixture metadata file, outside every protected snapshot; a failed audit
write fails the invocation. The fixture's Committed resolution is accepted by
the CLI only after the host's own typed `validate_for` consistency checks
(attempt/root/request) and recorded-registry receipt validation.

## Validation

Environment: `CARGO_TARGET_DIR=/Users/jiacao/.grv-native-work/owner-validation-target`,
`GRV_DUCKDB_NATIVE_LIB_DIR=/Users/jiacao/.grv-native-work/cmake`,
`GRV_DUCKDB_EXTENSIONS_DIR=/Users/jiacao/.grv-httpfs-prototype-extensions/osx_arm64`.
Serial per target, `--test-threads=1` for native/conformance. No ignored
live tests, real provider acquisition, cloud resources, gcloud, held or
package-32 activity in this package. CTest includes the existing source-including
signed-extension LOAD/mock-transport proof; it is not a new real-cloud or
shipping-FFI transport qualification. Baseline before edits: native lib 116 passed / 14 ignored;
conformance `peer_refusal_cli` 1 passed; tracked diff and untracked hashes
captured.

- Native lib: **117 passed / 0 failed / 14 ignored** (baseline 116/14;
  +1 `gcs_stored_history_loss_or_mismatch_refuses_without_repair_or_auth`).
- Conformance (parent full suite): **88 passed / 0 failed / 13 ignored**, including
  `peer_refusal_cli` **12 passed** (1 preexisting + 10 subagent + 1 parent), `host.rs` 22,
  pull_cli 2, push_cli 1, recover_cli 5, session_cli 4, and the remaining
  suites; ignored entries are all live/cloud-gated.
- Wire: 5 lib + 20 protocol passed. API: 7 lib + 10 build passed.
  SDK: 18 lib + 3 tests passed. DuckDB integration: 48 passed (22+2+9+4+7+1+2+1).
- CTest: subagent **14/14 passed**, unchanged proof artifacts; scoped as above.
- Parent independently reran native 117 + integration 48, the full conformance
  suite, and the final exact CLI suite after a Clippy-only correction. Wire/API/
  SDK counts above are subagent reruns of unchanged sources, not duplicate
  parent runs.
- Strict Clippy `-D warnings --all-targets` clean for `grv-adapter-duckdb`
  (native and non-native) and `grv-conformance` (native-duckdb and
  non-native); `cargo fmt --check` clean on both crates.
- Code diff vs baseline: only `crates/grv-adapter-duckdb/src/pull.rs`
  changed among tracked code (95 added test lines, zero baseline lines removed);
  `spec/README.md` also gained scoped checkpoint links/status corrections.
  `parent-preservation-and-hashes.json` verifies all unrelated baseline content
  remains unchanged.
  The two conformance files were **preexisting untracked files**, extended here;
  the new checkpoint and stage JSON are additional untracked files. No baseline
  tracked blocks or unrelated untracked file content were removed.
- Shipping dylib re-hashed after all work:
  `2709cc619eb125d1ecdc48377303019ea20bfe32f20c4e964c298b2b66abdd5f`
  (`libgrv_duckdb_guard.dylib`) - **unchanged**; no shipping-library rebuild
  occurred. No production fix was required or made.

Logs: `/tmp/grv-task24/baseline-native-lib.txt`,
`/tmp/grv-task24/baseline-tracked.diff`,
`/tmp/grv-task24/baseline-untracked.sha256`,
`/tmp/grv-task24/stage2-{cli,conformance,native-lib,wire,api,sdk,duckdb-integration,ctest,clippy-native,clippy-nonnative,clippy-conformance-native,clippy-conformance-nonnative,fmt}.txt`,
`/tmp/grv-task24/current-tracked.diff`,
`/tmp/grv-task24/current-untracked.sha256` (subagent stage).

Parent logs: `parent-native.txt` (117 + 48), `parent-conformance.txt` (88),
`parent-final-cli.txt` (12), `parent-native-clippy.txt`,
`parent-final-conformance-clippy.txt`,
`parent-final-conformance-nonnative-clippy.txt`,
`parent-final-native-nonnative-clippy.txt`, `parent-fmt-check.txt`,
`parent-diff-check.txt`, all under `/tmp/grv-task24/`.
`parent-conformance-clippy.txt` retains the development failure for a needless
borrow in the stricter audit helper; corrected without changing behavior or
weakening assertions, then rerun passing. Historical agent-stage source hashes
remain stage-scoped in the JSON; the reviewed parent stage has separate current
source/log/artifact hashes. No release register refresh or capability promotion.
