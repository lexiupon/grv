# macOS ARM64 0.1.0 critical safety review

Current status: scoped safety assertions reviewed and targeted gaps closed;
final frozen-candidate rerun remains under task18, not publication approval. Exact contracts remain authoritative; independent tests are not
composed into a stronger integrated fault claim. Full scenario mappings are
still being reviewed in scenarios.json.

## Inspected exact selectors

Latest native-enabled workspace run: 510 passed, 22 ignored; record
9b253fd3-7276-47f3-9eb4-c03d3f426e07 in ignored artifacts/release-validation/
0.1.0-prep/runs. Pre/post snapshot differs only in the new GCS Python harness,
not Rust code during this run. Thus useful regression evidence, not frozen
source qualification. Earlier workspace log independently contains the below
selectors. Run every candidate's applicable native suite again after freeze.

| Package/target | Exact selector | Inspected assertion / limit |
|---|---|---|
| grv-core lib | publication::tests::publication_commits_empty_and_canonical_runs_then_explicit_omission | Empty/canonical run commits, explicit omission yields empty next revision. Does not alone prove failed captures cannot omit. |
| grv-core lib | publication::tests::lost_commit_ack_is_adopted_or_resolved_by_chain_and_unapplied_commit_is_fenced | Applied ambiguous commit resolves existing identity; unapplied remains uncommitted and fresh publication skips reserved number. Local fault backend, not delayed cloud request. |
| grv-core lib | publication::tests::known_commit_survives_receipt_failure_and_replay_repairs_receipt | Known revision preserved despite maintenance failure; resolution repairs supersession receipt. Not a destination receipt. |
| grv-adapter-duckdb lib | pull::tests::native_second_target_failure_rolls_back_rows_binding_and_receipt | Second-table NULL/not-null failure leaves uninitialized destination and no committed rows/binding/receipt. Initial workspace example. |
| grv-adapter-duckdb lib | pull::tests::native_identity_transaction_replays_without_files_and_refuses_lost_receipts | Original receipt survives later revision and deleted source; no rewind; mismatch refuses and lost receipt is unknown. Not combined with kill/prune. |
| grv-adapter-duckdb lib | pull::tests::native_precommit_and_postcommit_process_exit_resolve_without_reapplying | Child exit at commit hooks resolves old rollback or original receipt without reapply. Hook exit, not arbitrary SIGKILL. |
| grv-adapter-duckdb lib | pull::tests::native_pull_owner_cancellation_joins_and_fences_before_outcome_resolution | Lock unavailable while owner active; cancel interrupts native work and releases lock only after join; reopen resolves. Precommit cancellation. |
| grv-core lib | admin::tests::intent_description_and_private_progress_never_authorize_deletion | Pending/private progress cannot authorize physical deletion; data retained before committed tombstone. Local fixture. |
| grv-core lib | admin::tests::gc_tests::gc_expired_allocated_claim_without_stopped_writer_evidence_remains_waiting | Expiry alone preserves partial data/open owner and reports waiting. Safety refusal, not eventual progress. |
| grv-core lib | admin::tests::gc_tests::gc_refuses_stopped_writer_proof_for_another_run_epoch_before_takeover | Wrong epoch proof refuses without changing control. Synthetic attester, not operational proof. |
| grv-conformance test host | runnable_cli_list_capabilities_echo_slice_and_environment_isolation | Credential/root canaries absent; child environment probe clean. Fixture host, not full shipped credential surfaces. |
| grv-conformance test host | cancellation_stops_descendant_before_acknowledging_and_clean_close | Controlled descendant absent before stopped ACK. Not all detached process/commit races. |

## Scoped gap closure and explicit evidence limits

- Commit process-exit hook -> reopen originalreceipt -> latercheckpointadvance
  -> originalreplay with deletedsource now added to the named native crash test
  and PASSED (pull-commit-advance-replay.log); originalrows do not rewind. This
  closes the inspected EX-PULLTX-003 conjunction; task29 separately tests
  actual SDK cancel/EOF at deterministic commit boundaries below.
- Task28 now PASSED: custom `salesforce_capture` productionadapter/SDK/CaptureJob
  gate seeds exact two-row derivedmonth-partitioned + empty tables at revision1,
  then REST secondpage503 AFTER rawbatchdurability. Canonicalization refuses;
  durable capture/terminal absent, publishedLATEST/schema/manifests/data bytes
  unchanged (mutable .runs excluded). Freshprocess sameattempt refuses before
  sourcequery counters increase. Firstsnapshotfilter harnessfailure and retry
  retained; synthetichelpers, not realservice fault or entireproductionCLI.
- Task29 now PASSED: `commit_boundary_tests::sdk_native_cancel_and_eof_at_commit_boundary_fence_and_immutable_replay`
  uses actual SDK wirecancel/EOF -> StopToken/PullWorker nativeinterrupt, with
  cfgtest-only before/afterCOMMIT synchronization. Ownerlock held/noprematureACK
  until join; postcommit originalreceipt recovered; beforecommit rollbackor
  exactcommittedreceipt. Advance3/deletefiles/originallookupcompare returns old
  receipt with no prepare/apply/rewind. Fourcases pass; deterministic schedule
  not allpossible races. Hooks absent from shippedlibrary normalbuild.
- Production helper isolation now tested: supervised_helper_isolates_inheritable_parent_descriptors
  opens fd>=64 WITHOUT CLOEXEC, proves ordinary shell can read exactcanary,
  then productioncontainedhelper cannotread whileparentfdremainslive. Explicit
  testenvironment only;9production tests and targetednegativecontrol PASS.
  Existing privateSTDIN/TLS/noargvcredential/redactedstderr assertions inspected;
  actualbundle heuristicsecret/fixtureartifactscan PASS. Universalcredential
  surface/leak detection is notclaimed; SFactualCLI auth used inlivecandidateGCS.
- admin::tests::gc_tests::gc_corrupt_coordination_refuses_before_destructive_effects
  now PASS: corrupt REAL publishedruncontrol/claim/supersessionreceipt after
  eligibleoldversion andlease;applyrefuses,allstoragebytesunchanged,dataretained.
  Localfaultbackend gate, notlivecloudcorruption. Existing preview/newpin/apply
  race,tombstonebeforedelete,wrongstoppedepoch andexpirywaitingassertions inspected.
  Production LocalWriterAttester refusescloudrootbeforejournalaccess;localproof
  needs lockedjournal exactroot/run/owner/created/base/input/metadata and stopped
  acceptedfile/export evidence. No cloudtakeover inferredfromlocalprocesslock.
  Conservative busy/unknown remains explicitscope, notsuccessfultakeoverclaim.
- Every retained claim must bind to actual shipped binary/native digests and
  clean-source qualification. Not all ignored tests are missing work: helpers
  run via custom tests, live selectors require named authorization.

No scenario or full release case is marked complete solely by this document.
No full-v1/four-platform claim is permitted. Candidate gate review must record
which exact requirement closes each gap or a reviewed capability exclusion;
known contract violation cannot be rebranded as a preview limitation.
