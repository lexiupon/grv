# Package27 — independent terminal held-GCS slices

**Finite protection/abort and release/prune slices passed; package27 remains
open.** Real run-expiry refusal now also passed as an independent slice below.
Delegated actual-GCS hold creation with modeled acknowledgement/readback faults
now also passed below. Delegated confirmation-fault also now passed below. Cooperative in-flight
cancel and ordinary-consumer special/evolution coverage remain. Actual HTTPS response loss
is not established by modeled acknowledgement faults.
No capability promotion, commit/push/publication, IAM/key/default/transport or
shipping runtime change. User-selected new subagent model is
Qwen3.8-Flash-Next-OrcaRouter/xhigh, one at a time with parent final validation.

## Implementation and parent review

Only new test `crates/grv-conformance/tests/gcs_held_terminal_cli.rs` changed.
`LifecycleSlice::Full` preserves original same-fixture matrix, still ignored
and **not qualified passing**. Independent ProtectOnly and ReleaseOnly fixtures
have local equivalents, monotonic known-stage labels/ordinals with no private
argv/env/provider output. Timing agent and split agent both delivered actual
validated code; parent inspected branches and independently ran tests/lints.

Offline `parent-split-test.txt`:5passed3ignored; focused strict Clippy/fmt/diff;
12 diagnostic tests and8 driver tests. Earlier22 holds/1 run-expiry/9 GC-abort-
inspection tests remain local only. Logs under `/tmp/grv-task27/`. No Cargo,
edit/rebuild or concurrent native lifecycle overlapped either live run.

## Actual independently owned live gates

Reviewed `spec/gcs-held-terminal-live-plan.md`, parent driver
`/tmp/grv-task27-terminal-live.py`, protected frozen manifest
`/tmp/grv-task27/terminal-freeze-split/artifact-hashes.json`. Twelve artifacts
include test binary/CLI/fixture/unchanged guard/signed httpfs+aws/manifest/test+
fixture source/driver/diagnostic module/plan. Pre/post/current hashes independently
verified equal in `parent-split-live-verification.json`; no raw diagnostic files
remain. Monotonic durations are observed, not guaranteed performance bounds.

1. `/Users/jiacao/grv-task27-terminal-hr1oirgh`:
   `live_gcs_confirmed_hold_protect_then_abort`, **1pass1152.95s**.
   Actual CLI source1 publication/full GcsView preparation/session hold
   confirmation/source2 publication/sourceGC: v1 manifest/data preserved,
   `.pruned` absent, exact immutable active hold, no release marker. Actual
   session abort seals run without derived allocation/output/publication;
   repeat abort preserves terminal control. Hold remains active/releasable;
   parent fixture teardown is **not protocol hold release**.
   GC735.653s, first/second abort10.253/0.481s.
   **24 exact generation deletes**, observed empty all-version inventory,
   bucket `grv-gcs-held-cc78b5fa765044048c46fcc9`404.
   Log SHA256 `1f943ac0fa4ec290c011a7c3594e27738154efdbd440befadde1e43632ab7fec`.
2. `/Users/jiacao/grv-task27-terminal-e9x9b9od`:
   `live_gcs_sealed_hold_source_gc_release_prune_and_repeat`, **1pass1523.68s**.
   Independent actual source1/preparation/confirmed hold/source2/abort fixture;
   omits earlier open-run GC only (covered separately above). SourceGC commits
   release operation targeting exact consumer/revision/retention ID, keeps
   immutable hold, validates release/prune markers and operation records,
   prunes oldv1, currentv2 unchanged. RepeatedGC preserves release marker
   bytes/validator, release-operation bytes, no derived effects or rewind.
   GC627.306s; repeatedGC462.415s; abort10.218s; repeated abort0.504s.
   **27 exact generation deletes**, observed empty all-version inventory,
   bucket `grv-gcs-held-ff6d39d900ab4185af8262cb`404.
   Log SHA256 `d07198e940027c1073f2ab566fbcc6f143679b8f5f2435aa80ae3f1dd116d283`.

Explicit principal <jiacao@on.sinch.com> / entit-finance-prod, project828148855924;
fixed TLS host/no proxy/redirect/retry, new bucket only settings disclosed in
plan, protected absence/create/empty-child intents, verified owned marker before
cleanup, exact known generation inventory/delete, no unknown resource authority.
No live/native/CLI/cloud resource remains running. Both cleaned before results
were written. Freeze remains stage-specific; later changes cannot inherit it.

## Small actual run-expiry slice — parent validated

New `crates/grv-conformance/tests/gcs_held_expiry_cli.rs`; reviewed plan
`spec/gcs-held-expiry-live-plan.md`; driver `/tmp/grv-task27-expiry-live.py`;
12-artifact protected freeze `/tmp/grv-task27/expiry-freeze/artifact-hashes.json`.
Same agent resumed to fix the initially early-expiry predicate/zero-sleep and
claim scope, then validated. Parent reviewed exact expiry-plus1s predicate and
independently ran **4passed1ignored**, strict Clippy/fmt/diff, logged under
`parent-expiry-*`. First list-only stage is not a passing executed test.

Actual `/Users/jiacao/grv-task27-expiry-tp5hrnzs`, exact
`live_gcs_expired_confirmed_run_refuses_renew_without_releasing_hold`:
**1pass421.20s**. Fresh root initialized maxleaseTTL180s (local30s), clockskew0,
pendinggrace1; no existing policy/default changes. Actual managed source,
full GcsView pull and external session hold confirmation precede real SystemClock
wait. Observed remaining166.648s, realwait167.741s; no timestamp edit/fakeclock,
no background lease renewal/native owner alive. Actual CLI sessionrenew refuses
`EXTRACTION_INCOMPLETE` in2.807s, without stored output effects or reader restart.
Control/hold/sourceLATEST bytes+validators and protectedcontext/journal unchanged;
closed derived inventory unchanged/no allocations/versions. Run staysOpen,
confirmed butexpired; hold active/unreleasable; checkedNotFound release marker.
Ordinary storage credential reads remain; not no-provider/SDKStopped/native
lease-expiry/cancellation/universal-hard-stop evidence. Teardown is not hold release.

**15 exact generation deletes**, complete observed empty all-version inventory,
bucket `grv-gcs-held-472212c2bdb84c74947bcbd9`404. All12 pre/post/current hashes
independently equal in `parent-expiry-live-verification.json`; no raw files or
native/CLI/resource lifecycle remains. Log SHA256
`5d8ea3d69cb0b5abd5cec946dfb0e543d7594bed3bed8c70ac0f19411ec2c6cd`.
Later artifacts do not inherit this qualification; original fullmatrix stillfailed.

## Small offline uncertain-write tests — parent validated

Two cfg(test)-only `holds.rs` additions:
`ambiguous_hold_create_with_unreadable_proof_preserves_unknown_until_exact_readback`
and `ambiguous_hold_confirmation_with_unreadable_proof_keeps_known_hold_and_no_authority`.
Real LocalBackend post-install failure yields MaybeApplied; a borrowed delegating
backend refuses only exact-target readback. Actual create/confirmation returns
OutcomeUnknown, retains immutable hold/control, no release/output. Exact create
identity re-observation encounters a precondition refusal (no second installed
write); confirmation adoption performs no second CAS. Before/after actual bytes
and validators remain identical, not merely mock counter assertions. Installed
confirmation is adopted exactly; original caller confirmation remains false
while evidence is unreadable. This is a **finite local modeled readback fault**,
not actual GCS response loss/HTTPS/SDKStopped or universal MaybeApplied outcome.

Agent initial compile failure remains in `agent-hold-fault-test.txt`. The same
agent resumed, repaired known errors and validated, rather than parent taking
over. Parent inspected actual changes and independently ran **24 holds tests**,
strict core Clippy/rustfmt/diff, logs `parent-hold-fault-{test,clippy,fmt,diff}.txt`.
Production region is byte-identical to HEAD; only cfg(test) changes. No provider,
cloud mutation or live/native build lifecycle in this work. Actual GCS fault
qualification and other package27 residual gates remain open.

## Small delegated actual-GCS creation fault — parent validated

`crates/grv-conformance/tests/gcs_held_fault_cli.rs` generic Boundary delegates
real storage operations; only successful exact-target create is then converted
to modeled MaybeApplied plus refused readback. Actual coreHolds returns Unknown;
caller cannot claim absence/confirmation. Same identity re-observation produces
real create-precondition refusal and complete source verification before exact
proof/confirmation; attempts2, successful installs1. Actual hold bytes and backend
validator unchanged. Hold active/unreleased, no consumer output. This is real
GCS write/full-source-verification plus **modeled fault above backend**, not
actual HTTPS response loss, GcsView reader/SDKStopped/native-writer evidence.

Initial reconnaissance-only agent turn-limit stage is not acceptance. Same
agent resumed with smaller localcheckpoint; parent validated1localpass. Next
small genericlivegate agent delivered2localpasses1ignored; parent independently
ran same suite and strict Clippy/fmt/diff before fresh freeze.

Reviewed `spec/gcs-held-fault-live-plan.md`, driver
`/tmp/grv-task27-hold-fault-live.py`, protected12-artifact freeze
`/tmp/grv-task27/create-fault-freeze/artifact-hashes.json`. Actual
`/Users/jiacao/grv-task27-hold-fault-sv5iednw`, exact
`live_gcs_hold_create_unknown_then_exact_proof_adoption`: **1pass175.92s**,
managedsourceCLI131.531s. Explicit selected storage identity/newownedroot only.
**14 exact generation deletes**, complete observed empty inventory,
bucket `grv-gcs-held-ac5ce4fbd2574e909837453a`404. All12pre/post/current hashes
independently equal in `parent-create-fault-live-verification.json`; no raw
reports/runningCLI/native/cloud resources. Hold teardown notprotocolrelease.
Log SHA256 `203e8162eff53cdcab5c4b6a6e7fd50e38ba496adc35113fdc3b06215e17cfff`.
This slice does not qualify confirmation-fault or original fullmatrix. Later
artifact changes do not inherit this exactstage qualification.

## Independent delegated actual-GCS confirmation fault — parent validated

Closed `Case::{Create,Confirm}` in the same test file selects exact hold-key or
consumer-control target. Confirmation acquires real hold/full source proof
unarmed, then performs actual successful consumer-control CAS before modeled
MaybeApplied/readback refusal. Caller getsOutcomeUnknown and original copy stays
unconfirmed while durable control is confirmed. Disarmwithoutwrite, sameproof+
originalcontrol adopts exact installedrecord; attempts1/successfulwrites1/no
secondCAS. Actualcontrol bytes+validator afterUnknown identical afteradoption;
immutablehold bytes+validator unchanged, releaseNotFound/no consumer output.
This remains **fault modeling above real GCS**, not actual network loss,
SDK/native-writer/cancellation proof or universal ambiguous-outcome recovery.

Agent delivered actual validated case; parent independently ran3localpasses/
2ignored, strict Clippy/fmt/diff. Confirmation review/freeze is a **new artifact
stage**, not an inherited create qualification; earlier create pass preserved.
Reviewed updatedfaultplan/driver with explicit `--slice confirm`, protected
`/tmp/grv-task27/confirm-fault-freeze/artifact-hashes.json`. Actual
`/Users/jiacao/grv-task27-hold-fault-7srqxy7s`, exact
`live_gcs_hold_confirmation_unknown_then_exact_control_adoption`:
**1pass177.90s**, managedsourceCLI130.753s. **14 exact generation deletes**,
complete observed empty all-version inventory,
bucket `grv-gcs-held-37bcbed8c22b459c9d3499f3`404. All12pre/post/current hashes
parent verified equal in `parent-confirm-fault-live-verification.json`; no raw
files or runningCLI/native/cloud resources. Log SHA256
`2b28153ab31ad6011d4510a45feb92d7b08a3ccc27ded24aae650a1e817490e2`.
Package27 stays open for held cancel and ordinary-consumer intersection slices;
original fullmatrixfailed/evidence retained; no publication/capability promotion.

## Next cancellation checkpoint — not yet acceptance

Read-only map located BuildWorker stop/join, but missed existing real SDK native
cancel/EOF test in `tests/build_process.rs`. Parent found/reviewed
`cancellation_and_eof_interrupt_native_invocation_join_owner_and_refuse_reexecution`:
actual channelCancel/EOF reaches SDK/native owner, not CLIkill==Stopped. Its
managed/local coverage does not establish held GCS intersection. Selected GCS
identity remains explicit gcloud, not ambient ADC as map suggested.

Attempted small offline test extended that path to external empty gs-root
preparation and compute cancellation, but failed: `ExecuteBuildRequest` permits
**managed execution only**. Parent's proposed external execution frame was
incorrect; this intentional refusal is not a production defect. No policy/API
change or managed fallback will make it qualify external cancellation. Same
agent resumed to reduce test to valid retained external preparation, channelEOF,
normal ownerjoin/lockrelease and unchanged Prepared durable record on reopen.
Forced watchdog kill must remain failure, never substitute for normal join. It does not
acquire actual corehold/nonemptyGcsView/source preparation or HTTPS; supplied
`holds_confirmed` flag alone is not confirmedhold evidence. KnownFailed terminal
can win cancel race: preserve completion/Stopped distinction. Scheduling delay
is not evidence of native IO entry or universal hardstop. The rejected test
and its failure log are preserved; code written/compiled is not test acceptance.
This is an intermediate
checkpoint only; actual held-session intersection remains to qualify.

## Parent-validated external empty SDK EOF checkpoint

`crates/grv-adapter-duckdb/tests/build_process.rs` now derives connection mode
from execution and prepares empty external self input. Exact
`external_empty_gcs_sdk_eof_joins_prepared_owner_without_receipt` observes real
workspace lock busy before channelEOF, normally joins SDK child, reacquires lock,
and twice reopens unchanged exact Prepared/session with no candidate/completion/
outcome. There is no externalExecuteBuild invocation and no cancelled/Stopped
acknowledgement claim. No corehold acquisition, source preparation, cloud read or
credential invocation; supplied holdsflag is below the real confirmation boundary.

Same agent resumed twice: reconnaissance-only stage and rejected externalExecute
stage are not acceptance. Final valid3-test suite parent independently passed
**3tests1.20s**, scoped strictClippy/rustfmt/diff. Logs
`/tmp/grv-task27/parent-empty-gcs-eof-{test,clippy,fmt,diff}.txt`. Childguard uses
bounded join and timeoutkill remains Err/failure, with no signalling knownreaped
PID; exceptional unclassified status-read can leak rather than blindly signal.
Containment is not shippinghardstop. Existing managed nativecancel/EOF test also
passed but does not inherit externalheld authority. This intermediate proof does
not close the residual actualheld in-flight supported-operation intersection.

## Parent-validated local confirmed-hold EOF checkpoint

`crates/grv-conformance/tests/gcs_held_cancel_cli.rs` exact
`local_confirmed_hold_survives_managed_cli_eof_and_incomplete_attempt_refusal`
uses actual localCLI sourcepush/fullpull/coreholdconfirmation/managednativebuild.
Fixturecounter plus durableExecuting prove dispatch, not exact nativeIO entry.
OwnedCLI loss closes SDK channel; after bounded lock release, source-free reopen
showsExecuting/no candidate/completion/outcome. ONE sameattempt recovery refuses
BUILD_INCOMPLETE, executioncounter staysone, second durable reopen is unchanged.
Actualcorehold remains active/unreleasable with no releasemarker/outputallocation/
version. Rawholdbytes+validator invariant **after loss across recovery**, not
pre/postloss snapshot. Legitimate control renewal CAS is allowed; fixed run/owner/
inputs identity preserved. No protocolStopped/HTTPS/GcsView/hardstop claim.

Initial agent recon-only, compile/runtime/harness failures remain in logs. Parent
independent first run failed strict protectedjournalread under active publication:
`parent-held-local-eof-test.txt` exit101; exact race cause unclassified, no general
history refusal weakened. Observer corrected to owned fixturecounter beforeloss,
strict protectedjournal read after owningCLI died. Native-owner lock checked
before native reopen. Parent retained second durable reopen across recovery.
Parent independently passed **1test8.57s**, repeat **1test8.58s**, strict scoped
Clippy/rustfmt/diff. Logs `parent-held-local-eof-quiescent-{test,clippy,fmt,diff}.txt`.
This local checkpoint does not qualify actualGCS held EOF or external GcsView
in-flight cancellation; those cannot be inferred from related tests.

## Generic GCS-storage held EOF code and pre-resource live refusal

New generic backend case in `gcs_held_cancel_cli.rs` preserves local materialized
pull/managed native invocation, explicit child-only cloud selectors and actual
backend holds. Strict pure ownedroot validator plus ignored
`live_gcs_confirmed_hold_survives_managed_cli_eof_and_incomplete_attempt_refusal`.
No GcsView input/native cloud-range cancellation or external ExecuteBuild claim.
Parent independently **2offlinepasses1ignored**, scoped strictClippy/fmt/diff.
Parent driver8offline ownership checks, reviewed
`spec/gcs-held-eof-live-plan.md`, fresh12-artifact freeze
`/tmp/grv-task27/held-eof-freeze/artifact-hashes.json`.

Actual attempted stage `/Users/jiacao/grv-task27-held-eof-yamhriny` **failed before
resource intent**. Protected state has no creation_attempted/ownership marker,
no creation-intent document or CLI log/test launch. Generic harness error cannot
distinguish selected acquisition/preflight from precreate404 request refusal;
exact cause unclassified. No live EOF pass, no invented bucket404/cleanup claim.
No resource mutation admitted; all12pre/post/current hashes independently equal
in `parent-held-gcs-eof-refusal-verification.json`. No running nativeCLI/provider.
Failure retained; no effect retry or capability promotion. A subsequent attempt
needs fresh fixture/stage, not relabeling this failed attempt.

## Actual GCS-storage held EOF pass after selected re-login

User reported successful re-login, authorized proceed. Fresh protected
`/tmp/grv-task27/held-eof-relogin-freeze/artifact-hashes.json` independently matched
all12 reviewed previous-stage current artifacts. New fixture/stage, not effects
retry on prior admitted resources. Prior `yamhriny` refusal remains failed and
cause-unclassified; later success alone does not diagnose its cause.

`/Users/jiacao/grv-task27-held-eof-ma2wff85`, exact ignored
`live_gcs_confirmed_hold_survives_managed_cli_eof_and_incomplete_attempt_refusal`:
**1pass275.26s**, sourcepush148.195s/full localmaterialized pull17.674s,
exact-attempt recovery8.202s. ActualGCS metadata/hold/control plus full staged local
source preparation, confirmed held managed native dispatch, ownedCLI loss/SDKEOF,
workspace lock release, sourcefree Executing/no completion/candidate/outcome and
sameattempt BUILD_INCOMPLETE/no secondexecution. Rawholdbytes+validator unchanged
across recovery afterloss, active/unreleasable/no release/no derived outputversion.
**Not GcsView native remoteIO cancellation, wire cancel_ackStopped, exact nativeIO
entry, universalhardstop or externalExecuteBuild authority.** No test-side hold
release; synthetic teardown is not protocolrelease.

**15 exact generation deletes**, complete observed empty all-version inventory,
bucket `grv-gcs-held-1c495f978443472ea4819e07`404. Parentindependent
`parent-held-gcs-eof-live-verification.json`:12pre/post/currentequal/no rawreports,
no runningCLI/native/provider or live resource. TestlogSHA256
`0a0848dfc428aa6eb6cc9c14974c54fb1a076ad27054639d6d314ed3e8464620`.
Package27 still open for ordinary-consumer special/evolution intersections and
remaining actual held external/GcsView cancellation scope review. This finite
independent pass does not retroactively pass original samefixture fullmatrix.

## Parent-validated ordinary local external evolved-revision preparation

New `gcs_held_evolution_cli.rs` exact
`local_ordinary_external_session_prepares_evolved_confirmed_revision` uses CLI
sourcepush revision1(id) then revision2(id,note), default full localpull/current2,
ordinary externalsessionprepare/coreconfirmedhold2. Originalmanifest bytes
unchanged; actual committed file count1 each, schemas/complete rows checked
including oldrevision again afterreplacement and newrow(2,new). Durable consumer
Prepared/sessionexact/no completion/candidate/outcome. Actualhold citation key/
targetrun/active/unreleasable/no release. Existing public nativeguard acquisition
of privateinput returns exactly(2,new), completeonewindow thenNone.

Parent independently **1pass10.70s**, strict scopedClippy/cargo fmt/diff. Logs
`parent-held-evolution-local-{test,clippy,fmt,diff}.txt`. Agent recon-only stage,
YAML/type/root and invented retention/generation-equivalence failures retained.
No production API changed: crateprivate metadataquery remains private; public
acquire/fetch/visit used. This is local ordinary external consumer **whole
replacement with added column**, not multifile mixed-schema typedNULL or GcsView/
special-coordinate qualification. Sourcepublishes; preparedconsumer does not.

## Failed first ordinary GCS evolved-revision stage — premise corrected

Generic code parent2offlinepasses1ignored/strict checks did not exercise live
external-GCS reconstruction. Reviewed frozen12-artifact
`/tmp/grv-task27/held-evolution-freeze/artifact-hashes.json`, plan
`spec/gcs-held-evolution-live-plan.md`, driver8offlinechecks. Actual stage
`/Users/jiacao/grv-task27-held-evolution-sjm5xu4s` **failed395.13s** at ordinary
sessionprepare generic okassertion, after actual sourcepush1/sourcepush2/localpull2.
Full CLIerrorenvelope not retained; exact reported cause unclassified.
**23 exact generations deleted**, observed empty allversioninventory,
bucket `grv-gcs-held-654d28e079c04467904e22f8`404;12pre/post/currentstable parent
`parent-held-evolution-failed-live-verification.json`. LogSHA256
`c886697a50d7215304eb14ffa189ea4db2dffab19ac8acb86be5027dc60c0018`.
No live pass/source-policy change or retroactive qualification.

Parent's locallymaterialized-external assumption was wrong: core
`verify_inputs` reconstructs FileAccessGcsView for external bound GCS backend
**even when discovered materialization is Local**. Existing core unit explicitly
covers both. Therefore ordinary external GCS preparation needs independent
readerconfig; missing config is a concrete test setup gap, not an asserted exact
cause of the retained generic livefailure. No forcingLocal/newpublicAPI/shipping
fallback. Previousagent resume unavailable(Agentnotfound); freshagent owns only
necessary readerconfig/livebranch correction. Local publicguard valueoracle stays;
live preparation/holds/schema source-evidence gate must not claim native private
GcsView row reads it does not execute. Correctedcode requires freshfreeze/fixture.

## Corrected ordinary external GCS evolved-revision preparation pass

Protected independent readerconfiguration added for live engine; existing public
native acquire/fetch valueoracle remains LOCAL-only. Ignored symbol corrected to
`live_gcs_ordinary_external_session_prepares_evolved_confirmed_revision`.
Parent independently2offlinepasses1ignored/strictClippy/fmt/diff; driver8checks,
reviewed amendedplan and fresh12-artifact
`/tmp/grv-task27/held-evolution-reader-freeze/artifact-hashes.json`. Prior failed
`sjm5xu4s` remains failed, not requalified by this fresh fixture.

`/Users/jiacao/grv-task27-held-evolution-7vozhhee`: **1pass409.91s**,
sourcepush1 132.800s/sourcepush2 160.567s/fullpull16.109s/ordinary
sessionprepare77.542s. Realsource whole replacement addedcolumn, actualcomplete
Parquet schema/row checks/unchangedoldmanifest, discoveredLocalmaterialization2,
actualcoreconfirmedhold2/activeunreleasable/no release. OrdinaryexternalGCS
verification reconstructs GcsView inputs; explicit independentreader setup,
signednative preparation/bind/admission succeeded, exactdurable Prepared/session
reopen/no completion/candidate/outcome. **No private nativeGcsView row read executed**:
livevalueoracle is committedParquet/fullsourcepreparation, notGcsViewexpectedvalue
or generation-integrity proof. Not mixed-schema typedNULL/append/specialcoordinate.

**23 exact generations deleted**, observed empty allversioninventory,
bucket `grv-gcs-held-5d45d4738dde40c29e63acf8`404. Parent
`parent-held-evolution-live-verification.json` independently12pre/post/currentequal,
no rawfiles/runningCLI/native/provider/resources. LogSHA256
`addb2ef013ac558885285fe3165d861973d45c46a71fdff6b2dab1b02531c6ac`.
No consumer publication/hold release; fixtureteardown only. Remaining private
GcsView reader-value/special-coordinate/held-cancel intersection scopes need final
review; this preparation slice alone does not close package27/fullmatrix.

## Independent external-reader LOCAL child checkpoint

Test-only `tests/support/held_external_reader.rs` plus existing evolutioncase
now invokes realchild C API private localinput read after publicguardengine closes.
Parentretains realWorkspaceLock, inherits duplicateFD in child, checkedhelper
identity/protectedconfig; childclosesengine before closeddecodedresult publication
and descriptorrelease. Parent joins successfulchild then releases/reacquireslock
and adopts exact(2,new). Parentbusyprobe proves itsownlock, not childcausalretention.
No OAuth/signedHTTPFS/nativeGCS read yet; this does not qualify cloudreader.

Parentindependently **4localpasses1ignored11.65s**, strictscopedClippy/fmt/diff.
Decodernegatives extra columns/rows/zero/null/over16varchar and positive actualCAPI
passed. Stringlength checked beforecopy/strictUTF8; RAIIstockvarchar/result cleanup,
no customstockallocationcap/heap-erasureclaim. Ownedchildguard timeoutkill fails,
not normal closure/SDKStopped/shippinghardstop. Initialrecon-only agentnotaccepted;
sameagent resumedlocalcheckpoint then hardening. Logs
`parent-held-private-reader-local-{test,clippy,fmt,diff}.txt`.
Nextsameagent boundedOAuth/privateGCSviewcode only; parentliveplan/freeze separate.

## Actual ordinary held private GCS-view external read

Fresh independent gate `live_gcs_ordinary_external_session_reads_evolved_confirmed_revision`
**passed, 396.78 seconds**. Evidence directory
`/Users/jiacao/grv-task27-held-reader-8g5c9ae7`; log SHA256
`aa8aab2170c4790d87bfa09289852bc6f03a868e262c83c6bf2bc358c39f5587`.
Actual source1/source2 whole-replacement evolution, full preparation and confirmed
revision2 hold precede an independent external stock C API reader. The child uses
its own selected keyless OAuth acquisition, pinned signed HTTPFS and temporary
BEARER_TOKEN secret. Catalog check requires the private input to be a view;
actual query returned exactly one `(2,new)` row. The child closes its native
connection before publishing its protected decoded result and exiting. Parent
joins before result adoption and independently reopens the same Prepared session:
no candidate/completion/outcome, active unreleasable hold, no release, unchanged
context journal. Parent retains a cooperative workspace lock throughout; its busy
probe is not causal proof of child-only lock retention.

Parent independently validated 4 local tests (11.86 seconds), 1 live ignored,
strict scoped Clippy/fmt/diff and 8 driver ownership checks. Fresh 13-artifact freeze
`/tmp/grv-task27/held-private-reader-final-freeze/artifact-hashes.json` matched
pre/post/current. Parent checked 23 exact-generation deletions, observed empty
all-version inventory and bucket404 for `grv-gcs-held-b2e24896f4bd4eaaa4414112`.
Verification: `/tmp/grv-task27/parent-held-private-reader-live-verification.json`.

This is an ordinary external reader, not GRV's exact connection guard. It does
not qualify shipping transport denials, native in-flight cancellation, hard stop,
per-range generation pinning, universal heap erasure, mixed-schema typedNULL union
or special cloud coordinates. Original full-matrix failures remain failures.
Package27 remains open; no capability promotion, publication, commit or push.

## Actual held GCS-view managed cancellation

Fresh independent gate `live_gcs_held_gcs_view_cancellation_refuses_reexecution_after_supported_interrupt`
**passed, 270.40 seconds**. Evidence directory `/Users/jiacao/grv-task27-held-cancel-awsnte0c`;
log SHA256 `f2938ef046dd7e60e6794dc222ad4e5bf514b9c31747bc49e89bdbd09399c146`; freeze
`/tmp/grv-task27/held-cancel-retry-freeze/artifact-hashes.json` (12 artifacts, pre/post/current
equal); verification `/tmp/grv-task27/parent-held-cancel-live-verification.json`.

Real push (150.2 s) and real pull (17.6 s) on a fresh owned child, protected credential-free
reader configuration resolved by the production strict loader to the explicit selectors, so
core reconstructed the input as a private GcsView on the shipping managed in-process path with
the exact guard. The owned CLI child was dispatched, killed and reaped so the SDK saw EOF; the
protected journal was read only once quiescent. Nonvacuous durable Executing dispatch, no
candidate, completion or outcome; the one same-attempt retry was refused; exactly one dispatch
line survived, so no retried effects. The real core hold stayed active and unreleasable with no
release marker and no `.pruned` tombstone; hold bytes and validator were invariant after the
loss and across recovery; the consumer session was unchanged and only the run-control fixed
typed identity was compared. Cleanup: 15 exact-generation deletes, observed empty all-version
inventory, bucket `grv-gcs-held-26eec279cacf4b0e99072524` returned 404.

First attempt `/Users/jiacao/grv-task27-held-cancel-xbopf8vv` **failed** at 250.89 s and cleaned
up completely (15 deletes, empty inventory, bucket `grv-gcs-held-813ae2865a874d1680475384` 404).
Cause was a wrong test expectation: it required the consumer run control's own `base_revision`
to be 1, but that `derived` build was interrupted and never committed, so its base revision is
legitimately 0; the held input citation is upstream revision 1 and that assertion passed. Test
assertion corrected to compare each snapshot against the pre-recovery snapshot. The failure is
retained and not retroactively passed.

Cooperative cancellation only: this proves dispatch, loss observation, refused re-execution and
invariant durable state. It does not prove entry into a native read, an `SdkStopped`
acknowledgement, a hard native stop, finite stock-HTTP overrun, atomicity, per-range generation
pinning, or universal credential erasure.

## Ordinary-consumer special-coordinate scope conclusion

Parent-verified from code: the ordinary managed path cannot express a special
coordinate. `crates/grv-core/src/canonical.rs` generates `data.parquet` /
`data-<n>.parquet`; `crates/grv-core/src/capture.rs` refuses a name that differs
from the canonical group and `crates/grv-core/src/ownership.rs` refuses
non-contiguous staged files; the declaration has no data-file-name field and
`Name` rejects non-canonical forms, so dataset and table names cannot carry one
either. `ObjectKey::valid_relative` is permissive, but nothing on this path ever
feeds it a special file name.

Therefore no ordinary-consumer special-coordinate test is authored, and none is
claimed. Special coordinates remain qualified where they can actually exist: the
reviewed reader-side fixture-URI refresh path (package 25, spaces, plus,
percent-encoded, Unicode and apostrophe passed) and the canonical coordinate
vectors. The delegated agent made no edits and ran no tests this round.

## Package 27 finite-scope closure

Parent re-verified all nine bounded live slices from their retained evidence: failure null,
cleanup error null, one exact test passed per slice, observed empty all-version inventory,
bucket 404, and pre/post freeze hashes equal to the recorded freeze.

| slice             | directory                            | result                                             |
| ----------------- | ------------------------------------ | -------------------------------------------------- |
| protect           | `grv-task27-terminal-hr1oirgh`       | pass, 24 generations, bucket 404, 12 hashes stable |
| release           | `grv-task27-terminal-e9x9b9od`       | pass, 27 generations, bucket 404, 12 hashes stable |
| expiry            | `grv-task27-expiry-tp5hrnzs`         | pass, 15 generations, bucket 404, 12 hashes stable |
| create fault      | `grv-task27-hold-fault-sv5iednw`     | pass, 14 generations, bucket 404, 12 hashes stable |
| confirm fault     | `grv-task27-hold-fault-7srqxy7s`     | pass, 14 generations, bucket 404, 12 hashes stable |
| held EOF          | `grv-task27-held-eof-ma2wff85`       | pass, 15 generations, bucket 404, 12 hashes stable |
| evolution prep    | `grv-task27-held-evolution-7vozhhee` | pass, 23 generations, bucket 404, 12 hashes stable |
| private-view read | `grv-task27-held-reader-8g5c9ae7`    | pass, 23 generations, bucket 404, 13 hashes stable |
| cancellation      | `grv-task27-held-cancel-awsnte0c`    | pass, 15 generations, bucket 404, 12 hashes stable |

Retained failures, not retroactively passed: the three original full same-fixture matrix
timeouts, the pre-resource refused EOF stage `grv-task27-held-eof-yamhriny`, the failed
evolution stage `grv-task27-held-evolution-sjm5xu4s`, and the failed first cancellation
attempt `grv-task27-held-cancel-xbopf8vv`. All cleaned up completely; no cloud resource from
this package remains.

# 27 is closed in this finite, per-slice scope only. It does not constitute the original

full same-fixture matrix, and it does not establish a hard native stop, finite stock-HTTP
overrun, `SdkStopped` acknowledgement, atomicity, per-range generation pinning, protection
against privileged same-key or same-schema replacement, universal credential erasure, or a
universal allocation or RSS bound. Cooperative cancellation, the documented cooperative
deadlines and the disclosed harness containment bounds remain the only timing claims.
Ordinary-consumer special cloud coordinates are not reachable and are qualified only through
the reviewed reader-side fixture-URI path in package 25. No commit, push, tag, publication or
capability promotion is made here, and the release-validation register was not refreshed.

## Limits and failed stages retained

`spec/gcs-held-terminal-qualification-blocker.md` retains three original
whole-matrix timeouts, cleanup/hashes and parent corrections to speculative
stall/cache diagnoses. Successful independent slices do not turn those into
passes or prove one full nine-command same-fixture lifecycle. Observed long
GC calls explain practical whole-budget pressure, not a proved exact latency
cause or universal finite-bound claim. No blind deadline increase installed.

Holds never expire; run expiry is separate. Consumers cannot release holds;
actual committed sourceGC releases only after releasability. Abort sealed run
is not an in-flight SDKStopped/cancel proof. Active hold protects GC, not
privileged object replacement or per-range generation immutability. Source
preparation remains mandatory; no allocator/helper/fallback/public-wire changes.
Cooperative native lease contract unchanged; harness sample/watchdog cannot
prove hard-stop, rollback, universal erasure or stock HTTP finite overrun.
