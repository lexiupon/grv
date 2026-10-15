# Package25 bounded refresh/source fault/nonempty COMMIT execution plan

**Status: parent-reviewed execution plan; live results pending.** A plan does
not pass the gate. Parent source review and final offline validation are done:
121 native /20 ignored +48 integration,22 host+12 peer CLI,15 CTest, strict
native/non-native Clippy/fmt/diff and12 offline driver tests pass (13 after the canonical-coordinate correction
below). The parent
applied and corrected the report-only crash draft (propagating try_wait errors,
non-success timeout classification, no post-reap kill, strict side selectors,
private paths); no agent report alone established acceptance. The ready audit
marker is published atomically after flush; whole-proof-process-group timeout
cleanup prevents an orphan test/crash child. These are harness rules, not
shipping hard-stop guarantees. Freeze the exact named artifacts before the
first acquisition, then run only the finite owned cases below. Package25 remains
open; publication/capability/default changes are not approved. No helper,
allocator, production transport, migration or wire change.

## Finite scope and genuine remaining coverage

Prior package23 OAuth,24 history,32 scanner and34 cooperative/uncertain-outcome
proofs remain separately labeled. Existing single-file live pull denials,
empty refresh and late-secondary failure are not missing implementations.
This stage adds:

1. `pull::tests::native_live_gcs_multifile_evolution_special_coordinate_refresh_and_replay`:
   actual two-file111/222 refresh; new attempt/revision/receipt; historical
   replay does not rewind. Evolution to id+extra gives `(111,NULL),(333,'new')`
   from a historical shorter file and a version2 file. The evolved key contains
   space, plus, percent, Unicode and apostrophe, encoded exactly once. Pure
   same-version schema inconsistency is distinct from ordinary SQL UNION
   coercion. Source-free reopen resolves/replays immutable historical receipts
   with no reader/provider on the reopened lookup owner.
2. `pull::tests::native_live_gcs_nonempty_precommit_and_postcommit_exit_recover_immutable_receipts`
   with isolated exact ignored child
   `pull::tests::native_live_gcs_nonempty_commit_crash_probe`: real nonempty
   two-file GCS apply reaches existing before/after COMMIT test exit86 hooks.
   Fresh source-free outcome lookup distinguishes NotCommitted/Committed;
   immutable receipt replay survives later checkpoint advancement without
   reapplying/rewinding. Child watchdog is a test-harness bound, not a native
   shipping hard-stop or arbitrary WAL/abrupt-kill/race proof.
3. `pull::tests::native_live_gcs_missing_replaced_source_loss_preserves_immutable_receipts`:
   original primary42 committed; exact-generation delete produces missing-
   primary refresh refusal and prior receipt survives. Same-key replacement84
   deliberately demonstrates **stale size/SHA/generation descriptors are not
   stock per-range generation pinning**: same-schema replacement is observed.
   New descriptor/request commits a new receipt, while historical replay does
   not rewind84. Delete all known objects; a fresh authenticated view read must
   fail, not return silent empty. Source-free lookup/replay still preserves all
   committed receipts; a new configured attempt fails and legitimately resolves
   NotCommitted. No effects retry, default repair or filtering unreadable history.

Native tests operate below full CLI preparation. Actual descriptor size/SHA/
generation is supplied by protected fixture files, but application-level
prepared integrity does not prevent privileged later replacement. Full SHA/
size/schema/row preparation remains mandatory at its normal upper-layer gate;
these stale-descriptor observations are not authority to bypass it.

Offline parent additions cover actual two-file pre-dispatch cancellation,
joined WorkspaceLock release, genuine Uninitialized versus initialized
NotCommitted, and first-successful-**metadata**-FFI publication expiry before
BEGIN/source dispatch. They do not falsely place the hook after source1.
Private fixture/audit parsers enforce closed fields, bounded stable regular-file
reads, no links, owner/mode/private parent, exact selectors/root/generations.

## Orchestration and lifecycle

Driver `/tmp/grv-task25-live-driver.py`, default static plan; explicit
`--execute --freeze <protected0600freeze>` required. No gcloud/cloud requests in
default mode. Freeze covers exact library test binary, unchanged native guard,
both pinned signed extensions/manifest, driver, plan, and native/pull/build/token
source paths. Required guard SHA256:
`2709cc619eb125d1ecdc48377303019ea20bfe32f20c4e964c298b2b66abdd5f`.
All exact test symbols must appear once in the prebuilt binary. Only the three
parent gates are directly launched, each `--ignored --exact --test-threads=1`
and exactly one passing test; crash gate owns its explicit ignored children.
Pre/post/every-launch hashes must match. No Cargo, edit/rebuild or parallel
native/conformance work while any live resource/native child lifecycle runs.
No inherited package32 test-artifact qualification after source changes.

Fresh selected provider per owner execution, original lease before acquisition,
no renewal/effect retry. Only `jiacao@on.sinch.com` / `entit-finance-prod`, project 828148855924. Driver validates projected selectors/overrides, token grammar and

> =420s expiry before any resource intent/mutation. Tokens stay in memory, never
> argv/environment/log/receipt/view/digest/profiling/durable documents or raw
> helper output. Selected acquisition failure stops once, cause unclassified;
> absence404 is not invented. Best-effort secret wiping is not whole-heap proof.

## Owned resources and destructive barriers

New home evidence directory0700, atomic0600 state/intents/fixtures/evidence.
Unique bucket `grv-gcs-pull-<24lowerhex>` absent404 before marker/create intent
and POST. New-bucket creation settings match the approved scaffold:
EUROPE-WEST1, uniform access enabled, public access prevention enforced,
soft-delete retention0, Delete lifecycle age1, unique owner label.
No existing bucket/resource IAM/ACL/policy/versioning change or keys. Marker
and project828148855924 verified before operations/deletion.

Four initial objects under `fixtures`: first111, second222, evolved333+extra
`'new'` at special key, primary42. Replacement local84 is known before fault
execution. Exact key/size/SHA upload intents precede uploads;
`ifGenerationMatch=0`; returned identity/size/generation journaled. Unknown
resource/version/mismatched generation refuses deletion.

The positive and crash tests finish before the staged fault test begins. A
separate protected `audit/` directory is outside immutable fixture inputs.
Fault test **closes its native owner and proves WorkspaceLock release before**
exclusive0600 `.ready` marker; driver validates strict stage/URI/generation and
protected input hashes before cloud mutation. Driver atomically publishes the
matching `.ack`; test performs next execution only afterward. All three stages
are bounded by the test process watchdog, not shipping deadline guarantees:

- delete-primary: generation+ifGenerationMatch conditional DELETE original.
- replace-primary: new protected replacement intent then generationMatch0
  upload to same now-absent key; ack distinct known generation.
- delete-all: strictly paginate inventory, require exactly recorded known
  generations, delete conditionally, observe complete empty all-version
  inventory, then ack. Source-loss observations run while bucket exists empty.

Finally cleanup on every creation attempt/failure. Marked owned bucket only;
strict paginated all-version shapes/unique entries/no cyclic token; known size,
generation and deletion-history checks. Response-lost upload can be recovered
only as single matching intent-sized version in this uniquely owned bucket;
recorded mismatches/unknown keys refuse. Observe empty complete inventory,
then delete bucket and observe404. Already absent bucket recovery records
absence without inventing unobserved inventory. Cleanup failure retains state
for `--execute --cleanup-state <private/state.json>`, not unknown-resource
wildcard deletion or false success.

REST fixed `https://storage.googleapis.com`, verified TLS, proxy disabled,
redirects refused, no retry, finite per-call timeout, bounded duplicate-refusing
JSON. Native reader keeps its existing exact read-only authority and cooperative
lease rules. Driver stage mutation never overlaps native query work; watchdog
kills/reaps only a failed proof child and is not shipping hard-stop evidence.

## Retained first-stage development failure and corrected plan

First frozen run `/Users/jiacao/grv-task25-live-_chfl9_q` failed in the fixture
loader **before reader acquisition/native apply**: the driver used generic URL
encoding `%2B` and `%27` for plus/apostrophe, aliases forbidden by GRV canonical
gs spelling. All11 frozen artifacts stayed unchanged; four uploaded known
generations were conditionally deleted, complete inventory observed empty,
bucket404 verified. No crash or fault test was launched. This is a test-driver
coordinate error, not a shipping failure or OAuth diagnosis. Original logs,
freeze and cleanup evidence remain intact.

Driver now mirrors the existing canonical coordinate rule: raw printable plus
and apostrophe, encoded space/percent/Unicode; HTTPS translation remains the
shipping implementation. Added offline exact-coordinate test (13 driver tests).
A fresh freeze and new exclusively owned fixture are required for the corrected
stage; no old effects retry/resource reuse or silent old-hash refresh.

Second stage `/Users/jiacao/grv-task25-live-h87rl16n`: positive multi-file/
evolution/special-coordinate test passed1/15.37s. Crash test reached both
exit86 hooks and postcommit receipt recovery, but its extra authenticated
reopened-view read failed: the fresh connection had not explicitly admitted
`app.rows` to its guard. The parent fixes the test-only relation admission
before authenticated inspection (also ensuring source-loss inspection cannot
pass merely from an unadmitted relation). No production guard weakening.
Fault test was not launched. Four exact generations deleted, observed empty
inventory and404,11 artifact hashes pre/post equal. This failure and positive
pass remain stage-scoped; a new reviewed build/freeze/owned fixture is required.

## Acceptance still required

Final actual code review/compile/offline tests, frozen live exact passes, pre/
post hash correspondence and owned empty-inventory/404 cleanup must be recorded
before package25 closure. Expected outcomes are not acceptance. Broader held
abort/prune/release/expiry27, S3 matrix26, package45/aggregate/install/platform/
release and separate publication approval remain open. No universal generation
pinning, privileged replacement prevention, finite HTTP/native overrun, immutable
rollback-at-expiry, arbitrary escaped-owner lifetime or total RSS bound is
claimed.
