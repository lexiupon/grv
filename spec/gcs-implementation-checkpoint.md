# GCS implementation checkpoint

Status: implementation in progress; not a release qualification or capability
advertisement. No push, tag or publication is recorded by this checkpoint.

## Latest decision (revision6 transition implemented, qualification open)

The user approved returning GCS native execution to in-process DuckDB within
the adapter, matching local/S3, and explicitly documenting cooperative deadline
and partial engine-memory limitations. See
[gcs-in-process-execution-decision.md](gcs-in-process-execution-decision.md).
Current code now executes GCS in-process; helper source is archived outside
production paths. Native revision6 offline slices and authorized live pull/self-
preparation pair `296qgt4m` pass, owned cleanup inventory empty/bucket404. Selected
OAuth reauthentication is restored; actual prebuilt held lifecycle `dw_ypv61`
passed1test/1962.69s with five matching pre/post artifact hashes and owned404.
Later generated-SQL admission Rust edits have separate evidence, not automatic
coverage by that held pass. Historical helper evidence below is not
qualification of this replacement. See
[in-process transition checkpoint](gcs-in-process-transition-checkpoint.md). No
helper allocation policy is approved; capability stays unadvertised. Stop/join/rollback at lease expiry
and a universal engine/RSS cap are not promised. Transfer-buffer bounds, exact
authority, credential protections and honest unknown-outcome recovery remain
mandatory. User requests pause after memory-related scope completes.

## Implemented

- Explicit protected gcloud reader selectors; bounded zeroizing token/expiry
  acquisition, 300-second monotonic execution lease and 120-second expiry margin.
- Coordinated GCS file/materialization records, canonical `gs://` provenance,
  fixed-host HTTPS derivative, full-object preparation and authenticated bucket
  metadata admission refusing Requester Pays. JSON API omitted default-false
  billing was confirmed live; null, malformed and explicit true remain rejected.
- Signed stock HTTPFS temporary bearer setup; no HMAC keys or policy exceptions.
- Exact read-only scanner/file admission and handle epochs, guarded pulls,
  source-free replay, empty refresh, held external-build dispatch.
- Guard revision 4 virtual stock request delegate: exact URL/read/deadline and
  GET content checks, remaining-lease timeout and cached-client disposal. Exact
  pinned HTTPFSParams subtype forwards to signed stock httplib. Curl is not
  admitted because pinned curl lacks a hard transfer deadline. Revision 3's engine
  hook is bypassed by the signed extension's private HTTPUtil methods; its mock
  proof does not establish a shipping request fence. Synthetic watchdog rollback.

## Evidence and limitations

- Earlier native shipping-path owned one-row GCS pull passed, including direct,
  nested-unlisted/glob denial, source-free reopen replay and empty refresh.
  Bucket/object generation removed; empty all-version inventory and HTTP404
  confirmed. Private evidence: `~/grv-shipping-gcs-27mbgze_`.
- After user reauthentication, revision-3 guarded live pull passed with explicit
  `jiacao@on.sinch.com` / `entit-finance-prod`, without fallback. Nonempty GCS
  external self-input preparation also passed: one-row output seeding and private
  remote view reads, unheld-request rejection, source-free reopen, abort and
  repeated terminal cleanup, plus injected precommit rollback leaving no durable
  session. This test supplies confirmed-hold evidence directly; it does not
  establish actual parent revision-hold creation/release or external execution/
  publication. Private evidence: `~/grv-shipping-gcs-mcwbxcbc`; owned generation
  deleted, all-version inventory empty and bucket HTTP404 verified.
- Native proofs: **9/9 passed**; engine-hook mock remains supplemental, not signed
  extension request qualification. Revision-4 signed-stock loopback direct,
  redirect, stalled HEAD and trickling GET proof passed with zero redirect-target
  requests. Added continuously trickling HEAD-header test **fails** the hard
  in-flight bound: httplib timeout fields are per-read, not absolute. Independent
  review also flagged shared request-parameter mutation under parallel reads and
  failed-setup temporary-secret retention. Source fixes now use private per-request
  parameter/request copies with client disposal and best-effort failed-setup
  secret revocation. Partial signed-loopback proof passes; rebuilt shipping-source
  scanner harness also passes all ten uncompressed oracles after these fixes
  (`~/grv-shipping-matrix-61mln9wb`, cleaned/bucket HTTP404). After the immutable
  held test finished, the production shared library was rebuilt; **9/9** native
  proofs, **77** adapter regressions and both live pull/self-preparation tests
  passed (`~/grv-shipping-gcs-fkhh1s11`, owned generation deleted/bucket HTTP404).
  The synthetic computation watchdog/rollback also passed again.
  Hard in-flight stop/join remains unresolved, not an accepted timeout limitation.
- Revision-4 GCS shipping-scanner slice: ten local-oracle trials passed. Fixed
  137,136,798-byte uncompressed fixture; wide/narrow/selective-wide query body
  bytes **137,244,704 / 788,768 / 4,542,849**, savings **99.43% / 96.69%**.
  Binding and each EXPLAIN add 262,144 bytes. Preparation traffic is not measured
  by this harness; it remains full verified preparation. Evidence:
  `~/grv-shipping-matrix-5evtjcwh`, all versions deleted/bucket HTTP404 verified.
  Reviewed sanitized slice index:
  [gcs-shipping-scanner-checkpoint.json](release-validation/gcs-shipping-scanner-checkpoint.json).
  ZSTD/dictionary/no-statistics companion slice also passed all ten oracles:
  wide/narrow/selective-wide **19,790,639 / 193,188 / 805,574 bytes**; selective
  narrow reads every row group, not statistics pruning. Companion evidence:
  `~/grv-shipping-matrix-scm55xtv`, cleaned and bucket HTTP404 verified.
- Actual parent-held external conformance **passed** against an unchanged
  revision-4 native library: actual dependency holds, fixed local/remote/empty
  input views and self-base, tracking refresh isolation, independently configured
  ordinary DuckDB writer, failed accepted export then restart/publication, exact
  output/provenance/full verification and terminal replay without adapter/config/
  engine/completion. Evidence: `~/grv-held-gcs-vmm02qcj`; all-version inventory
  empty and bucket HTTP404 verified. Successful published dependencies correctly
  retain their holds; this does not establish abort/prune/hold-release or expiry
  qualification. The preceding mixed-revision failure is preserved, not a pass.
  Final request-isolation/cleanup source fixes were added after this immutable
  run began; the live pull/self-preparation rerun covers those, not a new complete
  parent-held lifecycle.
- Native adapter regression: **77 passed**, credential/fixture-dependent tests
  ignored; synthetic-token watchdog interruption/rollback passed separately.
- API/storage/core foundation suites were passed during implementation. Latest
  focused core build **6 passed**, including real dependency-hold and backend
  remote-coordinate selection; source verification **12 passed** including GCS
  full-object and protected S3 regressions, cloud GCS **11 passed / 3 ignored**; later
  all-core reruns were interrupted by execution timeouts, not claimed complete.
- Independent contract review identified canonical-escape and dotted-label
  mismatches. API/storage validators now align with adapter exact coordinates;
  a shared storage/API/adapter byte-mapping corpus covers aliases, controls,
  Unicode, percent-looking, wildcard, quote and URL-sensitive bytes. API **7
  passed**, cloud storage **40 passed / 4 ignored**, shared adapter mapping passed;
  strict affected all-target Clippy passed. Stale-peer/durable zero-effect negative
  compatibility qualification remains open.
- Script suites: **49 passed** using Python 3.14 (system Python 3.9 lacks the
  documented Python >=3.11 APIs); cloud prototype discovery **18 passed**.
- Strict core/native adapter/conformance all-target Clippy passed before the
  final contract/native source fixes; affected API/storage/adapter Clippy passed
  after contract fixes. Full immutable candidate qualification remains pending.

## Subsequent isolated-owner work (uncommitted checkpoint)

User approved dedicated GCS owner-process architecture and explicit S3 profile
`lexiupon-aws-cli-s3`/`eu-west-1` for isolated synthetic buckets. The helper now
starts unconfigured before external GCS discovery's retained transaction;
authentication later configures that same connection without changing snapshot.
Private typed SCM_RIGHTS transfers restore verified local input scans without
filename reopening; lazy fd definitions are refused. An adversarial Darwin
ancillary-truncation leak was found, retained as regression and fixed with
full-platform-capacity ancillary storage and RAII rejection cleanup.

Native adapter regression: **95 passed / 13 ignored**, including seventeen owner
IPC tests. Actual unlinked descriptor/widening, configured GCS+local self-base
transaction/abort cleanup, source-free preparation rollback and parent-crash/
lost-reply recovery primitives pass. Latest authorized live pull/self-preparation
rerun `~/grv-shipping-gcs-oh8fzc6h` passed and cleaned all generations/bucket404.
Full actual parent-held post-isolation execution **passed1919.00s** at
`~/grv-held-gcs-d0xk4f7t`; prefix cleanup plus empty all-version inventory/bucket404.
After completion, serial native regression **97 passed/16 ignored**, five recovery
fault primitives (including inode replacement/interrupt-clone cancellation) and
strict affected Clippy passed. GCS scratch admission32MiB/source2MiB boundary
tests pass before helper/auth.32MiB remains a candidate reserve, not a qualified
worstcase pair-wide highwater bound. Supported-target/installed helper acceptance remains open.

Subsequent bounded-IPC checkpoint: commandframes2MiB+64KiB, aggregate command
strings2MiB, raw tagged2MiB responses without numericJSON staging and GCS decoded
metadata8MiB preflight reduce the earlier amplification. Native109passed/
17ignored, five recovery and four isolated descriptor/self-base slices passed;
synthetic concurrent owners/cancel/fresh reopen test passed. Live pull/self-
preparation at `~/grv-shipping-gcs-67d7npml` passed and one owned generation/
bucket cleaned, empty versions404. After independent review, preallocation
borrowed argument guards, structuralJSON preflight, exact schema-shape validation
and single-buffer descriptor remapping were added; native112passed/21ignored,
26IPCunit and seven explicit recovery/setup slices pass. Latest guarded helper
live pull/self-preparation at `~/grv-shipping-gcs-_791jmu6` passed/emptyversions404. The full-held raw-response IPC rerun passed1839.11s at
`~/grv-held-gcs-9zqrg2qn`; emptyversions/bucket404. Result/cleanup and post-run
helper/library digests verified; pre-run hashes absent. This pass uses the
earlier raw-response helper, not the latest borrowed admission/JSON preflight
guards compiled separately. Abort/prune/release/expiry acceptance remains open.
See [owner memory checkpoint](gcs-owner-memory-checkpoint.md) and
[peer refusal checkpoint](gcs-peer-refusal-checkpoint.md).

Configured GCS native metadata now streams bounded output instead of retaining
the full SELECT result. Reviews exposed that stock StreamClose does not EndQuery;
partial refusal now interrupts/fetches once before Close, with positive QueryEnd
witness/error revocation. Billion-row prefix refusal, oversized strings,
multichunk/scalar and rollback recovery tests pass. Native112passed/22ignored,
five isolated/seven recovery slices and nine ctest proofs pass. Latest live
pull/self-preparation `~/grv-shipping-gcs-d9labe3i` and65536-row ZSTD scanner
`~/grv-shipping-matrix-_ymm6gwa` passed/cleaned404. The4MiB adoptedchunk guard is
post-fetch and64KiB stock buffer is soft: upstream source/expression allocations
and cleanup-fault injection still require qualification. Prior full-held pass
predates this native change. See [native metadata streaming checkpoint](gcs-native-metadata-streaming-checkpoint.md).

Native guard revision5 now distinguishes proved post-secret setup revocation
from uncertain cleanup requiring whole-owner exit/fence. Synthetic signed setup
faults after secret/policy/filesystem installation, forced DROP failure, and
helper exit-status slices pass; read admission stays revoked. Native113passed/
22ignored, seven recovery artifacts and10ctest proofs pass. Latest live pull/
self-preparation `~/grv-shipping-gcs-6h97jpuz` passed/emptyversions404 after
completed review's inspection-completion/preamble containment fixes. Fullsecret
canary/freshrealOAuth and memory/deadline gates remain open; prior held/scanner
records retain their actual revision4 labels. See
[setup revocation checkpoint](gcs-setup-revocation-checkpoint.md).

S3 catalog initialization blocker was fixed with the exact pinned HTTPFSParams
subtype constructed without request-time settings/secret lookup; signed stock
client transport, exact authority, GRV SigV4 and conditional reads remain. Two
**8192-row diagnostic** slices passed ten local oracles and guard denials each,
with99.4253%/74.8567% and98.5184%/73.8854% projection/selective-wide content savings.
All owned objects/bucket cleaned404. Prior full-size failures/timeouts and cleanup recoveries remain archived. Both
full65536-row runs now **passed** ten local oracles/denials each:
99.4253%/96.6900% and99.0238%/95.9295% projection/selective-wide content savings,
private `~/grv-s3-shipping-vku_mrsd`, two generations deleted/all inventories empty/
bucket404 and unchanged artifacts across run. See
[s3-shipping-qualification-blocker.md](s3-shipping-qualification-blocker.md).
These are partial slices, not a release matrix or complete preparation traffic.

## Remaining release gates

Independent revision-4 security acceptance and complete shipping GCS matrix;
protected S3 shipping pushdown measurements (now explicitly authorized profile
`lexiupon-aws-cli-s3`, region `eu-west-1`, newly owned synthetic buckets only;
execution/evidence tracked separately); held GCS abort/prune/release/expiry
lifecycle closure; approved dedicated GCS owner-process implementation and
in-flight timeout and
cancel/commit races; faults/resources/overwrite behavior; ordinary DuckDB
consumption; supported-target/MSRV/package/install qualification. Capability
advertisement and release remain blocked. No implicit publication approval.
