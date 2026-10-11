# GCS implementation checkpoint

Status: implementation in progress; not a release qualification or capability
advertisement. No push, tag or publication is recorded by this checkpoint.

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

## Remaining release gates

Independent revision-4 security acceptance and complete shipping GCS matrix;
protected S3 shipping pushdown measurements (explicit authorized profile/root
not yet selected); held GCS abort/prune/release/expiry lifecycle closure; in-flight timeout and
cancel/commit races; faults/resources/overwrite behavior; ordinary DuckDB
consumption; supported-target/MSRV/package/install qualification. Capability
advertisement and release remain blocked. No implicit publication approval.
