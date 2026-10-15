# Package32 narrow real shipping-FFI gate — parent-reviewed execution plan

**Status: parent-reviewed plan; execution/results pending. Planning is not
acceptance.** This is within the already authorized sequential package32 scope.
No release/publication/capability/default change; no Task25 faults, Task27 held
lifecycle or Task26 S3 matrix. The original draft was incomplete at the agent
turn limit; the parent corrected code, claims, and the driver before approval.

## Boundaries and actual tests

1. `native::descriptor_tests::real_shipping_gcs_route_repeated_reads_and_unadmitted_url_refusal`
   uses the rebuilt Rust library test binary and the unchanged shipping native
   dylib, not a source-including mock. It starts the original300s lease **before**
   the real independently selected force-refresh provider; configuration cannot
   renew it. Fixed owned URLs A/B/A return111/222/111 through shipping FFI and
   actual signed HTTPFS HTTPS. The final query references B while only A is
   admitted and must refuse. The source-including proof establishes pre-dispatch
   denial; this real-route test observes the refusal but has no public request
   counter and cannot independently count zero rejected HTTP requests.
2. `build::tests::live_nonempty_gcs_external_self_prepare_reopen_and_cleanup`
   is run separately on the same uniquely owned bucket's id42 fixture. It
   retains actual full preparation/external/self/reopen qualification. It is
   not the existing pull refresh/cloud-fault matrix or a held lifecycle.
   This existing test also includes its local precommit preparation refusal and
   source-free abort/outcome/cleanup branch; those scoped assertions are
   disclosed, not new cloud replacement/missing-object or published-hold tests.

The new FFI test uses actual fixture size/SHA/generation supplied by the
protected driver; the descriptors do not waive SHA/size/schema/row preparation.
Its below-SDK A/B/A queries are not a full preparation proof themselves. Explicit
casts and `hive_partitioning=false, union_by_name=false` are retained. Source-
including auto-rebind/cache/retained-handle assertions remain separate offline
qualification; this HTTPS gate does not claim automatic rebind, pruning, cache
hits, request counts or parallel-cloud acceptance.

After joined synchronous work and owner close, the test reacquires the actual
WorkspaceLock and scans all present regular workspace artifacts for the memory-
only bearer, requiring the DB to be present. Inventory/read errors or unexpected
entries refuse. That scan does not prove argv/environment history, arbitrary
logs, universal heap erasure, or arbitrary escaped prepared-owner destruction.

## Named frozen artifacts / build boundary

Driver: `/tmp/grv-task32-real-route.py` (default offline plan only).

A protected0600 freeze file under `/tmp/grv-task32/` must match these named paths:

- `/Users/jiacao/.grv-native-work/owner-validation-target/debug/deps/grv_adapter_duckdb-273b55543ed855e5`
- `/Users/jiacao/.grv-native-work/cmake/libgrv_duckdb_guard.dylib`, required SHA256
  `2709cc619eb125d1ecdc48377303019ea20bfe32f20c4e964c298b2b66abdd5f`
- `/Users/jiacao/.grv-httpfs-prototype-extensions/osx_arm64/{httpfs,aws}.duckdb_extension`,
  exact bytes and hashes checked against `notices/native-extensions.json`
- driver, this plan, `src/{native.rs,build.rs,gcs_token.rs}` and the manifest.

Final build/offline checks precede freeze. The binary's exact test list must
contain both symbols exactly once. Each launch uses `--ignored --exact
--test-threads=1` and must report exactly one passing test. Hashes are checked
before/after each launch and at lifecycle exit. No Cargo, edit, rebuild or
parallel native/conformance run while the live lifecycle owns resources/native
work. Changed artifacts fail the stage; earlier held evidence does not qualify
this new binary.

## Identity, credentials and transport

Only `jiacao@on.sinch.com` / `entit-finance-prod`, project number828148855924.
The driver refuses ambient/configured auth overrides and independently validates
selected config-helper core selectors, bearer spelling and at least420s actual
expiry. Real force-refresh acquisition is memory-only; no token in argv/env,
raw helper output, logs, receipts, views, digest, profiling or durable JSON.
Stock/helper copies are not universal zeroization proof. Operator gcloud config
is trusted configuration, not independently pinned named-config identity.

Driver writer/cleanup REST calls use explicit selected gcloud acquisition,
fixed `https://storage.googleapis.com`, verified TLS, no proxy, no redirects,
no retry, finite per-call timeout and bounded duplicate-refusing JSON. The
shipping reader follows its existing exact read-only policy and cooperative
native deadline contract. No keys, impersonation, IAM/ACL/policy/versioning
changes or new reader transport. No retry of failed tests/effects or renewal of
the reader execution lease. Fresh acquisitions need not return different values.

## Resource ownership / intents / cleanup

`--execute --freeze <reviewed0600file>` creates a new home evidence directory0700
and protected atomic0600 state/intent/evidence files. No credential acquisition
in default plan mode. A selected acquisition failure stops before resource
intent/mutation; its cause stays unclassified and no404 is fabricated.

- Unique name `grv-httpfs-oauth-<24lowerhex>`. Absence404 must precede create.
- Durable owned marker/create intent precedes the create POST.
- Initial **new bucket only** settings follow the previously approved scaffold:
  EUROPE-WEST1, uniform bucket access enabled, public access prevention enforced,
  soft-delete retention0, one Delete lifecycle rule age1, unique owner label.
  These are creation settings, not changes to existing resources.
- Marker and project number are checked on the created bucket.
- Three fixed local Parquet files with same BIGINT id schema:111,222,42.
  All upload intents (exact key/size/SHA) are protected before upload;
  `ifGenerationMatch=0`; returned name/size/generation validated and journaled
  after each upload. Recorded known generations must match cleanup inventory.
- Automatic `finally` cleanup after any creation attempt, including failures.
  Recovery command if cleanup cannot finish:
  `python3 /tmp/grv-task32-real-route.py --execute --cleanup-state <private/state.json>`.
- Marker/project check before deletion; strict paginated all-version inventory
  validates field shapes, unique entries and no cyclic page token. Unknown keys,
  sizes, duplicate versions or mismatched recorded generations refuse deletion.
  A known upload-intent whose response was lost may be recovered only as its
  single exact-size generation in this uniquely owned bucket.
- Generation-qualified conditional deletion, observed **complete empty
  all-version inventory**, then bucket deletion and observed404.
  If an already absent bucket is observed during recovery, absence is recorded
  without inventing an unseen inventory. Cleanup error retains journal; no
  unknown-resource deletion, wildcard cleanup or false success.

## Parent corrections to the incomplete draft

The parent removed implicit untrusted binary discovery, manual lifecycle split
(preparation leaving resources alive), fabricated empty inventory on404,
non-strict pagination, placeholder VerifiedFile metadata, inaccurate whole-host
credential claims, missing exact-run verification, and missing pre/post launch
hash enforcement. Real test uses protected repo-local tempdir to satisfy native
ancestor trust, source descriptors validated before acquisition, and no runtime
production changes. Offline driver tests and final native/strict checks must be
recorded before execution. Any development failure remains evidence, not a
shipping defect or silently discarded stage.

## Outcome still required

Only actual exact live passes, stable frozen artifacts, complete owned cleanup
and parent final source/evidence review can close package32. A plan alone cannot.
Cooperative interrupts remain requests, not hard native stop/join/rollback-at-
expiry guarantees. No stock per-range generation pinning or privileged
replacement protection is established. Broader package45 and later packages
remain separately scoped and open.
