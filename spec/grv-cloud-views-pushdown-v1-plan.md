# Keyless GCS HTTPFS views and cloud pushdown: implementation and release plan

|                 |                                                                                                                                                                |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Status          | Implementation in progress; native guarded pull live proof passed; capability remains unadvertised                                                             |
| Authority       | [RFC](grv-cloud-views-pushdown-v1-rfc.md)                                                                                                                      |
| Evidence        | [Stock HTTPFS OAuth live proof](../prototypes/cloud-views/httpfs/oauth-live-evidence/README.md); [historical experiments](../prototypes/cloud-views/README.md) |
| Release process | [Release-validation catalog](release-validation/README.md), [development](../docs/development.md)                                                              |

## 1. Direction and boundaries

Use signed pinned DuckDB HTTPFS's **HTTPS reader with temporary HTTP bearer
secrets**, not the GCS HMAC reader. Initial provider: explicitly selected gcloud
account/project, matching the available production access. No HMAC/private-key
creation, organization-policy exception, ambient account discovery, automatic
impersonation, general credential broker or new GCS network transport.

Keep external `gs://` identities; trusted code translates exact object bytes to
canonical storage.googleapis.com HTTPS filenames. No globs/Hive inference.
Local materialization stays default; `gcs-view` is opt-in complete identity
replacement. Existing protected S3 behavior is unchanged. Full-object verified
preparation remains distinct from later query payload savings. GCS query-time
identity is **best-effort**, not generation-pinned; holds prevent normal GRV
pruning, not privileged out-of-band replacement.

Revise pre-1.0 contracts/peers together, rejecting incompatible state/packages
clearly without deletion/reset/reinterpretation. No dual-version install,
legacy migration or backwards-compatibility layer. Publishing needs separate
approval; this request authorizes planning/committing, not publication.

## 2. Evidence already complete versus remaining gates

- Completed: actual stock HTTPFS OAuth HTTPS live proof, ten oracle scans and
  one default production-owner denial probe; query projection **96.96%** and
  selective-wide **93.59%** payload savings on fixed fixtures. Special names,
  empty/evolved/no-statistics correctness and exact test-resource cleanup.
- Implemented, not release-qualified: coordinated GCS access/provenance records,
  full-object preparation, authenticated Requester Pays refusal, pure URI mapping,
  guarded stock HTTPFS pulls and held-build dispatch. An owned live one-row native
  pull passed exact/nested/glob denials, source-free reopen replay and empty refresh.
  Guard revision 3 adds a pinned engine pre-request hook retaining stock transport:
  no late second request, wrong URL or write request in deterministic proof.
  A synthetic-token native watchdog test interrupts computation and rolls back.
- Not complete: revision-3 live rerun (selected gcloud login now refuses token
  acquisition), complete cloud projection/predicate matrices, request cancellation
  races and bounded in-flight transport behavior, held-build/fault/resource tests,
  ordinary DuckDB consumption and supported-target package/install qualification.
- Requester Pays: explicit header and no-header both succeeded for the selected
  principal. Billing attribution/mandatory enforcement is unproven. Initially
  **refuse Requester-Pays view inputs** unless a separate qualification gate
  proves the production contract under an approved suitable identity.
- Prototype HTTPState instrumentation uses pinned internal layout. It is not a
  public metrics API or automatic qualification of the shipping adapter.

## 3. Work order and critical dependencies

Each ID is one reviewable PR/work package; split further if review size requires.
A task is complete only after its acceptance gate passes and evidence is linked.

```text
I1 contracts + expiry/URI/security decisions
 ├─ I2 explicit OAuth provider + trusted HTTPFS setup
 ├─ I3 coordinates + verified preparation + protocol/schema revisions
 └─ I5 existing S3 shipping-path pushdown baseline
I2 + I3 → I4 guarded GCS pulls + transactions + replay
I4 → I6 held external builds + independent DuckDB consumption
I4 + I5 + I6 → I7 shipping-path cloud/fault qualification
I7 → R1 packages + docs + capability activation
R1 → R2 release candidate + install/validation evidence
R2 → R3 approved publication + post-install checks
```

Subprocess conformance builds run serially per Cargo target directory; use
separate target directories for independent work. Cloud/IAM/resource effects
require explicit identity/project/scope authorization for each test session.
Never infer production publishing or new IAM authority from this plan.

## 4. Implementation tasks

### I1 — Finalize keyless contracts and release-blocking decisions

**Touchpoints:** RFC, client/execution/protocol specs, proposed reader selector,
source-coordinate model and guard design.

- Specify initial provider/account/project/scope schema and error ordering;
  reject ambient impersonation/credential/token overrides before network/effects.
- Use the implemented gcloud config-helper force-refresh projected JSON
  token-plus-expiry contract: explicit account/project, preflight/response auth
  override refusal, bounded private output, zeroizing buffers. Live token/expiry
  acquisition passed. No guessed one-hour lifetime. Enforce at most 300 seconds
  anchored before acquisition, expiry beyond budget plus 120-second margin.
  Worker/native watchdog remains an integration release gate.
- Set token refresh between executions only, deadline cancellation/rollback,
  unknown/insufficient lifetime rejection, and no mid-query secret replacement
  or automatic retry after results/effects. Define ordinary DuckDB expiry limits.
- Finalize canonical HTTPS mapping and fixed-host/no-redirect policy. If the
  pinned reader cannot prevent credential/authority leakage through redirects,
  settle a minimal safe restriction or defer the capability; no custom transport
  expansion without an explicit decision.
- Document best-effort identity, initial Requester Pays refusal, capability and
  coordinated pre-1.0 durable/wire changes.

**Acceptance:** reviewed expiry/authority/encoding decisions with executable
fixture test vectors; no remaining HMAC-required runtime contract; unresolved
security/lifetime choices remain blockers, not vague future work.

### I2 — Implement explicit OAuth selection and trusted HTTPFS setup

**Touchpoints:** `crates/grv-storage/src/cloud/credentials.rs`, DuckDB
`src/s3_config.rs` as precedent, proposed GCS config/provider, `src/native.rs`,
`src/native_extensions.rs`, native setup and worker ownership.

- Named provider returns token plus trusted expiry; privately transport to an
  exclusively owned connection, fresh before bounded scan execution.
- Required explicit selector and safe private config ownership/ancestor/link,
  closed-schema/size validation. No user-supplied arbitrary command execution.
- Reject effective-identity overrides and unrelated ambient HTTPFS credentials.
- Load packaged signed pinned HTTPFS; disable persistent secrets, unsigned
  loading, runtime INSTALL/autoload. Use temporary `TYPE http, BEARER_TOKEN`.
- Redact setup SQL/errors/traces/catalogs/profiling; no Authorization in the
  unredacted extra-header map; destroy secrets and token buffers on completion.
- Implement expiry deadline/cancellation coordination, reacquisition between
  executions, and clear denied/expired/unknown-lifetime failures.

**Acceptance:** provider mocks for cached/expired tokens, unknown expiry,
insufficient lifetime, ambient overrides, lost credentials and cancellation;
secret-leak canaries, escaping and concurrency-isolation tests; no credential
change affects source-free inspection/replay.

### I3 — Revise access contracts and canonical verified preparation

**Touchpoints:** adapter API/host/SDK/wire, DuckDB registration/binding/manifest
schemas, storage/cloud coordinates, `crates/grv-core/src/source.rs`, CLI pull.

- Add `gcs-view` access/materialization and revise closed records/peers together.
  Reject stale packages/durable metadata before effects; no migration/reset.
- Keep canonical gs:// provenance separate from exact native HTTPS scan filename.
  Shared vectors: UTF-8, apostrophe, percent/encoded-looking names, spaces/plus,
  wildcard/brackets, equals/@, slash/dot segments, malformed escapes and aliases.
  Reject any unsafe or unrepresentable key explicitly, never reinterpret it.
- Reuse tombstone, full-object hash/size/schema, exact-value/partition and row
  checks. Preserve preparation fallback and S3/local verification semantics.
- Discover Requester Pays metadata and refuse view mode unless qualified.
  Persist selector identities, not tokens; preparation/query principals explicit.

**Acceptance:** wrong mode/root/selector and incompatible metadata fail before
auth/mutation; shared mapping vectors hit exact bytes; integrity regressions
pass, no weakening of full-object preparation to inflate pushdown evidence.

### I4 — Integrate guarded GCS pulls, refresh and source-free replay

**Touchpoints:** DuckDB `src/pull.rs`, runtime/worker, `src/native.rs`, native
`grv_guard.hpp`, `pull.inc`, `shim.cpp`, staging scopes and host transactions.

- Reuse exact per-file projections/casts/typed-null UNION and typed empty views,
  hive_partitioning=false and union_by_name=false.
- Positive trusted scans may open only admitted exact HTTPS paths; enforce
  read-only authority through initial bind, nested scans and automatic rebind.
  A scoped secret/allowed_paths alone is not the production sandbox.
- Deny unlisted/glob URLs, redirects, writes, secret/catalog/config access,
  new readers/extension setup and callback/macro/nested-view escape routes.
- Bound bind/execute to token deadline, network/memory budgets and cancellation;
  prevent partial effects if token expires or a late table fails.
- Preserve transactional multi-table refresh, owner/checkpoint/receipt atomicity;
  include reader/materialization contract in receipt identity.
- Current-contract replay/inspection must execute before source/config/auth;
  lost receipts refuse, no repair or stale-state reinterpretation.

**Acceptance:** positive GCS pulls plus adversarial denials/rebind/expiry races;
empty/evolved files, late rollback/crash-commit recovery; replay with source and
credentials/config unavailable. S3 guard/resource tests remain unchanged.

### I5 — Establish existing S3 production pushdown matrix

**Touchpoints:** `native/verified_s3.inc` test counters, native/pull/conformance.

- Instrument approved bounded test counters by preparation, bind/footer, query,
  EXPLAIN/retry/overfetch; no unreviewed public output/ABI change.
- Correctness oracle and semantic plans for comparisons, ranges, AND/OR/null,
  string/boolean/date/decimal/timestamp, wrapper views, partition columns,
  evolution, nonidentity casts and no-statistics cases.
- Cold/warm/new connection and compressed/dictionary/wide/many-file fixtures;
  value-consuming full baselines, not metadata-only count(*).

**Acceptance:** actual protected S3 shipping path passes correctness and reviewed
relative savings gates; local/custom mocks are supplemental, not substitutes.
Do not replace S3's reader with stock HTTPFS.

### I6 — Support held GCS external builds and ordinary DuckDB

**Touchpoints:** core build holds/preparation, DuckDB build/runtime/worker,
driver/completion/recovery and external-consumer docs.

- Fixed exact admitted file sets/private self-base views with normal GRV holds;
  tracking refresh cannot change accepted inputs. Managed builds stay local.
- Test pruning/retention, accepted export restart/late failure and terminal replay.
- Ordinary DuckDB independently obtains explicit OAuth and temporary HTTP secret;
  no token in saved views/database, no automatic external-process renewal claim.
- Document best-effort object bytes, expiry, unavailable/pruned objects, and
  independently configured consumer authority without GRV guard guarantees.

**Acceptance:** held-input lifecycle/recovery and independent DuckDB queries
pass, including empty/evolution/special filenames, missing/expired token and
pruning, without custom external readers or leaked secret SQL.

### I7 — Qualify shipping GCS/S3 cloud, fault and resource behavior

- Exercise actual production scanner paths with local oracles, real payload/call
  counters and semantic plans. Verify HTTPFS metrics against independent tests;
  avoid an unversioned internal class-layout dependency in production telemetry.
- Cover the I5 matrix on GCS, token deadlines, no renewal during open handles,
  revoked/denied credentials, network/range failures, missing/replaced objects,
  many files, retries/cache/overfetch, cancellation and late refresh failure.
- Observe out-of-band overwrite behavior; do not invent generation pinning.
- Separate preparation/bind/query/EXPLAIN totals. Reviewed relative thresholds
  only on fixed qualifying fixtures; unsupported optimizations remain correct.
- Initial Requester Pays refusal tested. Supporting it is optional separate
  approved-identity enforcement/attribution qualification, not a release shortcut.
- Disposable resources with atomic ownership journals/markers, recovery and all
  version cleanup/absence proof. No unapproved IAM/key/policy changes.

**Acceptance:** correctness/security/resource gates on the shipping paths and
redacted reproducible evidence; no substituted custom-reader percentages.

## 5. Release tasks

### R1 — Package, document and activate only qualified capabilities

**Dependencies:** I1–I7 passed.

- Reuse pinned signed HTTPFS artifacts, native guard revision and integrity
  inventory; no runtime downloads/new system libcurl. Relocation, signatures,
  hashes, notices/provenance on each advertised supported target.
- Update client/execution/protocol/public schemas, CLI help, cloud docs, adapter
  README and embedded skills together. Examples select account/project and never
  persist tokens. Explain bounded queries, preparation cost, source-free replay,
  best-effort identity, holds/pruning and Requester Pays refusal.
- Advertise `gcs-view` only on qualified native combinations; explicit unsupported
  errors elsewhere. Choose next pre-1.0 version after reviewing protocol/native
  guard changes; do not reuse a published artifact/version or silently reset state.
- Add named gates to the existing release-validation catalog and artifact/evidence
  inventory. Candidate build/activation is not external publication approval.

**Acceptance:** full workspace/native/conformance and packaging/script suites,
strict lint/MSRV/supported-target CI; relocated install capability tests.

### R2 — Build and qualify an immutable release candidate

Use existing tools/catalog, not a parallel release framework:
`scripts/package.py`, `verify-bundle.py`, `audit-release.py`, `run-validation.py`,
`check-release-validation.py` and `archive-release-evidence.py` as documented in
[release validation](release-validation/README.md). Confirm flags from each tool
before running; credentials/config/evidence must remain private/redacted.

- Clean-tree reproducible candidate and manifest hashes/notices per target.
- Installation from candidate, adapter discovery, offline/online explicitly scoped
  smoke gates, keyless GCS view refresh/query/replay and existing S3/local checks.
- Record supported/unsupported combinations, gate failures/waivers and exact
  cleanup. Archive evidence tied to artifact hashes and commit; no secrets.
- Prepare release notes/change log, compatibility warning, supported targets,
  checksums and rollback instructions. Older binaries are not promised to read
  new durable state; retain matching workspace backup/export or use local mode.

**Acceptance:** all mandatory catalog gates pass (no unexplained skips), reviewer
sign-off, evidence complete and candidate hashes fixed. Publication remains held.

### R3 — Obtain approval, publish and verify installation

- Obtain explicit publication approval for exact version/commit/artifact hashes.
- Publish immutable release assets/checksums/notices and update install channels
  (including Formula where appropriate) using existing release procedures.
- Verify clean installation/discovery, pinned extension integrity and live keyless
  smoke with separately approved test scope; delete owned cloud fixtures.
- Archive post-install evidence and record failures. On failure withdraw/disable
  the new channel/capability and restore prior immutable artifacts; do not mutate
  published binaries, broaden IAM or automatically migrate/reset user state.

**Acceptance:** approved public artifacts match qualified hashes, clean install
smoke passes, cleanup confirmed, release inventory/status updated.

## 6. Definition of done and first batch

A user with the permitted production OAuth access can prepare/refresh/query an
exact `gcs-view` without keys or policy changes. Guards, token lifetime,
transactions/holds/replay and packaged ordinary consumption pass. Eligible
filters/projections save measured bytes on qualified S3/GCS fixtures without
weakening verified preparation or promising universal pushdown/generation pins.

**Start I1**, then I2/I3/I5 in parallel after contract decisions. I4 is the main
integration bottleneck. R1–R3 stay blocked until shipping evidence passes; no
production feature implementation or release publication is performed by this
planning commit.
