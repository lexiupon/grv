# First release validation plan

Status: **first macOS validation pass authorized by user review; incremental
coverage mapping and bug-fix/retest are in progress; publishing is not authorized**.
Current target: macOS ARM64 on the available host, with client v1/storage v2/adapter
interface v1 tests and an initial macOS release candidate after passing and
fixing defects. macOS x86-64 and Linux x86-64/ARM64 are future qualification
cells, not blockers for this initial pass. Proposed package version: `0.1.0`.
Do not claim four-platform qualification or `complete_client_v1: true` from
a macOS-only or partially mapped pass.

## Operational notes and scoped initial-release gates

The reusable [validation operations index](release-validation/README.md) now
tracks the six scoped macOS initial/preview release gates, rerun policy,
command catalog, current status and historical evidence index. It separates
previous development passes from immutable candidate qualification. Missing
historical source/binary/time provenance is explicit; prior passes are not
relabeled as evidence for later bytes. Small releases always run the baseline
candidate/package checks and add change-triggered provider/safety gates.

The scoped preview policy does not replace the full-release pass rules below
or weaken normative results. Final advertised integrations and limitations
must be reviewed before freezing a candidate; publishing remains separate.

## Decision and authority

Run reviewed named tests on macOS ARM64 and improve exact scenario/assertion
mapping alongside the first pass. Add live crash/response-loss/race tests and
fix/retest defects. Then freeze a candidate, rerun affected gates, review
evidence and initial macOS release limitations; publishing needs a separate
decision. Full-v1/four-platform qualification remains the longer-term target. Finding a defect starts an implementation-and-retest cycle; it does
not justify changing a required result to match the implementation.

[Client v1](grv-client-v1.md), [execution semantics](grv-client-v1-execution.md),
[process protocol](grv-adapter-protocol-v1.md) and [storage v2](grv-storage-v2.md)
remain authoritative. This plan supplies procedures and evidence requirements;
it does not change those contracts. Crypto-shredding and explicitly deferred
features remain excluded. Unadvertised optional capabilities need a documented
applicability decision, rather than an accidental skip.

The [scenario register](fixtures/release-validation/scenarios.json) captures
every required execution and protocol table row: 108 execution scenarios and
61 protocol scenarios. Its initial candidate file mappings are search aids,
not claims of coverage. During the authorized first pass, each exercised row gains an exact
automated selector, assertions proving its required result and release-case
links. Mapping may develop alongside execution, but must be reviewed before
claiming that row passed. Several assertions may share one test,
but each required result must remain independently assessable.

## What counts as a test

Use three complementary forms of evidence:

| Form                                | What it establishes                                                                                                                                                                 |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Automated conformance               | Every normative scenario, including malformed peers, exact conversions and deterministic faults. Run first on macOS ARM64; retain other platform cells for later qualification.     |
| Live lifecycle                      | Packaged production CLI and adapters against an actual DuckDB database, Salesforce org, S3 bucket or GCS bucket. Independently verify results and persisted state.                  |
| Live service with controlled faults | Real service effects with a deterministic barrier, process death, or a wrapper that suppresses a response after the actual effect. Demonstrate recovery from that precise boundary. |

A mock does not establish provider semantics. A successful live smoke test does
not establish crash safety. In-process/core tests supplement the packaged CLI
gates. Fault-only fixture capabilities must not appear in production manifests.

Previous local and live runs are useful baseline evidence. They do not approve
this plan, cover its unmapped cases, or qualify a subsequently changed candidate.

## Environment and permitted effects

Create a protected environment worksheet outside GRV and outside version
control. Record identifiers, executable versions, regions and allowed effects;
obtain credentials from the existing local stores. Never put credentials in
this specification, command arguments, transcripts or release artifacts.

| Target                     | Current availability                                                                                                                                                                         | Required boundary                                                                                                                                                                                                                                                                                                                                                                                                    |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Local                      | Available                                                                                                                                                                                    | Fresh protected directories, databases and state for each case. No existing application database.                                                                                                                                                                                                                                                                                                                    |
| S3                         | Dedicated staging prefix and named profile supplied                                                                                                                                          | Every mutation below a freshly generated child prefix. DuckDB independently selects the approved existing profile and performs only reads.                                                                                                                                                                                                                                                                           |
| GCS read                   | Existing complete `xyz/` fixture supplied                                                                                                                                                    | Read, list and verify only. No writes, deletes, uploads or permission changes in that prefix. Its current three-row and empty tables suffice for the read gate.                                                                                                                                                                                                                                                      |
| GCS write                  | **Authorized:** `gs://validation-gcs-bucket/grv-release-validation/`                                                                                                                         | Only fresh owned UUID children: create, conditional update, streaming upload, list/read and delete. Never mutate `xyz/`, other prefixes, bucket settings, IAM or retention. Actual provider permissions still need preflight.                                                                                                                                                                                        |
| Salesforce                 | Disposable fixture org `00D000000000001AAA`: scaled 1,010 rows strictly verified; acquisition and fixture-scoped mutations available; storage is not an execution blocker by review decision | Authentication, Describe, approved read queries, and CRUD confined to the dedicated fixture namespace. Bulk query-job creation is a service effect that changes no source records. During fixture provisioning only, deploy and assign the fixture-scoped `GrvFixAccess` permission set to the fixture user. No changes to global org settings, user profiles or non-fixture records; no token revocation or logout. |
| Salesforce fixture changes | **Authorized within the fixture namespace**                                                                                                                                                  | The org is ours to provision and discard; the reviewed mutation/reset procedure is `reset.sh` in the fixture directory (delete fixture records, reload pristine data). Storage is not an execution blocker by review decision; any actual reset/load failure still fails the case and must be reported.                                                                                                              |
| Platform runners           | macOS ARM64 available; other cells deferred for this initial pass                                                                                                                            | Record host/toolchain/native/artifact hashes and logs. macOS ARM64 results do not establish other-platform qualification.                                                                                                                                                                                                                                                                                            |

The worksheet uses `GRV_S3_TEST_ROOT`, `AWS_PROFILE`, `AWS_REGION`,
`GRV_DUCKDB_S3_READ_PROFILE`, `GRV_DUCKDB_EXTENSIONS_DIR`,
`GRV_GCS_TEST_ROOT` (read-only), `GRV_GCS_WRITE_TEST_ROOT`
(`gs://validation-gcs-bucket/grv-release-validation/`), `GRV_GCS_ACCOUNT`, `GRV_GCS_PROJECT`,
`GRV_SALESFORCE_TEST_ORG`, `GRV_SALESFORCE_TEST_ORG_ID`,
`GRV_SALESFORCE_TEST_IDENTITY` (`salesforce:<18-character-org-id>` for the
adapter live-service selector), `GRV_SALESFORCE_TEST_TRANSPORT` (`auto`,
`rest` or `bulk`) and `GRV_SALESFORCE_TEST_ADAPTER`. Adapter read configuration remains separate from
the GRV backend configuration, even when the approved profile names coincide.

Each case owns a UUID child prefix, records that ownership before effects and
checks every cleanup key against it. Cleanup lists and removes only owned
objects, aborts owned outstanding uploads, then proves absence. It never deletes
a parent prefix, shared fixture, coordination record of another case or source
record. A failed cleanup is reported separately and retried using the recorded
ownership. Interrupted uploads need explicit cleanup evidence; an empty object
listing alone cannot prove that multipart uploads are gone.

Record expected request counts and upload bytes before running a case. Larger
fixtures are generated locally. The initial live dataset limit is 1 GiB per
case; larger scale runs need a reviewed resource budget. Isolate time/lease
tests using supported test-root parameters or explicit fault barriers; never
force-expire an unrelated live owner or rely on an arbitrary sleep as proof.

## Provisioning a disposable Salesforce org

The Salesforce source for the live cases is a **disposable org that we
provision and discard**, not an existing account. The versioned fixture
definition lives in
[spec/fixtures/release-validation/salesforce/](fixtures/release-validation/salesforce/README.md):
object/field metadata, the deterministic value generator, the independent
value manifest, the four push declarations, and the provisioning and reset
scripts.

Requirements and procedure:

- The org must support custom objects. The tested Base org rejected custom
  objects; the Developer Edition fixture is deployed and verified. **Storage
  quota is not a readiness blocker by explicit review decision.** Retain
  actual service failures and cleanup outcomes as evidence; this decision
  does not turn a failed load/reset into a pass. Use the explicitly approved
  `--ignore-storage-limit` provisioning/reset flag for this pinned disposable
  org; it bypasses only the conservative preflight, not service errors.
- Deploying fields does **not** grant field-level security. The initial
  missing-field diagnosis was incorrect: assigning `GrvFixAccess` using
  `sf org assign permset --name GrvFixAccess --target-org grv-fixture`
  made all fields readable. The script deploys object/field permissions,
  idempotently assigns the set, then verifies live object/field access.
  Missing Describe/SOQL fields do not establish a metadata activation defect.
- A personal Dev Hub may sponsor an isolated, auto-expiring scratch org via
  `sf org create scratch --target-dev-hub grv-personal-hub`. Enable Dev Hub
  explicitly and check scratch permissions, quota and resulting storage.
  Missing `ScratchOrgInfo` does not prove an org defect. Do not use a
  production Dev Hub without separate authorization.
- The org must be one we can abandon: no production data, no shared users.
  Authorize it in the local `sf` store under the alias `grv-fixture` (the
  alias the fixture declarations reference) and record its username and
  18-character org Id in the worksheet (`GRV_SALESFORCE_TEST_ORG`,
  `GRV_SALESFORCE_TEST_ORG_ID`). No other Salesforce identity — in
  particular no production org — is authorized in the store the validation
  uses.
- Provision with `python3 provision.py --org grv-fixture --expected-org-id
"$GRV_SALESFORCE_TEST_ORG_ID"`: it checks the pinned org identity, deploys
  schema/permissions, assigns access, checks capacity unless explicitly waived,
  bulk-resets/loads both
  fixture tables, resolves lookups and strictly verifies all 1,010 rows.
  `--verify-only` verifies without mutating the org. Evidence records actual
  REST pages, counts, API version, limits and readback status.
- Current fixture: disposable org `00D000000000001AAA`, scaled by user
  review to 1,000 main + 10 related rows (manifest version 3). Clean reload
  and mutation/reset were strictly verified on 2026-10-07. Earlier 10,100-row
  evidence and actual reset storage failures remain historical, not current
  fixture state. The reduction preserves scalar boundary rows and three
  partitions; this is correctness, not performance qualification. Supported
  REST batchSize=200 and test Bulk maxRecords=100 preserve real locator
  boundary evidence without changing production defaults or decoder budgets.
- On a first run against a fresh org, retrieve the deployed metadata and, if
  it differs from the committed seed, commit the retrieved form as the
  canonical schema.
- Mutation and reset for the snapshot-change cases use the fixture's reviewed
  procedure (`reset.sh`): delete all fixture records, reload pristine data.
  The post-mutation expected state is the manifest with the stated mutation
  applied.
- Disposal is the cleanup: when the org is abandoned, its records, Bulk jobs
  and configuration die with it. Nothing in the fixture outlives the org, and
  nothing in the repository depends on a particular org's record Ids.

The procedure is repeatable: a new disposable org plus this directory
reproduces the fixture value-for-value (record Ids are org-assigned and are
not part of the oracle).

## Fixtures and independent oracles

Version the fixture definitions and expected outputs before executing them.
The oracle uses fixed literals, independently decoded Parquet and ordinary
DuckDB queries. Calling the same GRV conversion or comparison routine twice is
not an independent result check. Exact decimals and integer/timestamp values
are represented as strings or byte encodings in oracle JSON.

| Fixture        | Required shape and oracle                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scalars`      | Every client-v1 primitive, nullable/non-null rows, integer bounds, decimal coefficients/scales, Unicode, empty text, binary bytes, pre-epoch dates, local timestamp ms/us/ns and UTC ms/us. Include legal widening and deliberately lossy/retyped failures.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `canonical`    | Duplicates, nulls, signed zeros and float bit patterns supported by the writer contract. Fixed input multiset, shuffled order, different batches, spills and merge fan-in. Compare every file name, byte count, SHA-256, row count and writer profile across platforms.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `boundaries`   | 0, 1, 4095, 4096, 4097 and 10000 rows; zero rows still have an explicit contract. A row larger than 4 MiB but within the transfer contract; an oversized row/page that must fail. A wide projection with over 256 columns.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `partitioned`  | UTC month boundaries, multiple partitions, an explicitly empty partition and a partition removed in the next snapshot. A selected available partition alongside an unavailable unrelated one. Null partition source values must fail.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `evolution`    | Two compatible revisions plus appended columns in a later baseline; incompatible column order/type and wrong physical footer/schema fixtures. Selection of an old revision must retain its actual old contract.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `identities`   | Existing JCS golden vectors plus equivalent aliases/defaults/file references and explicit `transport: auto`. Persist authoring, canonical connection and resolved transport separately.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `remote-files` | At least three Parquet files, empty tables and canonical keys containing spaces, percent characters, Unicode and `@`. Persist exact URI, size, SHA-256 and validator separately from decoded SQL filenames.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `salesforce`   | The versioned fixture in `fixtures/release-validation/salesforce/`: a dedicated two-object allowlisted record set (1,000 + 10 rows) with an independently supplied value manifest — null text (including empty CSV text normalized to null), exact representable numeric/date/time values, relationship/formula/filter cases and real REST pagination. Checkboxes are non-null and this Bulk ingestion path stores timestamps at whole-second precision; non-null empty strings, nullable booleans and sub-second scalar acquisition require another fixture. Eligible ID/numeric/date/bool Bulk paging is separately tested with supported maxRecords=100; rich nullable-text Bulk remains a precise refusal, not successful acquisition coverage. Restrict expectations to representations actually available in Salesforce; test unsupported domains as precise rejection cases. |

Salesforce fixtures cannot be replaced with arbitrary production data. If a
native field cannot supply a boundary value, use a supported dedicated formula
or projection with a proved source domain, or cover that decoder boundary in
automated conformance. Record why the live representation is unavailable;
do not claim that a one-row ID query covers all conversions.

## Required release cases

`L`, `S` and `G` below mean local, S3 and an authorized **writable** GCS test
root. `G-read` means the existing read-only fixture. GCS writes are now authorized for the isolated root above. Cases lacking
harnesses or actual service access remain planned/blocked, not passed or
omitted. Initial execution is macOS ARM64 only.

### Baseline, local lifecycle and canonical bytes

| ID           | Procedure                                                                                                                                                                                    | Required result and evidence                                                                                                                                                                                        |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| REL-BASE-01  | Run registered scenarios with both parent and adapter adversaries on macOS ARM64 first; incrementally map selectors/assertions. Retain all-four-platform cells for later full qualification. | Exact named selectors and assertions mapped; no unexplained skip, parser crash, invented capability or result-schema exception. Include cross-language Arrow V5 golden fixtures.                                    |
| REL-LOCAL-01 | Initialize, repeat initialization, inspect a damaged root, then run packaged list/capabilities/install and fixture-command conformance.                                                      | Idempotent configuration, damaged-root refusal, manifest listing starts no child, deterministic shadowing, protected directory/tarball installation and explicit replacement. Fixture stays outside release bundle. |
| REL-LOCAL-02 | Capture a multi-table DuckDB snapshot with populated/empty tables, publish, repeat with a new attempt and inspect/full-verify.                                                               | One atomic complete revision, exact data and empty completion, unchanged snapshot gives the specified no-op, correct committed history and schema-valid JSON.                                                       |
| REL-LOCAL-03 | Change rows, remove a partition, explicitly drop a table and exercise stale-base publication.                                                                                                | Only complete successful snapshots authorize omissions; exact next state, guarded base conflict and no partial publication.                                                                                         |
| REL-LOCAL-04 | Remove source/adapter/declaration after accepted capture and after terminal publication; retry original and changed requests.                                                                | Accepted capture publishes without source access; terminal replay is immutable; changed request gives `REQUEST_MISMATCH`; missing/corrupt accepted evidence is refused.                                             |
| REL-CAN-01   | Write the fixed canonical multiset through differing batch, spill, merge and file boundaries.                                                                                                | Identical versioned Parquet bytes and file identities on all platforms, with duplicates/nulls/float bits preserved. A writer-profile change is explicit, not silently accepted as equivalent bytes.                 |
| REL-CAN-02   | Run scalar/boundary/evolution fixtures through actual native extraction, canonical writing and ordinary DuckDB reads.                                                                        | Exact values/schema/order and typed null widening; lossy decimal/timestamp conversion fails before acceptance/allocation. Empty tables retain their contract.                                                       |
| REL-CAN-03   | Throttle the sink, exhaust four credits, exceed source/scratch limits and fail a spill/write under supported harness resource offers.                                                        | Bounded queues and allocations, responsive control/cancel, explicit resource failure and stopped workers. Record configured bounds plus allocator/native accounting; RSS alone is not the budget oracle.            |

### Real backend contracts

| ID         | Procedure                                                                                                                                                  | Required result and evidence                                                                                                                                                                                           |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| REL-S3-01  | Create a control object, repeat create, conditional update, stale update, read, ranged read, list and idempotent delete.                                   | Exact conditional-write/precondition behavior, validator and size/range coherence; no fallback unconditional overwrite.                                                                                                |
| REL-S3-02  | Upload beyond one part; fail/cancel before completion and suppress a successful completion response.                                                       | Exact successful bytes/hash; ambiguous effect is resolved by the prescribed identity proof or stays unknown. Own pending uploads are accounted for and cleaned.                                                        |
| REL-S3-03  | Repeat multi-table DuckDB and Salesforce capture/publication/no-op/source-free replay into S.                                                              | Real production CLI/adapter lifecycle, exact committed manifests and bytes; replay cannot authenticate, query or allocate again.                                                                                       |
| REL-S3-04  | Pull multi-file and empty S3 views using the independently configured reader; query with ordinary DuckDB afterward.                                        | Saved views use exact verified keys; no glob/list/write fallback, local pseudo-file or ambient credential substitution. Schema/row checks are identical to local pulls.                                                |
| REL-S3-05  | Retag unchanged bytes, change bytes/size/validator, use special-character keys and attempt an unlisted key.                                                | Correct full-SHA fallback for an opaque validator mismatch; changed hash/size or unauthorized key refuses before destination mutation. No alias/double-decoding bypass.                                                |
| REL-S3-06  | Refresh views with a failing late table/check, then a successful new revision; prune sources and replay old receipts.                                      | Full rollback including ownership/checkpoints/receipts, successful new generation, immutable old receipt replay without S3/auth/reader config. Lost receipt metadata refuses.                                          |
| REL-GCS-01 | Read/list/head/hash/full-verify the existing G-read fixture and compare its three rows and empty table.                                                    | Zero cloud mutations; metadata, generation, file hashes/footer and declared rows agree with the independent oracle.                                                                                                    |
| REL-GCS-02 | In G, test create-if-absent, generation-matched update, stale update, delete and listing.                                                                  | Correct real GCS generation preconditions and durable read-back, with no unconditional replacement. Write scope now authorized; named live harness and cleanup evidence required.                                      |
| REL-GCS-03 | In G, cross an upload chunk boundary, cancel/fail an upload and suppress a successful conditional-write response.                                          | Exact bytes, bounded streaming and specified ambiguous-outcome proof; own resumable state/effects identified and cleaned. Write scope now authorized; named live harness and cleanup evidence required.                |
| REL-GCS-04 | Repeat multi-table DuckDB and Salesforce capture/publication/no-op, verified local pulls, managed/external fixed-input builds and source-free replay in G. | Same complete lifecycle/oracles as L/S. GCS inputs materialize as verified local files; no unadvertised GCS-view capability is required. Write scope now authorized; named live harness and cleanup evidence required. |

### Salesforce acquisition

| ID        | Procedure                                                                                                                                                        | Required result and evidence                                                                                                                                                                                                                                                                                                                |
| --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| REL-SF-01 | Pure validation/offline lookup, then real authentication against the explicitly expected org. Try a wrong expected identity and an unsupported pull declaration. | Pure operations make no auth/HTTP call; exact org match precedes run/request fixing. Wrong org and unsupported mode refuse without query/job/publication.                                                                                                                                                                                   |
| REL-SF-02 | Run `auto`, explicit REST and eligible Bulk against the fixed read fixture.                                                                                      | API v66.0 default, REST-first auto, independently verified identical representable values; explicit authoring and resolved transport remain separate.                                                                                                                                                                                       |
| REL-SF-03 | Cross real REST query locators and Bulk result pages with a slow consumer and explicit empty selection.                                                          | Complete counts/values, durable mandatory checkpoint before any rows or empty completion, budgets enforced, no dropped/duplicate pages.                                                                                                                                                                                                     |
| REL-SF-04 | Exercise exact decimals/timestamps, null versus empty text, formulas, functions, relationship/filter semantics and ambiguous case names.                         | Existing supported filter/conversion contract preserved. Bulk is eligible only where representations are proved lossless; precise refusal replaces blanket field/function bans.                                                                                                                                                             |
| REL-SF-05 | With the fixture's reviewed mutation procedure, make a row stop matching, change a partition and then query an empty snapshot.                                   | New complete snapshots remove the intended rows/partitions. A failed page/object never authorizes omission. Mutation and reset run against the disposable org per the fixture procedure. Storage is not a readiness blocker; use the reviewed `--ignore-storage-limit` reset flag and prove mutation/reset and cleanup in the case harness. |
| REL-SF-06 | Persist Bulk creation intent, perform one real POST, suppress its response and restart.                                                                          | Durable `OUTCOME_UNKNOWN`, possibly empty job IDs, exactly one POST and no inferred job adoption. Private out-of-band job cleanup evidence does not authorize restarting acquisition.                                                                                                                                                       |
| REL-SF-07 | Fail/cancel after an acknowledged REST/Bulk page; test unavailable authentication using isolated harness state, without revoking existing credentials.           | No partial capture acceptance and no re-query/repeated creation under that attempt. Fresh attempt reacquires all tables. Helpers/descendants stop and diagnostics remain sanitized.                                                                                                                                                         |
| REL-SF-08 | Publish an accepted capture, then remove source/auth/helper access and retry into L, S and G.                                                                    | Original outcome or accepted-capture publication without Describe/query/auth calls; G portion is authorized under the isolated write root and still needs lifecycle evidence.                                                                                                                                                               |

### Pull transactions and SQL

| ID          | Procedure                                                                                                                                            | Required result and evidence                                                                                                                                                               |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| REL-PULL-01 | Pull complete identity replacements from L/S/G to local DuckDB, using latest and fixed revisions; change mappings and attempt overlapping ownership. | Stable scope and target names, atomic obsolete-target removal, exact data/checkpoints, overlap refusal and no generated historical schemas.                                                |
| REL-PULL-02 | Import selected partitions/old schemas with an unavailable unrelated partition and typed appended-column cases.                                      | Only selected manifest files read, Hive inference disabled, actual historical schema preserved and unavailable selected data refused before mutation.                                      |
| REL-PULL-03 | Append SQL with existing business IDs, duplicate inputs/window selection, multiple outputs, zero rows and a fresh versus repeated attempt.           | Declared SQL alone decides business policy; every query sees one pre-write state; same attempt never repeats insertions and fresh attempt re-evaluates.                                    |
| REL-PULL-04 | Fail the second table/check and terminate at the destination transaction commit boundary.                                                            | All rows/schema/ownership/checkpoints/receipt changes roll back, or the immutable committed receipt is recovered. No assumed rollback of an unresolved commit.                             |
| REL-PULL-05 | Run prohibited DML/multi-statements/external reads and nested view/macro/rebind/custom-callback canaries.                                            | Initial and automatic rebind are guarded before side-effecting callbacks; no output writes or hidden input/provenance.                                                                     |
| REL-PULL-06 | Advance with a newer pull, prune the old source, remove credentials/private state, then replay the original attempt and delete its receipt store.    | Original immutable receipt survives checkpoint advancement/pruning; changed request refuses. Lost initialized history is `PROTOCOL_FAILURE` or `OUTCOME_UNKNOWN`, never a fresh workspace. |

### Managed and external builds

| ID           | Procedure                                                                                                                                                           | Required result and evidence                                                                                                                                                                                                           |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| REL-BUILD-01 | Discover inputs; attempt foreign-root/application-import inputs and direct/transitive target cycles.                                                                | Reject before run/hold/output execution with exact provenance/cycle evidence. Discovery transaction remains fixed through run creation, holds and verification.                                                                        |
| REL-BUILD-02 | Prepare against L/S/G, refresh/edit tracking tables, run source GC and invoke later.                                                                                | Exact held immutable input revision/file set used. Hold confirmation wins safely or preparation fails; edited/refreshed cached rows never substitute.                                                                                  |
| REL-BUILD-03 | Exercise self-base, held/dropped outputs, genuinely empty outputs, incomplete seeded outputs and omission-only completion.                                          | No self-hold; exact selected-output coverage; table existence never implies completion; omission-only changes commit according to the contract.                                                                                        |
| REL-BUILD-04 | Fail the second managed output or lose owner while its native work is active.                                                                                       | No successful complete candidate, new allocation or publication; cancel and wait for workers/connections. No automatic SQL rerun in the attempt.                                                                                       |
| REL-BUILD-05 | Use the real external-driver library, a live native process and descendant; renew while it owns the database.                                                       | Canonical engine/session locks retained; renewal does not open the engine; all invocation connections and descendants stop before completion. Escaped live ownership remains busy.                                                     |
| REL-BUILD-06 | Supply mismatched completion identities/digest/mapping; lose acceptance response; remove accepted result file and retry.                                            | Rejection before allocation for mismatches, immutable identical acceptance/replay, fresh-process continuation without managed invocation. Explicitly naming a missing file is not equivalent to omitting the accepted result argument. |
| REL-BUILD-07 | Use multi-file/empty held S3 views and self-base; fail export after a credited batch, remove reader config/source engine, restart export and later terminal replay. | Accepted fixed outputs export with a fresh stream without source SQL; fixed provenance and holds survive tracking refresh. Finalization requires acceptance; terminal replay opens neither adapter nor engine.                         |

### Publication, recovery and retention

| ID         | Procedure                                                                                                                        | Required result and evidence                                                                                                                                                                                                         |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| REL-REC-01 | Run the fault-boundary matrix below across destination commits, GRV publication and administration on L/S/G.                     | Independent before/after snapshots, exact effect counts and stable operation/attempt/run identities prove rollback, committed replay or explicit unresolved outcome.                                                                 |
| REL-REC-02 | Inspect pending/known outcomes, use show/renew/abort, lose ownership and restart from accepted or incomplete acquisition/export. | Inspection is read-only; abort resolves known commits, never labels them unpublished; incomplete acquisitions never restart under the same attempt.                                                                                  |
| REL-REC-03 | Pin/unpin independent scopes, retry IDs, preview GC, race a new pin/hold with applying GC and interrupt prune cleanup.           | Preview writes nothing; protections rechecked; only committed exact tombstones authorize deletion. History/coordination/high-water marks retained; cleanup resumes the same decision.                                                |
| REL-REC-04 | Recover live, expired, sealed, missing/corrupt and unproven run/claim evidence, including a remote write still unresolved.       | Valid owners remain respected; recovery never publishes/rebases/adopts working tables. Local flock is never remote stopped-writer proof. Safe waiting must be reported explicitly; any claimed later progress needs actual evidence. |
| REL-REC-05 | Fail an advertised post-publication hook, kill before/after private acknowledgement, remove source/backend/engine and retry.     | Known outcome persisted before hook; only fixed idempotent acknowledgement retries. No duplicate capture/build/publication. Current built-ins advertise no hook; test this generic supported lifecycle with an opt-in fixture.       |

### Secrets, artifacts and release installation

| ID         | Procedure                                                                                                                                                                                 | Required result and evidence                                                                                                                                                                                                                                    |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| REL-SEC-01 | Place synthetic credential canaries in parent backend/auth stores and inherited descriptors; flood stderr and inspect public output/documents/artifacts.                                  | No canary in control/result/diagnostic output, no inherited backend credentials, raw helper stderr never publicly rendered. Exclude legitimate private credential stores from the artifact scan; do not claim universal detection in arbitrary dataset content. |
| REL-SEC-02 | Break permissions/owners, substitute executable after hashing, use symlink/hardlink/path aliases and malformed frame/document/completion records.                                         | Protected-path refusal, exact bounded strict parsing/correlation and no mutation/secret disclosure. Installation checks use protected workspace ancestors.                                                                                                      |
| REL-SEC-03 | Cancel/EOF/kill with occupied credits, native work and helper descendants; race established completion with cancellation.                                                                 | ACKs freeze before cancel, stopped-work acknowledgement is real, escalation is bounded and known results survive shutdown races.                                                                                                                                |
| REL-PKG-01 | Build a clean committed macOS ARM64 candidate with locked dependencies, strict lint/formatting and minimum Rust. Retain other-platform cells for later qualification.                     | Exact source commit, toolchain/native/extension hashes and capability inventory; every applicable automated case passes, including test helpers that use a custom harness.                                                                                      |
| REL-PKG-02 | Package release-profile artifacts; relocate them onto clean matching hosts without the build tree/extension cache; install from directory/tarball and run representative live lifecycles. | All three executables work; packaged native guard and signed extensions load, no hidden source-tree path or network INSTALL dependency. Never distribute the fixture adapter.                                                                                   |
| REL-PKG-03 | Inventory exact Rust and native/extension dependencies, licenses and signed artifacts; verify checksums/signatures and provenance.                                                        | Complete notices for the actual shipped bytes on each platform. Project MIT licenses or binary version strings alone do not close the native third-party gate.                                                                                                  |
| REL-PKG-04 | Open the pre-release development bundle's existing storage, pull receipt and accepted/terminal attempt fixtures with the candidate, then repeat supported workflows and replay.           | Contract-compatible data/replay or an explicit documented migration/refusal. No silent history reset. This is the first-release compatibility baseline; retain it for the next release's upgrade tests.                                                         |

## Fault boundaries and repetitions

The fault harness pauses at a named boundary and records what became durable
before terminating only its own process group or suppressing a response.
Completion must be observed independently from persisted state and service
effects. A test that crashes before making the external call does not cover
an ambiguous successful call.

| Boundary       | Inject before and after                                                                                                    | Required oracle                                                                                                    |
| -------------- | -------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Acquisition    | Private creation/bootstrap intent, durable checkpoint/ACK, page receipt, table/source completion, capture acceptance       | At most one nonresumable acquisition; no unacknowledged emission/adoption or partial accepted snapshot.            |
| Destination    | Binding/ownership setup, transaction commit and receipt persistence/return                                                 | Proven rollback or original immutable commit receipt; never duplicate append or rewrite an older receipt.          |
| Build          | Run/hold confirmation, preparation/context, invocation stop/connection close, completion acceptance, batch export/adoption | Fixed identities/files, no SQL rerun, accepted completion precedes export; exact explicit empty/omission coverage. |
| Publication    | Intent, allocation, file upload, manifest/finalization, sealing, revision description, LATEST CAS, outer result, hook ACK  | Adopt only exact committed identity proofs; known outcomes survive later failures; no second publication.          |
| Administration | Pending CAS, pin/hold effect, prune decision, tombstones, pending clear, physical deletion                                 | Decision before destructive effects; protections and history retained; exact original decision replayed.           |

Run deterministic boundary cases once per applicable backend/platform cell.
Run explicit concurrent races at least 20 times per supported platform with
recorded seeds and both orchestrated winner orders. Deterministic data uses at
least ten recorded shuffle/batch/spill seeds. If a boundary is not injectable
in a release binary, record the instrumented binary digest plus a production
black-box lifecycle that exercises the same implementation. A skip, timeout,
unresolved cleanup or missing observation is not a pass. Expected busy/unknown
results pass only when the case specifically requires refusal and proves that
no prohibited effect occurred.

## Execution sequence and existing entry points

1. Run the first macOS pass in reviewed families and update exact selectors/
   assertions as each runs. Add missing fixture/fault harnesses; incomplete
   mapping remains an explicit gap, not a prerequisite to begin this pass.
   Never blanket-run ignored tests: some are helpers or need distinct permissions.
2. Freeze the reviewed source commit, lockfile, contracts, fixtures and hashes.
   Run the full native automated suite, strict lint and guard/extension proofs
   on macOS ARM64 for the initial candidate. Require a clean full run after
   fixes; iterative first-pass logs are baseline evidence. Later broad
   qualification needs clean full runs on all four platforms.
3. Build fresh **release-profile** candidate bundles. Run relocation/loading,
   installation and clean-host CLI gates, then live backend primitives.
4. Run real acquisition/pull/build/administration cross-products, then controlled
   faults and repeated races. Reconfirm offline replay with source/service
   access deliberately unavailable.
5. Verify owned cleanup and credential-canary scans. Review the full evidence
   dossier and exact native dependency notice inventories.
6. Freeze final release notes/checksums and review the concrete tag and artifact
   destinations. Publishing follows that review, rather than test completion
   implicitly publishing anything.

The required cross-products are DuckDB and Salesforce extraction into L/S/G;
verified local DuckDB pulls from L/S/G; S3-view pulls from S; managed and external
builds on L/S/G with backend-appropriate fixed files; and publication,
retention/recovery and source-free replay on L/S/G. Providers run with explicit
identities. Provider integration runs first on macOS ARM64; later qualification must
exercise native loading, authentication and cancellation on every claimed
platform.
Fixtures may share their read-only source; write roots and state are isolated.

Existing entry points are useful starting gates, **not the entire plan**:

```console
python3 scripts/check-release-validation.py
RUST_TEST_THREADS=1 cargo test --locked --workspace --all-targets --features grv-adapter-duckdb/native,grv-conformance/native-duckdb
RUST_TEST_THREADS=1 cargo test --locked --workspace --doc --features grv-adapter-duckdb/native,grv-conformance/native-duckdb
cargo clippy --locked --workspace --all-targets --features grv-adapter-duckdb/native,grv-conformance/native-duckdb -- -D warnings
cargo fmt --all -- --check
ctest --test-dir "$GRV_DUCKDB_NATIVE_LIB_DIR" --output-on-failure
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
```

Use a protected native library path and the reviewed toolchain. Run Cargo
builds/subprocess suites serially within each target directory; parallel workers
use isolated target directories and owned roots. Collect both stdout and stderr
privately and publish sanitized summaries, retaining exit status and assertions.

| Existing live selector                                                                                                                                           | Coverage boundary                                                                                                                                                                                                                                                                                        |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `grv-storage --features cloud --lib`: `cloud::tests::live_s3_conditional_multipart_contract`                                                                     | Real S3 primitives/multipart success; add cancellation, response-loss and upload cleanup cases.                                                                                                                                                                                                          |
| `grv-conformance --test push_cli`: `live_cli_s3_capture_publication_deduplication_and_source_free_terminal_replay`                                               | S3 fixture capture/publication/replay; add production adapter/data cross-products.                                                                                                                                                                                                                       |
| `grv-conformance --features native-duckdb --test s3_views_cli`: its two `live_s3_*` tests                                                                        | Exact URI/validator fallback, multi-file refresh/rollback and source-free receipts.                                                                                                                                                                                                                      |
| `grv-conformance --features native-duckdb --test s3_build_cli`: `live_external_s3_build_fixed_view_inputs_self_base_accepted_export_restart_and_terminal_replay` | Fixed held S3 inputs/self-base, external invocation, accepted export restart and terminal replay.                                                                                                                                                                                                        |
| `grv-storage --features cloud --lib`: `cloud::tests::live_gcs_read_only_contract`                                                                                | Real GCS list/head/read/hash only.                                                                                                                                                                                                                                                                       |
| `grv-conformance --test read_fixture`: `live_gcs_complete_published_fixture_read_only`                                                                           | Complete GCS published fixture verification; no write lifecycle.                                                                                                                                                                                                                                         |
| `grv-adapter-salesforce --test live_service`: `named_org_authentication_metadata_and_read_only_rest_query`                                                       | Org/auth/Describe/one-row REST smoke gate.                                                                                                                                                                                                                                                               |
| `grv-conformance --test salesforce_live_cli`: `production_cli_org_capture_publication_and_source_free_terminal_replay`                                           | Rich scaled fixture gate with `auto` or `rest`, independent complete value oracle/no-op/offline replay. Explicit Bulk rich projection has a separate precise-refusal selector; eligible Bulk paging is in `grv-adapter-salesforce --test live_bulk_pages`. Never use the historical production identity. |

Select the exact named test with `-- --ignored --exact --nocapture
--test-threads=1` after checking its worksheet. Run each explicit transport
separately. The current CI workflow is a starting point; its development-profile
bundle and summarized artifacts do not yet satisfy the final release-profile,
live matrix and complete evidence requirements above.

## Evidence and pass rules

Every execution record contains: case and normative IDs; source/spec/fixture
hashes; executable/native/extension digests; platform/toolchain; backend and
redacted connection identity; attempt/run/operation/invocation IDs where
applicable; deterministic seed; permitted effects; command/selector; UTC times;
exit code and schema-validation result; independent row/schema/hash assertions;
durable before/after facts; fault position/effect count; resource observations;
cleanup outcome; sanitized evidence paths and SHA-256 digests.

Allowed states are `planned`, `blocked`, `running`, `passed`, `failed` and
`not_applicable`. A passed record requires every stated assertion and owned
cleanup. `not_applicable` requires a reviewed contract/capability reason and is
never a substitute for unavailable credentials. New evidence cannot overwrite
an earlier failure. Record the fix and repeat the failed case, its affected
regression families and gates invalidated by changed source/native/artifacts.

The dossier distinguishes unsupported safe progress from proved successful
recovery. In particular, cloud stopped-writer evidence is currently conservative:
local journal locks do not prove remote uploads stopped. Review required
liveness separately; record blocked cleanup/progress instead of weakening the
safety oracle to make a test green.

Release requires all applicable register rows and release cases passed on their
required matrix cells, zero unexplained ignores, independently verified bytes,
clean owned resources, complete notices, clean committed source, and a reviewed
public capability/limitation inventory. Only then may the bundle be marked
`complete_client_v1: true` and treated as the first full release. A preview with
blocked GCS writes or incomplete notices must be separately named/scoped and
cannot quietly replace that target.

## Execution-readiness review (2026-10-06)

**First macOS ARM64 pass is authorized: run named families, map coverage as
we go, add fault harnesses, fix bugs and retest. This is not yet complete
full-v1/four-platform release qualification.**
Storage is not a blocker by explicit review decision. This distinction does
not change the required results or authorize cloud effects beyond the
worksheet. No full release case is marked passed by this review.

| Area                                            | Readiness                       | Remaining action                                                                                                                                                                                                                                                                                                                     |
| ----------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Normative traceability                          | Not ready                       | Incremental first-pass mapping is approved. Map existing exact selectors/assertions as they execute; unresolved rows remain gaps. `--require-mapped` is a coverage completion check, not a prerequisite for beginning this pass.                                                                                                     |
| Salesforce source fixture                       | Available                       | Scaled 1,010 rows strictly verified; permissions assigned, alias pinned; clean reload and mutation/reset passed. Four declarations now pass core/adapter authoring schemas, including explicit `utf8` for the derived `_month_` column. Fixture readback is not GRV acquisition evidence.                                            |
| Salesforce production acquisition               | Scoped development gates passed | Rich production REST/auto fixture schema/values/no-op/replay, empty/relationships/wrong identity, mutation/omission/reset and eligible Bulk pages/checkpoint/empty/cleanup passed on scaled fixture. Rich text Bulk precisely refuses. Separate frozen candidate and SDK wire/credit/failure matrix qualification remain.            |
| Other fixtures/oracles                          | Partial                         | The plan names scalar, canonical, boundary, partitioned, evolution and remote-file oracles, but there is no versioned release fixture package for them here. Existing identity vectors and in-test fixtures are starting assets; inventory, independently verify and map them rather than assume coverage or duplicate them blindly. |
| Existing automated/local gates                  | Runnable after native setup     | The listed commands and many native/conformance tests exist. This shell has no exported native/extension or live worksheet variables; restore protected paths and explicit identities before execution.                                                                                                                              |
| Existing S3 / GCS-read / Salesforce smoke gates | Conditionally runnable          | Exact named selectors exist; restore the authorized worksheet, profiles, native paths and explicit org identity. They are starting gates, not all required lifecycle/fault assertions.                                                                                                                                               |
| Writable GCS                                    | Authorized                      | Use `gs://validation-gcs-bucket/grv-release-validation/` and owned UUID children; add/run write and fault/lifecycle harnesses. Never write to `xyz/`.                                                                                                                                                                                |
| Live fault matrix                               | Partial                         | Existing mock seams and specific export restart tests do not cover all successful-effect response loss, commit/publication/administration boundaries or repeated races. Add reviewed barriers/effect-count/cleanup oracles.                                                                                                          |
| Candidate and four-platform evidence            | Not ready                       | Checkout is not a clean frozen candidate; four-platform CI exists but qualifying immutable release-profile artifacts and clean full-run/live matrix evidence are not established.                                                                                                                                                    |
| Packaging and native notices                    | Not ready                       | Existing development/relocation evidence is baseline only. Prove clean-host release-profile installation and inventory the exact shipped native/extension dependencies and notices.                                                                                                                                                  |

Start with static draft consistency, exact selector/assertion mapping and
fixture preflight; then run existing automated/local and authorized live
starting gates as baseline evidence while closing richer harness gaps. Do
not use a broad `--ignored` run or count a smoke gate as a full release case.
Record first-pass findings and reviewed mappings in the register and worksheet.
Freeze a candidate after fixes, then rerun qualifying initial-macOS gates.
The later broad release still requires the full matrix.

## Review decisions before execution

- Initial macOS ARM64 validation/candidate scope and package version `0.1.0`
  are the current target. Review initial release limitations before tagging;
  full-v1/four-platform qualification remains separately tracked.
- GCS writes are authorized under `gs://validation-gcs-bucket/grv-release-validation/`;
  use owned UUID children and preserve the existing `xyz/` fixture.
- Confirm the disposable Salesforce org (Developer Edition or above; the
  Base Edition candidate was rejected by the provisioning gate) and the
  fixture field/value matrix. The mutation/reset procedure is the fixture's
  `reset.sh`; org disposal covers Bulk query-job cleanup.
- Improve reviewed normative selector/assertion mappings alongside execution;
  review applicability, independent oracles and fault boundaries. Add missing
  tests and resolve failures before freezing the initial macOS candidate.
- Confirm platform/live runner access, test budgets and the exact native notice
  provenance path. Record any remaining cloud recovery liveness limitation.
- Review final evidence and concrete publishing destinations after execution.
