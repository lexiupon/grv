# GRV cloud views and DuckDB pushdown (RFC)

|              |                                                                                                                                                      |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Status       | Draft; user-approved simplified reader direction; no new capability advertised                                                                       |
| Scope        | Keyless GCS views using stock HTTPFS/OAuth HTTPS and measured S3/GCS pushdown                                                                        |
| Companion to | [Client](grv-client-v1.md), [execution](grv-client-v1-execution.md), [adapter protocol](grv-adapter-protocol-v1.md), [Storage v2](grv-storage-v2.md) |
| Work plan    | [Implementation plan](grv-cloud-views-pushdown-v1-plan.md)                                                                                           |
| Evidence     | [Prototypes](../prototypes/cloud-views/README.md), [live GCS results](../prototypes/cloud-views/live-evidence/README.md)                             |

## Validated direction

Production policy prohibits HMAC creation. The user-authorized
[real GCS OAuth HTTPS prototype](../prototypes/cloud-views/httpfs/oauth-live-evidence/README.md)
passed ten oracle scans and one negative authority probe on signed pinned
HTTPFS with temporary HTTP bearer secrets. Query-only payload savings:
**96.96% projection / 93.59% selective wide**. No custom reader, key, new service
account, IAM grant or policy exception was required; all fixtures were cleaned.

This revision plans that keyless path. It is not a shipped feature: positive
production guards, token lifetime, coordinates/lifecycle and packaging remain
release gates. Historical HMAC/custom-transport experiments remain evidence,
not alternative required runtime paths.

## 1. Objectives and approved decisions

GRV supports local, S3 and GCS storage. DuckDB pulls currently support local
materialization and `s3-view`, not GCS views. This RFC adds:

1. **GCS views:** query exact selected GRV Parquet files through DuckDB's stock
   HTTPFS HTTPS reader, authenticated with explicitly selected short-lived OAuth tokens.
2. **Pushdown support:** verify eligible predicates/projections reach native
   Parquet scans and reduce actual reads on suitable fixtures for S3 and GCS.
   Correctness remains mandatory when pruning is unavailable.

User decisions:

- Use standard **HTTPFS/OAuth HTTPS**, rather than requiring prohibited HMAC
  keys or introducing a custom GCS transport. Retain external gs:// identities
  and translate trusted exact files to canonical HTTPS scan URLs.
- Require explicitly selected **account and project**; no active-config fallback.
- Revise draft contracts together before 1.0; no legacy installation,
  multi-version negotiation or durable migration layer is required.

Local remains the default. Add explicit `gcs-view`, initially for complete
identity replacement only. Existing protected S3 reader behavior stays unchanged;
this decision does not authorize replacing it with stock HTTPFS.

A local tool can reuse a standard reader without implementing a cloud transport.
It still processes declarations/SQL and holds credentials: local execution does
not remove secret handling, exact-source, integrity, transaction or guard duties.

## 2. Deliberate guarantee boundary

| Phase/path                    | Initial guarantee                                                                                                                     |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| GRV preparation               | Resolve selected revisions/manifests, check tombstones, verify file size/hash/schema and values under existing contracts              |
| GCS view query through HTTPFS | Exact saved object names and native DuckDB reader behavior; read failures surfaced; **not GRV generation-pinned per-range execution** |
| Existing protected S3 path    | Retain its existing exact-file conditional/version/validator behavior                                                                 |
| Ordinary external DuckDB      | Independently configured reader/temporary secrets; no GRV guard or retention protection implied                                       |
| Local materialization         | Verified local rows, no later cloud downloads                                                                                         |

**Preparation verification is not a permanent immutable-object guarantee.** A
stored `gs://bucket/key` view may later read changed content at the same key.
Generation/ETag protection by stock HTTPFS must not be assumed beyond what is
actually validated/documented for the pinned extension. This RFC explicitly
accepts best-effort GCS query-time identity for initial support. Generation
consistency inside a query also must not be claimed without reader evidence.

Normal GRV version files are immutable by convention; GRV holds/pruning rules
protect their normal lifecycle. They do not prevent an out-of-band privileged
writer from replacing objects. If consumers need stronger byte-identity
guarantees, use local verified materialization or a future verified remote reader.

Update dependent client/execution/protocol contracts to state this distinction;
do not call the new GCS scan a protected generation-pinned reader. This boundary
applies to held GCS external-build views as well: holds prevent normal GRV
pruning, not privileged out-of-band replacement. Provenance identifies selected
GRV inputs but does not establish every later HTTPFS byte still equals their
prepared hash. This limitation must be explicit before build capability release.

## 3. Scope and non-goals

In scope:

- Complete identity-replacement GCS pull views, schema evolution and typed empties.
- Fixed exact-file external-build inputs with GRV holds, subject to §2's boundary.
- Explicit GCS OAuth identity/project selection, pinned HTTPFS loading, guarded
  trusted scans, transactional refresh, receipts/replay and inspection.
- Production S3/GCS pushdown matrix with actual byte/request measurements.

Not initial scope:

- Custom GCS filesystem or per-request bearer-renewal callbacks, conditional
  backend GCS range fast path, metageneration pinning or new libcurl. Refresh
  between bounded executions, not seamlessly during a query.
- Changing existing S3 reader guarantees, universal filter/file pruning,
  server-side SQL, Hive inference, or manifest constants replacing physical data.
- HMAC key creation, project IAM changes or credential provisioning by normal
  GRV operations; runtime extension downloads; automatic secret/account fallback.
- New append/SQL-transform view pulls; managed builds continue local inputs.
- Pre-1.0 backward compatibility/migration or a storage-layout change.

## 4. HTTPFS and explicit authentication

### 4.1 Explicit keyless provider and identity

- S3 keeps its existing credentials/protected reader unchanged.
- GCS initially uses the named gcloud OAuth provider with mandatory account and
  project, invoked with both selectors. No service-account/HMAC key. Reject
  environment/config token-file, credential-file or impersonation overrides
  changing the selected principal; never use active-account fallback.
- Impersonation, attached identity and federation are future explicit keyless
  providers, not assumed permissions or fallback in the initial release.

Proposed engine-adjacent selector (not today's schema):

```json
{
  "config_version": 1,
  "readers": [
    {
      "scope": "gs://bucket/prefix",
      "provider": "gcloud",
      "account": "reader@example.com",
      "project": "example-project"
    }
  ]
}
```

All selectors are mandatory and unambiguous. Scope routes to a configured
identity, not authority to read arbitrary prefix contents. Preparation and query
selection remain explicit even when deliberately choosing the same principal.
Reuse private-file protections: bounded closed input, owner-only regular files,
protected ancestors and no link substitution. Persist selectors only; tokens
live in memory/private process channels, not a general credential broker.

### 4.2 Temporary secrets and bounded token lifetime

Trusted setup loads packaged pinned HTTPFS and installs a temporary TYPE http
secret with BEARER_TOKEN and scopes corresponding to admitted canonical HTTPS
paths on an exclusively owned connection. Never log or persist setup SQL.
BEARER_TOKEN is natively redacted; EXTRA_HTTP_HEADERS is not. Never put
Authorization there. Disable persistent secrets, runtime INSTALL/autoload and
untrusted secret/catalog/config access. No tokens in views, receipts, errors,
profiling SQL, command arguments or environment dumps. Scope is prefix routing,
not exact authorization; every open requires trusted exact-path authority.

The provider must return trusted expiry with the token, not assume a printed
token is fresh or valid for one hour. Resolve how the existing gcloud provider
obtains expiry without credential disclosure in PR1/2. Reject unknown or
insufficient lifetime before scans. Establish an execution deadline before
expiry with reviewed clock-skew/cleanup margin; cancel and roll back if exceeded,
then reacquire before the next execution. Do not replace a secret during a query
and assume open handles refresh. No whole-operation automatic retry after
results/effects, partial refresh commit or seamless mid-query renewal. Test
bind/execute/rebind expiry and cancellation races. If reliable expiry/deadline
enforcement cannot be provided, release is blocked; do not substitute custom
transport silently.

Ordinary DuckDB consumers obtain their own token/temporary secret; GRV does not
configure or renew indefinitely held external connections. Document expiry and
the same best-effort object boundary. Fresh per-case tokens worked in the live
prototype; it did not test expiry or renewal during execution.

### 4.3 Project and Requester Pays

Account authentication, bucket ownership and quota/billing project are distinct.
Selecting --project alone does not send a GCS header or grant permissions. When
authorized, trusted setup can supply explicit x-goog-user-project in
EXTRA_HTTP_HEADERS; this map must contain no credentials. The caller needs the
required project permissions.

Live header-bearing stock HTTPS scans succeeded on a verified Requester Pays
bucket, but no-header scans also succeeded for this principal. Offline checks
verify header emission; billing attribution and mandatory rejection for lesser
roles are not established. Qualify with an approved suitable identity without
broadening IAM automatically. Until the production gate passes, explicitly
refuse Requester-Pays buckets in gcs-view and document the limitation; never
silently omit billing selection.

## 5. Source verification, SQL and guard integration

### 5.1 Preparation and coordinates

Reuse existing GCS full-object preparation reads and verification first. This
avoids adding backend conditional ranges solely to enable view queries. Include
all existing tombstone, size/hash, physical/logical schema, prefix compatibility,
exact-value/partition and row-count checks. Preparation may download entire
objects; that cost is visible and is not query-time pushdown cost.

Generalize exact source-coordinate/access types for `gcs-view`. Use shared
canonical URI/key/native filename test vectors. The experimental native JSON
API parser escaped apostrophe/plus differently from GRV; it is historical
research, not a production mapping. Keep canonical gs:// coordinates in saved
provenance and map object bytes to exact storage.googleapis.com HTTPS URLs for
scans. Encoding must not reinterpret query/fragment, glob, encoded-looking names
or dot segments; refuse unsafe/unrepresentable keys rather than read another
object. Test the **stock HTTPFS** filename behavior
on the pinned engine for UTF-8, quote, percent, space, wildcard/brackets, plus,
`=`, `@`, malformed escapes, aliases and literal encoded-looking names.

### 5.2 View SQL

Keep per-file explicit projections and `UNION ALL`, identity/logical casts,
trailing typed nulls, `hive_partitioning=false` and `union_by_name=false`.
No globs or bucket scans: the saved view lists exact resolved manifest objects.
Empty selections preserve ordered schema with typed nulls and `WHERE false`.

Filters and projections can reach individual Parquet scans; typed nulls or
statistics can eliminate branches. Hive directory-derived pruning is disabled;
query predicates do not automatically consult manifest partition identities.
Physical partition columns can benefit from ordinary Parquet statistics.

### 5.3 Guard and authority

Use standard HTTPFS transport, but preserve the existing trusted scanner/setup
boundary and bind/rebind rules. Test that user SQL cannot supply arbitrary remote
paths, invoke credential setup, read secret catalogs, install extensions, or
introduce new file readers through nested views/macros/custom callbacks.

A scoped DuckDB secret is **credential routing, not an access-control sandbox**.
The adapter must authorize exact trusted scan paths without prefix/glob
expansion. Prefer the existing guard and exact allowed-path mechanism; validate
its behavior with actual HTTPFS opens and filename conversion. If a small
path-only authorization shim is needed, it may delegate reads to HTTPFS—it is
not justification to reintroduce a custom signing/range/generation transport.
If exact-path restrictions cannot be enforced, that is a blocking integration
issue, not something made safe merely by running locally.

Set clear engine/network/resource limits, cancellation and failure behavior.
Use pinned HTTPFS's range/cache/retry behavior rather than promising the
experimental reader's exact 64-KiB response contract. Measure actual requests,
overfetch and caching on the standard path; retain existing S3 limits unchanged.

## 6. Pushdown contract and measurements

- Results/types equal local materialization on unmodified fixtures.
- Eligible integer/string comparisons, ranges, AND/OR/null predicates,
  booleans, decimals, dates and timestamp representations are classified by
  scan pushdown and actual byte reduction. Include wrapper views, aliases,
  schema-evolution nulls, physical partition columns and nonidentity casts.
- Statistics-unusable expressions, missing statistics, compressed/dictionary
  data and many-file views remain correct without universal savings promises.
- Measure actual production S3 reader and stock HTTPFS/GCS traffic, not just
  `EXPLAIN` or the simulated/custom transport. Use value-consuming aggregates,
  not metadata-only `count(*)`, for full-scan baselines.
- Separate preparation/hash, bind/footer, query, EXPLAIN, retries and overfetch.
  Compare cold/warm/new-connection cases with documented cache settings.
- Establish reviewed relative thresholds for fixed qualifying fixtures; the
  earlier >75% prototype reductions are evidence, not a universal SLA.

## 7. Lifecycle and pre-1.0 contract revision

Revise API/host/SDK/wire/binding/registration/public schemas and specifications
coherently before 1.0. No dual-version installation or old-record migration is
required. Reject incompatible packages/durable records clearly before effects;
never reinterpret, delete or reset them automatically.

Within the revised contract:

- Transactional multi-table refresh updates views, ownership, checkpoints and
  receipts together; late failure restores all prior state.
- A view references a fixed selected file-name set, not a directory glob. Its
  content is best-effort under §2's GCS query-time identity boundary.
- GRV holds protect external-build input revisions against GRV pruning; provider
  versions are not substitutes for holds/tombstone checks.
- Standalone views can become unavailable after pruning or credential changes.
- Successful receipt replay/inspection must not access source, auth or reader
  configuration. Lost receipt metadata refuses; managed builds remain local.

## 8. Existing evidence and what it does not prove

Historical prototypes remain unchanged and accurately labeled:

- **26 simulated native trials** on patched DuckDB v1.5.6 showed compatibility of
  GRV-shaped SQL with projection/filter optimization.
- **10 real GCS native trials** using a _custom libcurl generation-pinned reader_
  matched local oracles. Projection saved 96.67%; selective filtering saved
  93.25%; accepted payload totaled 36,777,427 bytes across 640 requests.
- Separate bearer JSON API probes verified generation/encoding failures, hashing
  and special keys. The same principal also read without a Requester Pays
  project; billing attribution/negative rejection was not established.
- All throwaway object versions and the test bucket were deleted, with HTTP 404
  absence evidence. The deterministic Python suite passed 18 tests.

See [live evidence](../prototypes/cloud-views/live-evidence/README.md).
None of the historical custom-reader runs proves stock HTTPFS integration.
The [stock reader offline prototype](../prototypes/cloud-views/httpfs/README.md)
passed 11 synthetic-HMAC loopback trials with independently verified signatures
and native counter agreement. HMAC creation in entit-finance-prod was refused
by constraints/iam.disableServiceAccountKeyCreation (HTTP 412); the attempted
provisioning's empty bucket/scoped grant/account were cleaned. No policy changed.

The subsequent [keyless stock HTTPS live evidence](../prototypes/cloud-views/httpfs/oauth-live-evidence/README.md)
contains ten oracle scans and a negative production-owner authority probe.
Query-only wide/narrow/selective-wide bytes: 18,655,696 / 566,336 / 1,196,150;
savings 96.96% / 93.59%. All fixtures cleaned, no keys/IAM/policy changes. Native
HTTPState counters are pinned internal prototype instrumentation, not a public
metrics API. Requester Pays with a project header succeeded, but no-header also
succeeded for this principal; enforcement/attribution is not proven. Production
positive scanner integration, redirects and expiry were not tested. Do not
substitute any prototype for shipping-path qualification.

## 9. Implementation and release gates

Direction: stock HTTPFS/OAuth HTTPS, mandatory account/project, coordinated
pre-1.0 revisions, no legacy migration, local default and unchanged S3.

1. Finalize gcloud identity/expiry/deadline and private token injection contract.
2. Canonical HTTPS coordinates and exact read-only authority, redirects and
   nested/rebind/secret-access/cancellation adversarial tests.
3. Preparation verification, transactional refresh, holds, source-free
   inspection/replay under best-effort GCS query-time identity.
4. Shipping S3/GCS pushdown/type/fault/resource evidence; Requester Pays either
   fully qualified under an approved identity or explicitly refused.
5. Relocatable signed packaging, notices, docs/skills/schemas, compatibility
   checks and release evidence before capability advertisement.
6. Release candidate/install qualification and approved publication, with
   immutable-artifact rollback rather than automatic durable-state migration.

See the [ordered implementation/release tasks](grv-cloud-views-pushdown-v1-plan.md).
Publishing is not authorized by this planning/commit request. Concrete stock
limitations must be escalated, not used to silently add a custom transport or
weaken organization policy.

## 10. References

- [DuckDB GCS import](https://duckdb.org/docs/current/guides/network_cloud_storage/gcs_import)
- [DuckDB S3 API support](https://duckdb.org/docs/current/core_extensions/httpfs/s3api)
- [DuckDB HTTP partial reads](https://duckdb.org/docs/current/core_extensions/httpfs/https)
- `crates/grv-adapter-duckdb/native/s3.inc`, `verified_s3.inc`, `grv_guard.hpp`:
  existing protected S3 setup/scan boundaries (not replaced by this RFC).
- `crates/grv-adapter-duckdb/src/pull.rs`, `src/s3_config.rs`:
  generated SQL, verification, transactions and explicit reader precedent.
- `crates/grv-core/src/source.rs`, `crates/grv-storage/src/cloud/mod.rs`:
  parent verification and existing GCS full-object preparation.
