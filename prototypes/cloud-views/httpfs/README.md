# Stock HTTPFS GCS-view prototypes: HMAC and keyless OAuth

**Experimental, not production capability.** This uses the actual official
pinned HTTPFS extension on patched DuckDB v1.5.6, not the custom cloud
filesystem from `../native/`. The validated transport is a loopback
S3-compatible HTTP server with synthetic HMAC credentials. HMAC live validation
is blocked by policy, but a separate **keyless OAuth HTTPS live experiment now
passes** on real GCS: [results and cleanup](oauth-live-evidence/README.md). Ten
oracle scans and one authority probe, 96.96% query-only projection and 93.59%
selective-wide payload savings, no new keys/service accounts/IAM/policy changes.
Remaining production integration gates are explicit in
[validation status](../httpfs-validation-status.md).

## Reproduce offline

Prerequisites: existing patched native build/headers at `/tmp/grv-duckdb-native`,
Python 3.11+ (default `python3.13`), C++17 compiler, the exact pinned HTTPFS
source headers, and the verified official osx_arm64 HTTPFS extension. No runtime INSTALL or unsigned extensions.

```sh
# Fetch packaging inputs if absent: bounded official URLs; no credentials/cloud writes.
python3.13 scripts/fetch-duckdb-extensions.py \
  --output "$HOME/.grv-httpfs-prototype-extensions" --platform osx_arm64

# Export synthetic fixtures using the existing pinned engine, not a cloud operation.
bash prototypes/cloud-views/native/live.sh export --directory /absolute/fresh/fixtures

bash prototypes/cloud-views/httpfs/run.sh \
  --fixtures /absolute/fresh/fixtures \
  --output /absolute/private/httpfs-offline.json
```

`PYTHON`, `GRV_NATIVE_DIR`, `GRV_DUCKDB_SOURCE` and `GRV_HTTPFS_SOURCE` override
tool paths. HTTPFS headers default to
`/tmp/grv-httpfs-source/duckdb-httpfs-4bc690dba4496c765777a0269d48fdbaff7cdc11`;
the counter-layout header's SHA256 is checked before compilation. The helper
compiles only the experimental harness and removes its executable afterward.
It verifies the extension hash/footer/signature before LOAD. Tests disable
persistent secrets, automatic extension installation/loading, unsigned
extensions and external-file caching. Every trial creates a fresh engine with
one thread and temporary scoped `TYPE gcs` secret. No production source changes.

## Recorded results

[`results.json`](results.json): **11 trials passed**, comprising ten
correctness-oracle scans and one authority probe. All observed requests had
valid AWS4 HMAC signatures and hit recognized exact fixture objects.

| Trial                                  | HTTP body bytes served | HEAD + GET requests |
| -------------------------------------- | ---------------------: | ------------------: |
| Wide full scan                         |             18,721,232 |                  28 |
| Narrow projection                      |                631,872 |                  36 |
| Selective narrow scan                  |                131,101 |                  13 |
| Selective wide scan                    |              1,261,686 |                  14 |
| Typed-null elimination                 |              9,414,176 |                  28 |
| Requester-pays flag                    |             18,721,232 |                  28 |
| Empty file                             |                    534 |                   6 |
| Special filenames                      |             18,721,232 |                  28 |
| `gcs://` scheme alias                  |             18,721,232 |                  28 |
| Selective wide scan without statistics |              1,753,641 |                  29 |
| Authority probes                       |                      0 |                   0 |

**Projection saved 96.62%; selective wide filtering saved 93.26%.** Relative
assertions require >75% reductions on these fixed fixtures only. Counters are
actual server-served HTTP body bytes, including binding and EXPLAIN traffic,
not estimated file sizes. HTTP/TLS overhead is excluded; loopback uses plain
HTTP with test-only endpoint override. Fewer bytes need not mean fewer requests:
the narrow scan made more requests than the wide scan.

Native stock HTTPFS `HTTPState` counters are also captured before `QueryEnd`
resets them, using pending execution through result readiness. Every oracle
trial's total native body bytes/HEAD/GET/request counts exactly matched the
independent loopback server. View creation, query (including scan binding) and
EXPLAIN are recorded separately. Query-only wide/narrow/selective-wide body
bytes were **18,655,696 / 566,336 / 1,196,150**, respectively (**96.96% / 93.59%**
query-only savings). These differ from the table's all-phase measurements.

Instrumentation depends on the exact pinned internal extension class layout;
it is prototype-only, not a stable/public production metrics API. Counters
exclude HTTP headers and TLS/wire overhead. Independent server-count agreement
is verified on loopback. Subsequent OAuth live runs capture these counters on
GCS HTTPS, but do not independently compare them to GCS server-side counts.

Views match GRV's per-file explicit cast/typed-null/UNION SQL with
`hive_partitioning=false` and `union_by_name=false`. Results/types match local
materialization, including decimal/timestamp columns and historical nulls.
No-statistics selective scanning remains correct. Spaces, plus, literal percent,
hash, Unicode and SQL apostrophe worked as literal filenames using URL
compatibility mode. This is not an exhaustive production canonical-URI proof.

Requester Pays emitted signed **`x-amz-request-payer: requester`**, not evidence
that GCS's billing project can be selected by this option. Real service behavior
of this HMAC option remain untested. The separate OAuth HTTPS experiment accepts
an explicit Google billing header, without proving attribution/enforcement.

## Guard findings

Standalone exact `allowed_paths` rejected unlisted/glob/nested-unlisted scans
before any extra mock requests. These settings alone are not a production
read-only fence. A direct allowlisted local write-open attempt was refused in
this harness; its negative result does not establish universal write denial.

The existing actual GRV production owner rejected direct, nested and glob
remote Parquet calls before network activity. That establishes the default
negative boundary, **not positive trusted GCS HTTPFS scanner integration**.
Secret scope is credential selection, not sandbox authority. Production must
still authorize trusted exact read scans under initial/nested/rebind guards and
prevent secret/catalog/config/write/extension escape routes.

## Optional live mode: not executed

Requires explicitly approved existing HMAC key/private storage and associated
service-account/project metadata. Do not paste secrets into chat, scripts,
command arguments or saved view SQL. No key/IAM creation is performed by this
harness. It does not infer a gcloud identity.

Manifest TSV, no header (last field means physical trailing `extra` exists):

```text
gs://owned-bucket/exact-old.parquet<TAB>/absolute/private/old.parquet<TAB>0
gs://owned-bucket/exact-new.parquet<TAB>/absolute/private/new.parquet<TAB>1
```

With `GRV_HTTPFS_HMAC_ID`, `GRV_HTTPFS_HMAC_SECRET`, `GRV_HTTPFS_ACCOUNT`,
`GRV_HTTPFS_PROJECT` supplied privately in the environment, and
`GRV_HTTPFS_LIVE_APPROVED=yes`:

```sh
bash prototypes/cloud-views/httpfs/run.sh --live \
  --manifest /absolute/private/owned-manifest.tsv \
  --output /absolute/private/httpfs-live.json
```

Live endpoint is fixed to `storage.googleapis.com`; mock endpoint overrides are
TEST-only. This draft live mode checks wide/narrow/selective/selective-wide and
null-elimination results/plans, reporting native HTTPFS body/request counters
per phase. True wire/TLS bytes (`live_transport_bytes`) remain **null**.
Account/project fields are explicit supplied metadata, not verified key-owner
association. Project forwarding/billing attribution is not established. Do not
claim those capabilities from the presence of environment variables.

## Keyless OAuth mode

`run.sh --oauth` runs the same pinned stock reader over HTTP(S) with a temporary
`TYPE http, BEARER_TOKEN` secret; it does not create a GCS HMAC secret. Loopback
mode verifies synthetic bearer authorization and billing headers. Live mode
requires privately supplied `GRV_HTTPFS_BEARER_TOKEN`, explicit account/project
and approval. OAuth manifests use canonical literal
`https://storage.googleapis.com/bucket/percent-encoded-object` URLs.
`live_oauth.py` obtains tokens under explicit gcloud account/project, uploads
only isolated owned fixtures, runs oracle/counter checks and cleans in `finally`.
See [live evidence/reproduction instructions](oauth-live-evidence/README.md).

## Remaining work

- HMAC execution/association remains policy-blocked and is not required by the
  keyless candidate. Finalize OAuth token lifetime/renewal and identity contract.
- Shipping-path HTTPS metrics/fault qualification and Requester Pays
  enforcement/attribution or explicit refusal; live prototype counters alone
  are not production qualification.
- Positive production guarded exact-path GCS scans, secret isolation, read-only
  authority and automatic rebind/cancellation/resource tests.
- Exhaustive canonical filename vectors, credential revocation and lifecycle
  tests, transactional refresh/holds/receipts and packaging qualification.

No GCP resources participated in the offline runs. A subsequent explicitly
authorized live provisioning attempt created a new private empty bucket and
disposable service account with bucket-only object-viewer access. HMAC creation
was refused with HTTP 412 by enforced
`constraints/iam.disableServiceAccountKeyCreation`. No key was created and no
policy was changed. All created resources were removed and absence verified.
See [`live-blocker.json`](live-blocker.json), [`cleanup.json`](cleanup.json) and
[validation status](../httpfs-validation-status.md). Real HTTPFS/GCS HMAC reads were
not reached. Subsequent OAuth HTTPS reads succeeded, separately documented above.
The offline mock requests contain only synthetic credentials.
