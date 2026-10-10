# Real GCS: stock HTTPFS + keyless OAuth

**Live prototype passed; not a shipped GRV feature.** Actual signed pinned
HTTPFS on patched DuckDB v1.5.6 read private GCS Parquet objects via HTTPS using
`CREATE TEMPORARY SECRET ... TYPE http, BEARER_TOKEN ...`. No custom reader,
HMAC/service-account key, new service account, IAM grant or policy exception.

Explicit identity: `jiacao@on.sinch.com`; project: `entit-finance-prod`.
Fresh OAuth tokens obtained with both selectors on `gcloud auth print-access-token`,
kept in process memory/child environment, never emitted or saved in evidence.
Only owned private throwaway buckets were used, in `europe-west1`, with uniform
bucket access, public-access prevention, zero soft-delete duration and a one-day
delete lifecycle as a cleanup backstop. All resources were explicitly cleaned.

## Measurements

[`results.json`](results.json) contains **11 live trials**: ten local-oracle
correctness scans and one default production-owner authority probe. Narrow and
selective wide savings are asserted >75% on these fixed fixtures, not universal.

| Query                                 | Query HTTP body bytes | HEAD + GET requests |   Rows |
| ------------------------------------- | --------------------: | ------------------: | -----: |
| Wide full scan                        |            18,655,696 |                  20 | 65,536 |
| Narrow projection                     |               566,336 |                  28 | 65,536 |
| Selective narrow                      |                65,565 |                   5 |     16 |
| Selective wide                        |             1,196,150 |                   6 |     16 |
| Typed-null elimination                |             9,348,640 |                  20 | 16,384 |
| Special filenames wide                |            18,655,696 |                  20 | 65,536 |
| Empty                                 |                   178 |                   2 |      0 |
| Selective wide without statistics     |             1,688,105 |                  21 |     16 |
| Requester Pays, no header, narrow     |               566,336 |                  28 | 65,536 |
| Requester Pays, explicit header, wide |            18,655,696 |                  20 | 65,536 |

**Projection saved 96.96%; selective wide filtering saved 93.59%.** These are
query-only HTTP payload measurements. View creation and later EXPLAIN each add
32,768 body bytes and four requests on two-file trials; they are recorded
separately. Query counters include scan binding. No headers/TLS/wire overhead.
Counters come from stock HTTPFS `HTTPState`, captured before `QueryEnd` resets,
using the exact pinned internal header (hash checked before compilation).
Independent loopback tests validate native body/request totals against server
counts. No custom transport, proxy or estimated file-size calculation is used
for these live measurements; this instrumentation is prototype-only internal ABI.

Remote types/values match verified local fixtures under GRV-shaped exact-file
cast/typed-null UNION views, with hive inference and union-by-name disabled.
Special keys include spaces, plus, percent, hash, Unicode and an SQL apostrophe,
encoded into literal HTTPS paths. Broader wildcard/dot-segment/encoded-slash
canonicalization remains a production gate.

## Requester Pays: success with a limitation

The test enabled Requester Pays on its own bucket and verified the metadata
flag through the GCS JSON API. A stock HTTPFS scan with
`EXTRA_HTTP_HEADERS MAP {'x-goog-user-project': 'entit-finance-prod'}` succeeded.
A no-header scan **also succeeded** for this explicitly selected identity.
Google documents a no-project exception for callers with
`resourcemanager.projects.createBillingAssignment` permission. We have not
independently established that this specific permission caused the result.

An initial run incorrectly expected mandatory no-header rejection, stopped
when the read succeeded, and still cleaned all seven object generations and
its bucket. The final run treats this as a measured limitation, not failed
OAuth transport. See [`initial-run-cleanup.json`](initial-run-cleanup.json).

Thus the prototype proves header-bearing GCS reads are accepted, and offline
mock checks prove the header is emitted. It does **not** prove billing
attribution or mandatory project rejection for lesser-privileged identities.
Do not advertise complete Requester Pays qualification from this result.

## Guard and token boundaries

Exact standalone allowlisted paths rejected unlisted, glob and nested-unlisted
reads. The actual existing production owner rejected direct/nested/glob HTTP
scans by default. This is **negative boundary evidence**, not positive trusted
production HTTPFS scanner integration. Secret scopes are prefix routing, not
exact authorization or a sandbox. Redirect containment, catalog/config escape,
rebind, cancellation and read-only authority remain production gates.

Tokens are refreshed before each case; no mid-query refresh or expired-token
recovery test was run. Replacing a secret need not update already-open handles.
`BEARER_TOKEN` is redacted by DuckDB's secret renderer; token-bearing setup SQL
must still never be logged. Do not put Authorization in extra headers, which
are not redacted. Only the non-secret billing project uses the header map.

No object-generation pinning/out-of-band replacement guarantees were tested.
Verified preparation and best-effort query-time identity remain separate.

## Cleanup and reproducibility

[`cleanup.json`](cleanup.json): all seven final-run object generations deleted,
all-version inventory empty, bucket deleted and HTTP404 verified. Absence also
independently confirmed with explicit-account/project gcloud describe. No new
keys, service accounts, IAM grants or organization policies participated.

To reproduce **only with explicit authorization for the specified project**:

```sh
python3.13 prototypes/cloud-views/httpfs/live_oauth.py --approved \
  --account=EXPLICIT_ACCOUNT --project=EXPLICIT_PROJECT \
  --fixtures /absolute/private/native-fixtures
```

Requires existing pinned native build/headers/extensions and the local oracle
fixtures described in [the parent README](../README.md). It prints the exact
owner-only state path and performs tests/cleanup in `finally`. The earlier live
runs used a fixed `/tmp` pointer; the current script no longer creates or relies
on that unsafe shared pointer. If interrupted, use the printed exact journal
rather than guessing bucket names:

```sh
python3.13 prototypes/cloud-views/httpfs/live_oauth.py \
  --cleanup-state /absolute/private/owned/state.json
```

Post-run safety review hardened the resource orchestrator without further cloud
operations: atomic owner-only journal/evidence writes, pre-recorded creation
intent, a unique bucket label included in the creation request, marker-verified
cleanup, and absent-bucket reconciliation after interrupted deletion. Current
cleanup refuses existing unmarked/mismatched buckets; the old completed journals
need no further cleanup. Environment/configuration authentication overrides are
rejected rather than silently changing the selected principal. These newer
creation/recovery paths were tested with deterministic mocks, not another live
run. Historical live results remain from the recorded execution; they do not
prove every crash recovery path or authenticate the effective principal against
all possible gcloud config overrides retroactively.

Scripts suppress external error details; inspect stage/status and private state,
never print OAuth tokens. This is an experimental resource orchestrator, not
GRV's normal credential provider or production cleanup tool. Restarted cleanup
proves bucket absence, but deletion counts can be incomplete if interrupted
between a remote deletion and its journal write.
