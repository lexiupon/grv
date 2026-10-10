# Live GCS experiment

Project: **entit-finance-prod** (`828148855924`). Explicit selected identity:
`jiacao@on.sinch.com`. Only synthetic data in a newly created owned bucket was
used; no pre-existing bucket, application data, project IAM or global gcloud
configuration was changed.

Bucket: `grv-view-probe-cbd6142146e5436e90558e19`, `europe-west1`, Standard,
uniform bucket-level access, public access prevention enforced, soft delete
disabled, one-day object deletion lifecycle as a backstop. Versioning was
explicitly enabled on this bucket for retained-generation tests; Requester Pays
was then enabled for billing-project forwarding tests.

## Transport findings

[`transport.json`](transport.json) records real Google JSON API outcomes:

- Canonical special-character key read and full SHA-256 match.
- Bounded media ranges with fixed generation selector and precondition.
- Invalid credentials and contradictory generation precondition refused.
- Retained old generation still read correctly after overwriting the key.
- Deleting that exact old generation made the pinned reader fail; it did not
  switch to the replacement object.
- Changing outer `contentEncoding` to gzip after pinning made the range reader
  refuse; restoring identity metadata restored the read.
- Explicit `userProject=entit-finance-prod` succeeded with Requester Pays enabled.
  The same selected principal also succeeded without `userProject`; therefore
  absent-project refusal and actual billing attribution are **not established**.

The synthetic transport Parquet fixture was 287,548 bytes. The full-hash and
magic probe made eight HTTP requests (one metadata plus seven media requests),
accepting 287,560 media bytes: every byte once for hashing plus 12 magic bytes.
Mutation/failure probes made additional requests, not included in those counters.

This uses the same available principal for provisioning and reading; it does
not prove an independent least-privilege reader, token renewal or provider-chain
isolation. Tokens were obtained with explicit account/project, held privately
and not printed or written to evidence. Local fixture paths and secrets are not
part of committed evidence.

## Native scanner: real GCS ranges

[`native.json`](native.json) records **10 passing correctness-oracle trials**
using patched DuckDB **v1.5.6**, the GRV-shaped per-file cast/typed-null UNION
views, and the custom experimental native libcurl GCS filesystem. Real requests
use the fixed HTTPS Google JSON download endpoint, generation plus
`ifGenerationMatch`, explicit quota project, and exact 206/length/range checks.
Each request is at most 64 KiB. Fixture outputs match local materialization.

| Query                                    | Accepted HTTP payload bytes | Requests |
| ---------------------------------------- | --------------------------: | -------: |
| Wide full scan                           |                  18,721,232 |      294 |
| Narrow projection                        |                     623,056 |       22 |
| Selective wide scan                      |                   1,262,788 |       25 |
| Typed-null branch elimination            |                   9,414,176 |      158 |
| Decimal/timestamp values                 |                   2,426,170 |       44 |
| Decimal/timestamp filter                 |                   1,262,788 |       26 |
| Empty selection                          |                      98,304 |        6 |
| Empty file                               |                         534 |        3 |
| Selective scan without column statistics |                   1,754,743 |       40 |
| Special-key selective scan               |                   1,213,636 |       22 |

**Narrow projection saved 96.67%; selective wide filtering saved 93.25%.**
Aggregate: **36,777,427 accepted/received payload bytes across 640 requests**,
with no rejected or retried payloads in these scans. Counters include view
binding and EXPLAIN reads, so they differ slightly from the earlier simulated
experiment which excluded EXPLAIN traffic. HTTP/TLS overhead, upload bytes,
credential lookup and Python transport/failure probes are not included.

Each trial uses a fresh engine/connection with one thread and external-file
caching disabled. Local oracle materialization is not in the network counters.
This proves a joined experimental native scanner and real GCS transport, **not**
a guarded production adapter pull/refresh or stock HTTPFS reader. Ten-minute,
2,500-request and 120,000,000-byte reservation limits bound the native run.

The native parser currently uses a stricter URI spelling than GRV's printable
key escaping: apostrophe and plus are escaped as `%27` and `%2B` in the native
special-key manifest, while the Python transport follows GRV spelling. Both
reach the same exact GCS object. This discrepancy is an explicit production
integration wrinkle, not evidence that the native conversion can be dropped
into GRV unchanged. Live scanner generation mutation was not run; the Python
transport tested retained/deleted generations separately.

## Cleanup

[`cleanup.json`](cleanup.json): **seven retained/live object generations deleted**,
all-version inventory verified empty, exact throwaway bucket deleted, and
absence verified by **HTTP 404**. Soft deletion was disabled before any upload.
No cloud test resources were intentionally left behind. The one-day lifecycle
was only a backstop, not the cleanup mechanism.

## Reproduction and safety

`../live_gcs.py` is an explicitly scoped experimental harness. It requires a
private state file recording successful creation of a uniquely named bucket in
this exact project. It checks bucket/project identity and private access before
mutating data; every upload intent is journaled before the effect. Fixture
objects are confined to `fixture/`, with <=32-MiB object limits. It has no
bucket discovery or recursive deletion path.

After provisioning a **new** owned bucket and recording a private state, the
stages are:

```sh
python3.13 prototypes/cloud-views/live_gcs.py --state /private/owned-state.json \
  transport --fixture /private/synthetic.parquet
python3.13 prototypes/cloud-views/live_gcs.py --state /private/owned-state.json \
  requester-pays
python3.13 prototypes/cloud-views/live_gcs.py --state /private/owned-state.json \
  upload-native --fixtures /private/native-fixtures
# Run native live reader with fresh explicitly obtained token in env; see native docs.
python3.13 prototypes/cloud-views/live_gcs.py --state /private/owned-state.json cleanup
```

These commands are not permission to reuse production objects. The state
contains project/project number, explicit account, exact `grv-view-probe-<24 hex>`
bucket, location, created timestamp, local private directory and `created:true`.
Do not fabricate successful creation ownership for an existing bucket.

Cleanup inventories **only the owned bucket**, including every retained object
version. It validates the entire inventory against pre-journaled exact keys
before any delete, deletes exact generations conditionally, verifies all
versions empty, removes that bucket, and verifies HTTP 404. Unexpected objects
or pagination stop cleanup for manual review. Bucket deletion is not authorized
for any other bucket; the harness never changes project IAM.

## Remaining release gates

This experiment is not production GCS-view capability. The selected initial
reader is now **stock HTTPFS/OAuth HTTPS**, not this historical custom
bearer/generation-pinned transport. [Stock keyless live evidence](../httpfs/oauth-live-evidence/README.md)
proves transport/oracle/payload feasibility without keys. Production exact guard scopes/rebind,
transactional pull refresh, fixed-build holds, revised-contract receipt replay,
private temporary secrets, resource limits and ordinary external DuckDB
consumption still require implementation and tests. See the [RFC](../../../spec/grv-cloud-views-pushdown-v1-rfc.md).
