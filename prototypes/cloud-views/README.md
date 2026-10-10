# Experimental cloud views and pushdown

Companion to [the draft RFC](../../spec/grv-cloud-views-pushdown-v1-rfc.md).
These experiments do **not** add a production GCS-view capability. No GRV
production files, public schemas or advertised capabilities are changed.

**Design update:** initial GCS support is planned with standard DuckDB
HTTPFS/OAuth HTTPS, not HMAC keys prohibited by production policy. The custom
generation-pinned transport below is historical evidence, not the shipping
reader. The [stock-reader loopback prototype](httpfs/README.md) passed 11 trials
with synthetic HMAC signatures; HMAC live auth was blocked. The subsequent
[keyless OAuth HTTPS live experiment](httpfs/oauth-live-evidence/README.md)
now passes ten oracle scans and one authority probe using stock HTTPFS, with
**96.96% query-only projection / 93.59% selective-wide payload savings**. No keys,
new service accounts, IAM grants or policy changes; all fixtures cleaned. See
[validation status](httpfs-validation-status.md) for remaining production gates.

## Evidence layers

| Experiment                       | What it establishes                                                                                              | What it does not establish                                                           |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `native/run.sh`                  | Pinned-engine behavior with GRV-shaped views and an instrumented exact-object filesystem over local bytes        | Production S3 signing/transport, live GCS, protected adapter lifecycle               |
| `native/live.sh`                 | Joined native DuckDB/GCS range scans with real payload reductions; 10 oracle trials passed                       | Protected adapter lifecycle, independent least-privilege auth, renewal, stock HTTPFS |
| `gcs_range.py` loopback tests    | Intended GCS JSON API request construction, bounded/validated responses, generation pinning and failure behavior | Actual Google API behavior or DuckDB integration                                     |
| Opt-in `gcs_range.py` live probe | Read-only metadata/header/footer ranges (and optional full hash) against a user-authorized GCS fixture           | View creation, query pushdown, auth refresh, production conformance                  |

The live native experiment joins DuckDB and real GCS reads; the simulator and
Python fault/transport experiments remain separate supporting evidence. None is
a production GCS view implementation. See [live evidence and cleanup](live-evidence/README.md).

## GCS transport contract tests

Python 3 standard library only; no dependencies, credentials, or cloud network
needed. Tests start an ephemeral loopback HTTP server and clean it up.

```sh
python3 -m unittest discover -s prototypes/cloud-views -p 'test_*.py' -v
```

Recorded result: **15 tests passed** under Python 3.9 and Python 3.13. Covered behaviors:

- exact encoded JSON API object names, including UTF-8, literal percent and space;
- bounded 64-KiB media requests and fixed generation selectors/preconditions;
- explicitly selected quota project;
- retained old generations remain readable after replacement; deleted pinned
  generations fail instead of reading the new current object;
- size/generation mismatch and encoded/transcoded objects refused;
- 200 instead of 206, bad Content-Range, wrong generation, unexpected content
  encoding, truncated responses and redirects refused;
- optional generation response header absent, but request identity remains fixed;
- full SHA-256 comparison and mismatches;
- invalid URI/bounds/credentials and repinning refused before reading;
- duplicate headers/metadata fields, oversized/hostile metadata, late chunk
  failure, encoding mutation after pinning, and redacted authentication errors.

The reader bounds metadata at 16 KiB, transport chunks at 64 KiB and caller
reads at 8 MiB. The full-hash check streams bounded chunks. Failed responses
expose no bytes to the caller; counters are accepted bytes, not all bytes that
may have reached the network stack. Redirects/proxies are disabled; the live
endpoint is fixed. This is not a complete production GCS URI parser, credential
provider, cancellation implementation or HTTP client. Socket timeouts are not
a total deadline; aggregate request/hash budgets remain production work.
No retries or token
refresh are implemented. `pin` deliberately refuses a changed validator;
production's full-hash fallback requires separate integration.

## Optional, explicitly authorized live GCS probe

**Not run as part of the deterministic tests.** An authorized real-cloud probe
has now passed; see [live evidence](live-evidence/README.md) for its exact scope
and limitations. Use only a known fixture and
read-only identity you authorize. It does not list buckets, upload, mutate or
delete objects. Obtain the fixture size and generation from its trusted
manifest; do not discover arbitrary production data. Token value is supplied
through an environment variable, never a command-line argument or saved output.
Avoid shell tracing/history that could expose tokens. For an account you select:

```sh
export GRV_PROTOTYPE_GCS_TOKEN="$(gcloud auth print-access-token \
  --account reader@example.com --quiet)"
python3 prototypes/cloud-views/gcs_range.py \
  --uri 'gs://fixture-bucket/exact/part.parquet' \
  --size 123456 --generation 1234567890123456 \
  --project your-quota-project
unset GRV_PROTOTYPE_GCS_TOKEN
```

Replace every placeholder. The example is not permission to use an ambient
account or access other objects. Keep credentials out of saved logs. With no
`--sha256`, this checks current metadata and Parquet header/footer magic only;
it does **not** validate the entire Parquet footer/schema or full contents.
Add `--sha256 <trusted-manifest-hash>` to hash every byte of the pinned object:
this deliberately incurs full download costs and is reported separately.

Output contains request/accepted-byte counters and whether full hashing ran;
never tokens or server error bodies. Generation is a string, not a JSON number.
A read of the retained old generation may succeed after a replacement; a
missing old generation must fail. Test mutation scenarios only with explicit
write authorization in a separate disposable fixture harness.

## Native scan experiment

See [`native/README.md`](native/README.md) for compile/run instructions and
measured evidence. **26 oracle trials passed** on patched DuckDB **v1.5.6**,
independently reproduced with identical evidence. On the ordered fixture,
projection reduced callback bytes **96.84%** and selective wide filtering
**93.42%**. Both scheme identities produced identical results because they use
the same simulated transport, not independent cloud implementations.

Native transport is simulated over immutable local bytes; its scheme and
generation tests must not be presented as Google Cloud protocol validation.
A real custom GCS filesystem and protected scanner integration now join real cloud reads experimentally via [`native/live.sh`](native/LIVE.md).
Production guarded adapter integration remains outstanding. Existing regression checks: native adapter library tests
**60 passed, 1 live-credential test ignored**; script tests **49 passed** with
Python 3.13 (the script suite requires newer Python than the system 3.9).

## Remaining work before production support

The RFC contains the full release checklist. In particular:

1. Integrate the validated **stock HTTPFS/OAuth HTTPS** path into production:
   explicit identity/token expiry, canonical filenames and positive exact guards.
   Live transport/payload feasibility is proved, not the full adapter lifecycle.
2. Implement private temporary scoped secrets with mandatory account/project
   selectors; validate Requester Pays or explicitly refuse unsupported buckets.
3. Preserve preparation verification and qualify production S3/GCS pushdown;
   document GCS best-effort query-time bytes rather than generation pinning.
4. Coordinated pre-1.0 contracts, transactional refresh, holds/builds, source-free
   receipts, guarded rebinding, packaging and ordinary DuckDB consumption.

Custom GCS bearer transport, renewal callbacks and generation-pinned query
reads are deferred, not prerequisites for the simplified initial release.

Passing these prototypes is evidence toward a design, not a claim that all
cloud or lifecycle wrinkles are resolved.
