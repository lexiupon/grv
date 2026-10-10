# Experimental live GCS path (opt-in)

This is a read-only prototype, not a production cloud connector. `run.sh` retains its
simulated behavior. `live.sh` reuses the same pinned patched DuckDB v1.5.6 build,
compiles `live.cpp` with system `-lcurl`, and never downloads/builds DuckDB or
executes credential discovery. `live.cpp` includes the base prototype under a
main guard. Python 3 is needed only for offline manifest conversion/validation.

## Export, upload, scan

```sh
bash prototypes/cloud-views/native/live.sh export --directory /absolute/fresh/fixtures
```

The directory must not exist. Export writes deterministic `old.parquet`,
`new.parquet`, `empty.parquet`, `old-no-stats.parquet`, `new-no-stats.parquet`,
and `fixtures.json`. Metadata/stdout contain byte sizes and MD5 hashes (byte
identity only, not a security claim). Rows/schema/layout match the simulated
fixtures: two ordered 32,768-row files, eight row groups each, decimal amounts,
microsecond timestamps, wide payloads, and an evolved nullable `extra` column.
No-statistics variants remove footer column statistics, preserving data pages.

The parent uploads these **five** fixtures to its owned bucket and records exact
object metadata. No cloud mutation is performed by this native program. The
parent supplies a JSON manifest with this schema:

```json
{
  "project": "entit-finance-prod",
  "objects": [
    {
      "name": "old.parquet",
      "uri": "gs://owned-bucket/fixture/native/old.parquet",
      "local_path": "/absolute/fresh/fixtures/old.parquet",
      "size": 9317962,
      "generation": "1234567890123456",
      "sha256": "optional-lowercase-SHA256-of-local-file"
    }
  ]
}
```

Include an entry for every exported Parquet file. `size` may be a number or a
positive decimal string; `generation` must be a positive decimal string.
Optional `sha256` is verified locally by the adapter when provided. Optional
sixth entry `name: "special.parquet"` may reference the local `new.parquet` under
a separately uploaded special object key. Do not include `fixtures.json` as an
object. The JSON adapter never accesses cloud. The C++ program consumes bounded
TSV internally; callers may also supply TSV directly (no header):

```text
role<TAB>canonical-gs-uri<TAB>absolute-local-path<TAB>decimal-size<TAB>decimal-generation
```

Roles: `old`, `new`, `empty`, `old-no-stats`, `new-no-stats`, optional `special`.

```sh
# Parent obtains token separately; do not print it or put it in command arguments.
# GRV_PROTOTYPE_GCS_TOKEN must already be set in the environment.
bash prototypes/cloud-views/native/live.sh scan \
  --manifest /absolute/native-manifest.json \
  --quota-project entit-finance-prod \
  --output /absolute/fresh-results.json
```

Output must be a fresh file; it is created mode 0600. No token, HTTP
headers/bodies, bucket/key names, generations, or local paths are retained in the
JSON. SQL/plan object paths are replaced by role labels. Query result values are
retained (these generated fixtures contain no customer data). Completed trials
and aggregate budget counters are retained even if a later trial/check fails.
Failures omit raw DuckDB/server error text to avoid accidental disclosure.

## Transport and trials

Fixed origin/endpoint:
`https://storage.googleapis.com/download/storage/v1/b/<bucket>/o/<encoded-key>?alt=media&generation=<g>&ifGenerationMatch=<g>`.
Authorization is only `GRV_PROTOTYPE_GCS_TOKEN`; quota is explicitly passed as
`x-goog-user-project`. TLS peer/host verification enabled. No proxies, redirects,
netrc, retries, content decompression, transport byte cache, or full-object
fetch. Each range is at most 65,536 bytes; exactly 206, Content-Range start/end/
manifest-size, Content-Length, body length and absent/identity Content-Encoding
are required. Duplicate relevant response headers are refused. A generation
response header, when present, must match. Unexpected oversized payload/header
streams abort callbacks; native transport buffering is one 64KiB response plus
at most 16KiB response headers. Engine memory limit is 512MiB, one thread, spill
disabled, external file cache disabled.

A global ten-minute deadline, ten-second connect timeout, up to thirty-second
request timeout (clamped to remaining deadline), 2,500 attempted requests and
120,000,000 reserved requested payload bytes bound the run. Accepted payload
bytes/calls are actual validated curl responses, including view binding and
EXPLAIN traffic; attempted requests and received payload are also recorded.
Every trial uses a fresh Engine/connection/FS and a locally materialized oracle.
Trials cover full wide, narrow, selective, typed-null elimination, decimal/
timestamp values/filter, empty selection/file, no-statistics, optional special
URI. Results/types must match the oracle; relative projection/filter/null-branch
and no-statistics reduction checks must pass. EXPLAIN plans are retained.

## Exact special keys and limitations

Canonical URI keys use uppercase percent escapes for every byte except ASCII
unreserved characters and literal `/`. For example the raw key
`space quote'percent%hash#star*[bracket]+unicode-雪.parquet` is represented as:

```text
gs://owned-bucket/space%20quote%27percent%25hash%23star%2A%5Bbracket%5D%2Bunicode-%E9%9B%AA.parquet
```

The URI is decoded **once** to the exact key, then re-encoded as one JSON endpoint
path segment, including `/` as `%2F`. Literal percent requires `%25`; plus
requires `%2B`; quote `%27`; wildcard/brackets `%2A`, `%5B`, `%5D` are literal,
not expanded. Unicode is UTF-8 percent encoding. Noncanonical aliases, query/
fragment suffixes, raw reserved characters, lowercase escapes, and `%2F` slash
aliases are rejected. NUL/CR/LF keys are unsupported. Invalid UTF-8 is not locally
validated (GCS enforces its key requirements). No listing/globbing/prefix
allowlist, URI aliases or fallback FS is supported. No live generation mutation
or ETag test is performed; generation and ifGenerationMatch pin every request.

## Offline checks

```sh
bash prototypes/cloud-views/native/live.sh self-test
bash prototypes/cloud-views/native/run.sh --output /absolute/offline-regression.json
```

Self-test makes zero network calls: exact special-key conversion, single decode,
noncanonical refusal, read-only allowlist, seek, bounded callback handling and
duplicate header refusal. The existing simulated suite verifies 26 correctness
trials, relative bytes, allowlisting, repeated reads, and ETag/generation mutation
refusal. Offline success is **not** evidence of live GCS results; the parent must
run live scan and review its retained report.
