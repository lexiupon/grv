# Experimental native Parquet pushdown proof

**Not production code. SIMULATED cloud transport.** This standalone C++ program
uses GRV's existing patched DuckDB **v1.5.6** dylib and its compiled static Parquet
extension. It does not load HTTPFS, contact S3/GCS, use credentials, invoke GRV's
production reader/guard, or validate a production cloud integration.

See the [main RFC](../../../spec/grv-cloud-views-pushdown-v1-rfc.md) for the
draft design and [findings.md](findings.md) for experimental observations.

A separate opt-in [`live.sh` path](LIVE.md) now connects native DuckDB to real
GCS ranges. Its [recorded live evidence and cleanup](../live-evidence/README.md)
shows 10 passing oracle trials. The simulated results below remain distinct
from that live transport evidence and do not validate production adapters.

## Reproduce (macOS, existing native build)

From the repository root:

```sh
bash prototypes/cloud-views/native/run.sh \
  --output prototypes/cloud-views/native/results.json
```

Prerequisites: the existing `/tmp/grv-duckdb-native/cmake/libgrv_duckdb_guard.dylib`,
matching `/tmp/grv-duckdb-native/duckdb-1.5.6` source headers, and a C++17 compiler.
The helper compiles only `prototype.cpp`; it installs/downloads nothing and does
not rebuild DuckDB or link a separate stock engine. Overrides:

```sh
GRV_NATIVE_DIR=/path/to/existing-native \
GRV_DUCKDB_SOURCE=/path/to/matching/duckdb-1.5.6 \
GRV_NATIVE_BUILD=/path/to/existing/cmake \
CXX=c++ bash prototypes/cloud-views/native/run.sh --output /tmp/native-proof.json
```

`run.sh` contains the complete compile/link command (headers for core, Parquet,
and vendored Thrift; direct dylib link and rpath). Static Parquet is explicitly
initialized via the built library's `grv_native_load_static_extensions` export.
The C-API database wrapper exposes the matching native C++ connection for FS
registration. This is intentionally an internal-ABI experiment, not a supported
public C-API integration pattern. Linux/other ABIs are not tested.

The helper removes its executable/build directory on exit. The program's RAII
fixture directory removes all generated Parquet files on success or exception.
The optional JSON is retained intentionally. No other repository paths change.

## What is measured

Two ordered files contain 32,768 rows each, eight 4,096-row groups per file,
uncompressed Parquet with bloom filters disabled. Fields are BIGINT,
DECIMAL(18,2), microsecond TIMESTAMP, a 256-character payload, and a trailing
VARCHAR present only in the newer file. Each branch casts physical columns to
**the same native type** and supplies a typed NULL for the old trailing field:

```sql
SELECT CAST(id AS BIGINT) AS id,
       CAST(amount AS DECIMAL(18,2)) AS amount,
       CAST(ts AS TIMESTAMP) AS ts,
       CAST(payload AS VARCHAR) AS payload,
       CAST(NULL AS VARCHAR) AS extra
FROM read_parquet('s3://fixture-bucket/old.parquet',
                  hive_partitioning=false, union_by_name=false)
UNION ALL
SELECT CAST(id AS BIGINT) AS id,
       CAST(amount AS DECIMAL(18,2)) AS amount,
       CAST(ts AS TIMESTAMP) AS ts,
       CAST(payload AS VARCHAR) AS payload,
       CAST(extra AS VARCHAR) AS extra
FROM read_parquet('s3://fixture-bucket/new.parquet',
                  hive_partitioning=false, union_by_name=false)
```

Every trial creates a fresh in-memory database and immutable object map; external
file caching is disabled and threads=1. Native seekable FS reads split into
**at most 64 KiB** callbacks, matching the API/chunking strategy in
`crates/grv-adapter-duckdb/native/verified_s3.inc`. Callback payload bytes and
calls are counted, including view binding/footer reads. EXPLAIN runs **after**
the measured query; its own traffic is excluded. Range repetition counts again,
not as unique coverage. Fixture loading, oracle materialization and metadata
inspection are outside these simulated transport counters.

Results are compared with a fully materialized local-file oracle (same types,
NULLs and values), including ordered raw decimal/timestamp rows. JSON contains
version, source ID, exact cloud view SQL, query, EXPLAIN, result, and per-object
bytes/calls. The built engine reports an empty source ID; the run used dylib
SHA-256 `35f926113bd95f309af4dbd557bcb494141a4621879c5461c5b035706754ac45`.

## Actual recorded results

`results.json`: **26 oracle trials passed**, identical measured totals for
`s3://` and `gs://`:

| Trial                                         | Callback bytes | Calls |
| --------------------------------------------- | -------------: | ----: |
| Wide full scan                                |     18,688,464 |   292 |
| Narrow full scan (`sum(id)`)                  |        590,288 |    20 |
| Selective wide (`id BETWEEN 40000 AND 40031`) |      1,230,020 |    23 |
| Typed-null elimination (`extra='present'`)    |      9,381,408 |   156 |
| Empty selection (`id<0`)                      |         65,536 |     4 |
| Decimal/timestamp filter                      |      1,230,020 |    24 |
| No column statistics, selective wide          |      1,721,975 |    38 |
| Stats-unsuitable full-match predicate         |     18,688,464 |   308 |
| Empty Parquet file                            |            356 |     2 |

Projection saves **96.84%**, selective wide filtering **93.42%**. Assertions
require >75%, not exact totals/EXPLAIN text. Typed-null elimination leaves only
32,768 footer bytes on the old object versus 9,339,824 in its full scan.

The no-statistics fixture strips footer column statistics with the already-linked
Parquet/Thrift codec, preserving pages. It still saves bytes: native filtering
reads IDs first and can avoid other columns in nonmatching groups. An initial
assumption of >2x more bytes than the stats scan was disproved; measured increase
is 40%. The test now requires >25%. Function predicates can appear in the native
scan without usable min/max pruning: `md5(id::VARCHAR)=...` is correct and still
saves via late materialization, while `length(md5(id::VARCHAR))=32` reads all wide
data. A BIGINT-to-VARCHAR filter is correct but reads all ID groups. Pushdown is
not a universal stats-pruning guarantee.

## Identity/security limits

Exact byte-string URI allowlisting only: no glob expansion, prefix delegation,
or cloud fallback. Unlisted names, wildcards, query suffixes and child paths are
refused. A URI with encoded spaces/percent/hash/unicode, SQL apostrophe, plus and
bracket characters passes only when explicitly listed. **This does not validate
production canonical URI parsing/decoded alias behavior.**

Each callback checks simulated pinned ETag **and** generation. Mutation after the
first successful callback makes the next engine read fail for both schemes and
both validators. Repeated same-range reads through a live FS handle also refuse
a changed validator; the FS retains no read cache. Engine cache-enabled lifecycle,
mid-response mutations, conditional HTTP status/header checks, content hashes,
network retries, IAM, GCS generation semantics and production guards remain
unvalidated. Generation/ETag are simulator fields, not real cloud guarantees.

Only the enumerated native types and identity casts are established. Widening,
rounding, decimal rescaling, timestamp timezone changes, nested types and arbitrary
nonidentity casts need separate proofs; do not extrapolate these byte results.
