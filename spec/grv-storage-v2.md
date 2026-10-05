# GRV (Golden Record Vault) Storage v2 specification

|         |            |
|---------|------------|
| Status  | draft      |
| Version | 2          |
| Date    | 2026-10-05 |

## Overview

GRV needs a storage layout for its data. A **dataset** is a collection of
**tables**; a table is versioned, and may additionally be partitioned. The
layout must satisfy:

- It works identically on a local filesystem, S3, and GCS — no
  backend-specific layout variants.
- Multiple **versions** of a table (or of a table's partition) coexist on
  disk; nothing is overwritten in place.
- Concurrent writers to the same (table, partition) are coordinated by the
  layout itself — no central allocation service.
- The state of a dataset — which of its tables are present, and at which
  version — is a **revision**: a complete, named snapshot that can be
  re-resolved at any time. The state advances only through an explicit
  **publish step**, and a table can leave the state by being omitted from
  a later revision.
- Concurrent publications that change the same (table, partition) are
  detected, never silently overwritten.
- Provenance is auditable from the data directory alone: which **run**
  produced a given version, and which revisions of other datasets a version
  was **derived** from.
- The layout is engine-agnostic: GRV is the ingest (raw/input) and the
  release (output) layer, and transformation engines — dbt among them —
  are the build layer in between (§11).

GRV v2 is the format of a new store. There is no earlier on-disk format and
no data to migrate, so nothing here is constrained by compatibility with
earlier drafts.

## Conventions

- The capitalized key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are
  to be interpreted as described in RFC 2119 and RFC 8174. In the
  specification sections, lowercase "must" and "must not" are equally
  binding; capitals only add emphasis.
- **Times** are RFC 3339 timestamps in UTC. **Durations** in `grv.json`
  (§2) are integer seconds.
- **Random identifiers** — claim tokens, lease and owner tokens, mutation
  ids, retention ids, and pin ids — are canonical lowercase UUIDs (version 4 or
  7), generated freshly for each use. **Run ids** and **operation ids** are
  ULIDs (§6).
- **JSON equality.** Two JSON documents are equal when they are equal as
  JSON values: object member order and insignificant whitespace are
  ignored; array order is significant.

## Specification

### 1. Storage backends

`GRV_DIR` is a single path scheme, rooted at either:

- a local filesystem path, e.g. `/var/grv/data`
- an S3 URI, e.g. `s3://bucket/prefix`
- a GCS URI, e.g. `gs://bucket/prefix`

Everything below is defined in terms of relative paths under `GRV_DIR`.
The layout is identical on all three; backends differ only in how they
fulfil the **backend contract** below.

**Backend contract.** The layout requires six operations:

| operation            | meaning                                                             |
|----------------------|---------------------------------------------------------------------|
| `get`                | read an object's content and its validator                          |
| `head`               | read an object's validator and size without returning its content   |
| `list-by-prefix`     | list object names under a prefix; optionally with delimiter `/`, returning only the immediate child names (common prefixes) |
| `delete`             | remove an object (idempotent)                                       |
| `conditional-create` | create only if the object does not exist, atomically with its full content; fail otherwise; returns the new validator. Readers observe either no object or the complete content |
| `conditional-put`    | replace only if the object's validator matches one read earlier (compare-and-swap); readers observe the old or the new content, never a partial one; returns the new validator |

A *validator* is an opaque, backend-specific string used for content
validation and conditional writes: the S3 `ETag`, the GCS object
`generation`, or, locally, the lowercase hex SHA-256 of the object's
content. It is not portable across backends or copies, and is not
necessarily a rewrite counter: an S3 ETag or a local content hash repeats
when identical content is rewritten. Every mutable coordination record
(`grv.json`, `.claim`, `.schema.json`, `LATEST`, run controls, and
allocation records) MUST therefore carry a fresh random `mutation_id` on
every transition, including renewal, release, and takeover, so that its
content never returns to an earlier value, even when nothing else changes.
Control records are never deleted or unconditionally rewritten.

The contract requires strongly consistent object reads and conditional
writes, and listings that include completed creations. A listing is not an
atomic snapshot: decisions spanning objects require the coordination below.
Local implementations must make acknowledged writes durable (including file
and directory synchronization) before reporting success.

**Durable read-back.** A successful `get` or `head` that finds an object
MUST also establish the durability of the returned content and its path.
This applies even when the original write was never acknowledged. Locally,
the backend synchronizes the opened file, its containing directory, and
any ancestor directory entries needed to persist the path before returning
success; a synchronization error is a failed read, never evidence of
absence. A listing only discovers names: an object found by listing must
be confirmed by `get` or `head` before its presence is used as proof of a
completed write or operation. Thus ambiguous-write checks, run recovery,
and operation replay can safely adopt objects left by a writer that
crashed between making them visible and synchronizing them. No additional
backend operation is required.

**Ambiguous outcomes.** A request that fails or times out may still have
taken effect. Before treating a conditional write as lost, the caller
rereads the object:

- a `conditional-create` succeeded iff the object now holds the caller's
  content — for a data file, the same size and SHA-256; for a JSON record,
  equal JSON;
- a `conditional-put` succeeded if the current content carries the
  caller's new `mutation_id`, and the reread validator is then the caller's
  latest validator. Otherwise the outcome stays **unknown**: the write may
  have succeeded and been overwritten since. The caller must not assume
  either outcome and relies on the protocol's own proofs (§5
  `last_release`, §6 recovery, and §8 for operations and publications).

Immutable objects are never replaced, so for `conditional-create` a
reread that finds content is conclusive. An absent object means either
that the create did not take effect or that GC or temporary-file cleanup
has since removed it; retrying is harmless in both cases.

Every prefix passed to `list-by-prefix` in this specification ends in `/`
(e.g. `<table>/region=eu/`), so that `region=eu` never matches `region=eu2`
and `version=1` never matches `version=10`.

Per-backend mapping:

| operation            | local filesystem                          | S3                                            | GCS                                     |
|----------------------|-------------------------------------------|-----------------------------------------------|-------------------------------------------|
| `get`               | read and hash the opened file; synchronize the file and path before returning | `GET` + `ETag`                               | `GET` + `generation`                      |
| `head`              | `fstat` and hash the same opened file; synchronize the file and path before returning | `HEAD`                | object metadata `GET`                     |
| `list-by-prefix`    | directory walk (`readdir` for delimiter)  | `ListObjectsV2` with `prefix` (+ `delimiter`) | object list with `prefix` (+ `delimiter`) |
| `delete`            | `unlink`                                  | `DELETE`                                     | object delete                             |
| `conditional-create`| write and synchronize a temp file, then `link(2)` it to the target (fails if it exists), unlink the temp, and synchronize the target's path before success | `PUT` (or multipart complete) with `If-None-Match: *` | write with `ifGenerationMatch=0` (`x-goog-if-generation-match: 0`) |
| `conditional-put`   | under an exclusive `flock` on the containing directory: read and hash the current content, compare, write and synchronize a temp file, `rename`, and synchronize the target's path before success | `PUT` with `If-Match: <ETag>`   | write with `ifGenerationMatch=<generation>` |

**Local validator cache.** For immutable objects only — data files,
manifests, and other objects created once by `conditional-create` — a local
implementation MAY reuse a SHA-256 that it computed earlier for the same path
while the file's device, inode, size, `mtime` and `ctime` (both at nanosecond
resolution) are unchanged. The cache is consumer state outside `GRV_DIR`, and
is allowed only on filesystems with nanosecond timestamps. A cached `get` or
`head` still performs the durable read-back synchronization. The cache MUST
NOT be used for `conditional-put` targets (`grv.json`, `.claim`,
`.schema.json`, `LATEST`, run controls, allocation records) or for full
audits that require check (b) of §4.

The local backend requires a filesystem with reliable `flock` and `link`
semantics; network filesystems without them are unsupported. Local
temporary files are named `.tmp-<random>` in the target's directory; they
are never layout objects and may be removed once their modification time
is older than `max_lease_ttl` (§2). A writer whose temporary file has
disappeared retries the write or, for a version object, abandons the
version (§5). The local validator is a content hash rather than
file metadata because a `rename` frees the old inode for reuse, and coarse
file timestamps could then make a replaced file look unchanged; with
content hashes and unique `mutation_id`s, a compare-and-swap cannot succeed
against a stale read.

Two deliberate omissions: the layout never requires **rename** as a layout
operation (object stores lack it; version commits are made safe instead by
the manifest-as-commit-marker, §4 — the local backend uses rename
internally), and never requires **conditional delete**. S3 supports
conditional deletion with `If-Match`, but this layout does not use it;
deletions follow the GC protocol (§10) or are idempotent housekeeping. Nor
does it need an unconditional overwrite: every object is either created
once with `conditional-create` or is a control record changed only by
`conditional-put`.

### 2. Top-level layout

```
GRV_DIR/
├── grv.json                      # store format and shared parameters (below)
└── datasets/
    └── <dataset>/
        ├── <table>/              # one folder per table (§3)
        ├── .retired              # optional; the dataset is retired (§10)
        ├── .holds/               # dependency holds created by consumer runs (§9)
        │   └── <consumer-dataset>/revision={n}/<retention-id>.json
        ├── .runs/                # runs (§6)
        │   ├── <run-id>.control.json    # mutable run lease and phase
        │   ├── <run-id>.allocations/    # one mutable record per version allocation
        │   │   └── <claim-token>.json
        │   └── <run-id>.json            # immutable sealed run file
        └── .states/
            ├── LATEST            # JSON: revision + dataset lease + pending operation (§8)
            ├── operations/       # immutable operation descriptions (§8)
            │   └── <operation-id>.json
            ├── released-holds/   # hold release markers, written by this dataset's GC (§9)
            │   └── <consumer-dataset>/revision={n}/<retention-id>.json
            └── revisions/
                └── revision={n}/
                    ├── data.parquet       # the revision (§7)
                    ├── .superseded.json   # supersession receipt (§8)
                    └── .pins/             # revision pins (§10)
                        ├── <pin-id>.json
                        └── <pin-id>.released.json
```

- The top-level folder is always `datasets/`; each dataset gets one folder
  under it.
- A dataset is a collection of tables; each table lives in its own folder
  directly under the dataset folder.
- `.runs/`, `.states/`, `.holds/` and `.retired` are per-dataset metadata.
  The dot prefix distinguishes them from table folders. `.holds/` is the
  only prefix that other datasets' runs write to, and only to their own
  `<consumer-dataset>/` subfolder (§9).

**`grv.json`** identifies the store format and holds the parameters that
all participants must share. It is created once, with `conditional-create`,
when the store is initialized:

```json
{
  "format": "grv",
  "format_version": 2,
  "mutation_id": "3f1c2a9e-5b7d-4e0a-9c1b-7d2e4f6a8b0c",
  "max_clock_skew_seconds": 30,
  "max_lease_ttl_seconds": 900,
  "pending_grace_seconds": 604800
}
```

| field                    | notes                                                        |
|--------------------------|--------------------------------------------------------------|
| `format`, `format_version` | `"grv"` and `2`; participants MUST refuse any other value  |
| `mutation_id`            | fresh on every change (§1)                                   |
| `max_clock_skew_seconds` | `max_clock_skew`: bound on the clock difference between any two participants (§5, §10) |
| `max_lease_ttl_seconds`  | `max_lease_ttl`: upper bound on every claim and lease TTL (§5) |
| `pending_grace_seconds`  | `pending_grace`: GC's grace period (§10)                     |

Every participant, readers included, reads `grv.json` before using the
store. A missing `grv.json` means the root is uninitialized: a participant
may initialize it only while `datasets/` is empty, and must otherwise treat
the store as damaged. An administrator changes a parameter by CAS;
participants reread the file at least at the start of every run, publish
step, and GC pass; GC reads `pending_grace` and `max_clock_skew` after
acquiring the dataset's lease (§10). No protocol decision other than GC's
depends on `pending_grace`. Changes never weaken fencing, which relies only on
compare-and-swap, but lowering a value can shorten protection for work
already in flight — a read of a superseded revision, or a run awaiting
publication — so values are lowered only when that is acceptable.

The `datasets/` level is deliberate, not ceremony: `GRV_DIR` is a
*root*: it holds `grv.json`, and the layout reserves the right to place
other top-level structures there later (system state, GC staging, caches)
without a breaking change. It also keeps "list all datasets" a single
delimited prefix listing on object stores, where a root shared with other
tools' objects would make every listing a filter, and it gives every
dataset a uniform path (`<root>/datasets/<name>/...`).

### 3. Table layout and `.layout.json`

Every table folder contains a `.layout.json` describing the table's layout.
There are two cases:

**Case a — non-partitioned table:**

```
<table>/
├── .layout.json
├── .schema.json        # durable table-wide schema baseline (§4)
├── .claim              # version allocation; cycles acquire→release, never deleted (§5)
├── .pins/              # table pins (§10)
└── version={n}/
    ├── data.parquet    # plus optional data-1.parquet, data-2.parquet, ... (§4)
    ├── manifest.json
    ├── .pins/          # version pins (§10)
    └── .pruned         # GC tombstone (§10)
```

```json
{ "table": "<table>", "partition_keys": [] }
```

**Case b — partitioned table:**

```
<table>/
├── .layout.json
├── .schema.json        # shared across all partitions (§4)
├── .pins/              # table pins (§10)
└── key1=value1/
    └── key2=value2/
        ├── .claim        # version allocation; cycles acquire→release, never deleted (§5)
        ├── .pins/        # partition pins (§10)
        └── version={n}/
            ├── data.parquet    # plus optional data-1.parquet, ... (§4)
            ├── manifest.json
            ├── .pins/          # version pins (§10)
            └── .pruned         # GC tombstone (§10)
```

```json
{ "table": "<table>", "partition_keys": ["key1", "key2"] }
```

`.layout.json` schema:

| field            | type         | required | notes                                        |
|------------------|--------------|----------|----------------------------------------------|
| `table`          | string       | yes      | must equal the table folder name             |
| `partition_keys` | string array | yes      | empty = case a; keys in path order = case b  |
| `extensions`     | object       | no       | map of extension id → that extension's table configuration (below) |

Rules:

- Dataset and table names must match
  `^[a-z0-9][a-z0-9_-]{0,63}$`. The leading-alphanumeric requirement
  means no name can start with a dot, so table folders can never collide
  with `.runs/`, `.states/`, `.holds/`, `.pins/`, `.claim`, `.pruned`,
  `.retired`, or any other dot-prefixed layout object.
- Dataset names, table names, partition key names, and partition values
  use canonical lowercase ASCII on **every** backend. Uppercase input is
  rejected, never silently folded. Thus two valid sibling identifiers
  cannot differ only by case, even when created concurrently; no listing
  or name-reservation service is needed. Case-sensitive source identifiers
  must be mapped to distinct valid identifiers by the caller. This rule
  does not apply to data column names, data values, or canonical ULIDs.
- `partition_keys` is the discriminator between the two cases: empty means
  case a, non-empty means case b. Keys appear in the declared order in the
  path, and entries are unique.
- The path segment `key1=value1/key2=value2` is the **partition**. In case
  b, `version={n}` directories, `.claim`, and partition `.pins/` appear
  only at full partition depth; no intermediate directory holds versions.
- Partition **key names** and **values** must both match
  `^[a-z0-9][a-z0-9_-]{0,63}$`, and key names must not be `version` or
  `revision`, so a partition segment never reads as a version or revision
  segment. Writers validate on write (layout, claim, manifest, revision);
  non-conforming names or values are rejected. Values are always written
  as strings (a numeric year is `2025`). The restricted alphabet makes the
  `key=value` path form unambiguous — no escaping is defined or needed.
- `version={n}` is always the *last* path segment. A version is therefore
  scoped to (table, partition); a non-partitioned table is the degenerate
  case of an empty partition. `n` is written in canonical decimal (no
  leading zeros) and satisfies `1 ≤ n ≤ 2^63−1`, matching the `int64`
  revision column (§7).
- A table's `.layout.json` is created by the table's first writer via
  `conditional-create` and is **immutable** thereafter. A writer whose
  create fails reads the existing file; if its `partition_keys` differ from
  the writer's, the write is rejected — a different `partition_keys` is a
  different table (§4).
- **Extensions.** `extensions` names the extensions that every writer of
  the table must implement, keyed by extension id (for example
  `crypto-shredding/1`, defined in the companion
  [grv-crypto-shredding-v1-rfc.md](grv-crypto-shredding-v1-rfc.md)), each with its
  configuration for this table. A writer that does not implement every
  listed extension MUST NOT acquire claims, register schemas, or create
  version objects for the table. Readers that do not implement an extension
  read the stored data as-is. Because `.layout.json` is immutable, a
  table's extensions are fixed when it is created.
- In case b, each of a version's data files additionally contains one
  column per partition key, named `_{key_name}_` (e.g. `_key1_`,
  `_key2_`), of GRV type `string` (§4), duplicating the partition values
  as data (hive-style). The writer MUST NOT use a name of the form `_{k}_`
  (for a partition key `k`) for a data column, and every row's `_{k}_`
  value MUST be the non-null string equal to the partition value in the
  path. For a non-empty file this makes each data file self-describing;
  for an empty file the partition identity is taken from the path.
- The (table, partition) identifier has one canonical form — the ordered
  list of (key, value) pairs — and appears in two media: the path form
  `key1=v1/key2=v2` (directory names, and the `partition` column of the
  revision parquet, §7) and the JSON object form `{"key1": "v1", ...}`
  (`manifest.json`, run files). The JSON object must contain exactly the
  declared partition keys and their string values; object member order is
  irrelevant. Tools reconstruct the path in `.layout.json` key order and
  treat the two encodings as one identifier.

### 4. Versions and `manifest.json`

- `version={n}`: `n` is a positive integer, unique per (table, partition).
  Versions are **immutable**: once written, a version's data files and
  `manifest.json` are never modified. The only objects later added to a
  version directory are pin and pin-release records under `.pins/` and the
  `.pruned` tombstone (§10), and the only later deletions are GC pruning (§10).
- A version consists of one or more **data files** plus its
  `manifest.json`. Every version has `data.parquet`; a large version may
  additionally have `data-1.parquet`, `data-2.parquet`, …, `data-<k>.parquet`
  with contiguous indices from 1 (`<i>` a positive integer without leading
  zeros; no other `data*` files may appear in a version directory). The
  files are **union-safe**: each is a complete set of rows (no row is split
  across files), every row of the version is in exactly one file, and the
  version's data is the union of its files' rows in any order. All data
  files in one version MUST have the same logical schema (below),
  including the partition columns. The manifest's `data_files` array names
  exactly the files that exist, in the order `data.parquet`,
  `data-1.parquet`, `data-2.parquet`, … (numeric by index, not lexicographic),
  each with its SHA-256, so a consumer can
  verify the set before combining.
- `manifest.json` is the **commit marker** of the version: a version
  directory is valid only when every data file listed in the manifest and
  the manifest itself are present. Writers create the data files first and
  `manifest.json` last, all via `conditional-create` (§5), so readers never
  observe a half-written version and no writer can overwrite another's
  objects. Creating the manifest **commits** the version. A version whose
  data files are later lost (pruning, abandonment) is unreadable, and
  readers report it as such.

**Empty versions.** A version whose data files contain no rows (a zero-row
parquet) is an **empty version**. Committing an empty version of a (table,
partition) and publishing it in a revision is the explicit termination of
that partition's data: the partition stays in the state, but its current
version carries no rows — self-describing, so any consumer of the version
can see it without the revision. A terminated partition may be
re-populated by a later non-empty version. This complements state-level
omission (§7): omitting the (table, partition) from a revision removes it
from the state entirely, while an empty version keeps it present and
explicitly empty. A consumer materializing a state (e.g. the pull job, §11)
treats both as "no rows for this partition in this state." An empty version
is an ordinary version: it can be pinned and pruned like any other (§10),
and is unrelated to the `.pruned` tombstone.

`manifest.json` schema:

| field          | type    | required | notes                                         |
|----------------|---------|----------|-----------------------------------------------|
| `table`        | string  | yes      | table name (equals the folder name)           |
| `partition`    | object  | yes      | `{}` for case a; else the key/value map      |
| `version`      | integer | yes      | the version number of this directory          |
| `run_id`       | string  | yes      | ULID of the run that produced it (§6)        |
| `created_at`   | string  | yes      | RFC 3339 UTC time the manifest was written (the version's commit) |
| `claim_token`  | string  | yes      | token of the claim that allocated this version (§5); the fence |
| `data_files`   | array   | yes      | the version's data files in index order; each element `{name, sha256, size, validator}` (below); the fence |
| `row_count`    | integer | yes      | rows across all data files; 0 iff the version is empty |
| `derived_from` | array   | no       | source references; present iff derived (§9)   |
| `metadata`     | object  | no       | map of engine name → engine-specific fields; readers MUST ignore entries they do not understand (§11) |

Data file object (each element of `data_files`):

| field       | type    | notes                                                         |
|-------------|---------|---------------------------------------------------------------|
| `name`      | string  | `data.parquet` or `data-<i>.parquet`                          |
| `sha256`    | string  | lowercase hex SHA-256 of the file's content                   |
| `size`      | integer | size in bytes                                                 |
| `validator` | string  | the backend validator returned when the file was created (§1) |

Source reference object (each element of `derived_from`):

| field          | type    | required | notes                                                     |
|----------------|---------|----------|-----------------------------------------------------------|
| `dataset`      | string  | yes      | source dataset; never this version's own dataset (§9)     |
| `revision`     | integer | yes      | committed source revision (§7)                            |
| `retention_id` | string  | yes      | the run's dependency hold on that source revision (§9)    |
| `table`        | string  | no       | source table; absent means the whole source revision      |
| `partition`    | object  | no       | source partition; only with `table`; absent means the whole table |

Example (case b, derived from two sources):

```json
{
  "table": "orders",
  "partition": { "region": "eu", "year": "2025" },
  "version": 3,
  "run_id": "01M3KQA080R6Y8C2D9F0G1H2J3",
  "created_at": "2026-09-28T10:15:00Z",
  "claim_token": "9f2c1e4a-7b3d-4c1e-9a02-6d5f8e1b2c3d",
  "data_files": [
    { "name": "data.parquet", "sha256": "…", "size": 104857600, "validator": "…" },
    { "name": "data-1.parquet", "sha256": "…", "size": 52428800, "validator": "…" }
  ],
  "row_count": 48211,
  "derived_from": [
    {
      "dataset": "raw_events",
      "revision": 7,
      "retention_id": "28f68c81-dbdc-4ab6-a811-81d7b989c693",
      "table": "orders",
      "partition": { "region": "eu", "year": "2025" }
    },
    {
      "dataset": "ref_data",
      "revision": 2,
      "retention_id": "fb9b64a3-aeeb-47b4-b60c-4ad5c5e997d0",
      "table": "regions"
    }
  ]
}
```

`derived_from` is absent for versions produced directly; when present it is
a non-empty array with one element per source dependency, at the
granularity the producer knows (§9). Every (dataset, revision,
retention_id) it names must be one of its run's inputs (§6). In a
non-partitioned table (case a), the manifest's `partition` is the empty
object `{}`. The manifest does not duplicate the data's column schema: the
parquet file is self-describing about its own schema.

**Verifying a data file.** A data file *verifies* against its manifest
entry if it exists, its size equals the recorded `size`, and either (a) its
current validator (`head`) equals the recorded `validator`, or (b) its
SHA-256 equals the recorded `sha256`.
Check (a) is a metadata read and is the normal path; check (b) reads the
whole file and is the fallback when validators differ legitimately (the
directory was copied or restored to another bucket or backend) and for
full audits. On the local backend the validator is itself the SHA-256, so
the two checks coincide. A version *verifies* when every listed data file
verifies and no other `data*` file is present.

The manifest is also part of the **fence** against stale writers:
`claim_token` ties the version to the claim that allocated it, and the run
file must list the version with the same token (§5, §6).

**Logical schemas.** Schemas are compared in GRV's logical form, not as raw
Parquet schemas, because different Parquet writers encode the same column
differently. A data file's **logical schema** is the ordered list of its
top-level columns, each `{"name": …, "type": …}`, derived from its Parquet
schema by this mapping:

| Parquet physical type and annotation                       | GRV type |
|------------------------------------------------------------|----------|
| `BOOLEAN`                                                  | `"boolean"` |
| `INT32` without annotation, or with `INT(32, signed)`      | `"int32"` |
| `INT32` with `INT(8, signed)` or `INT(16, signed)`         | `"int8"`, `"int16"` |
| `INT64` without annotation, or with `INT(64, signed)`      | `"int64"` |
| `INT32` or `INT64` with `INT(bits, unsigned)`              | `"uint8"`, `"uint16"`, `"uint32"`, `"uint64"` |
| `FLOAT`, `DOUBLE`                                          | `"float32"`, `"float64"` |
| `BYTE_ARRAY` with `STRING`, `ENUM`, or legacy `UTF8`       | `"string"` |
| `BYTE_ARRAY` with `JSON`                                   | `"json"` |
| `BYTE_ARRAY` without annotation                            | `"binary"` |
| `FIXED_LEN_BYTE_ARRAY(16)` with `UUID`                     | `"uuid"` |
| `FIXED_LEN_BYTE_ARRAY(n)` without annotation               | `{"fixed_binary": {"length": n}}` |
| any physical type with `DECIMAL(p, s)`                     | `{"decimal": {"precision": p, "scale": s}}` |
| `INT32` with `DATE`                                        | `"date"` |
| `INT32` (`ms`) or `INT64` (`us`/`ns`) with `TIME(unit, isAdjustedToUTC)` | `{"time": {"unit": u, "utc": true or false}}` |
| `INT64` with `TIMESTAMP(unit, isAdjustedToUTC)`            | `{"timestamp": {"unit": u, "utc": true or false}}` |
| group with `LIST` (any conforming three-level or legacy two-level form), or a `repeated` field outside a `LIST` or `MAP` group | `{"list": {"element": T}}` |
| group with `MAP` or legacy `MAP_KEY_VALUE`                 | `{"map": {"key": K, "value": V}}` |
| group without annotation                                   | `{"struct": {"fields": [{"name": …, "type": T}, …]}}` |

Units `u` are `"ms"`, `"us"`, or `"ns"`. A present Parquet `LogicalType`
annotation is authoritative; use the legacy `ConvertedType` only when
`LogicalType` is absent. Legacy `TIME_MILLIS` and `TIME_MICROS` map to
`{"time": {"unit": "ms", "utc": true}}` and
`{"time": {"unit": "us", "utc": true}}`, respectively;
`TIMESTAMP_MICROS` is `{"timestamp": {"unit": "us", "utc": true}}`,
and `INT_16` is `"int16"`. A modern `TIME` with `isAdjustedToUTC: false`
remains `utc: false` even if it also carries a legacy time annotation.
For both time and timestamp columns, changing `unit` or `utc` changes the
logical type and is rejected by schema registration below.
`INT96`, `INTERVAL`, and any annotation not listed have no GRV type:
writers must not produce them (for example, configure Spark to write
`TIMESTAMP_MICROS`), and a file containing one cannot be registered.

The logical schema deliberately ignores repetition (`REQUIRED` vs
`OPTIONAL`: every GRV field is nullable to readers, and a writer may encode
any field either way), the names of list and map wrapper groups, field ids,
and file key-value metadata. Field names are compared exactly. Two logical
schemas are equal when they are equal JSON; schema *S* is a **prefix** of
*B* when *B*'s column list begins with *S*'s columns.

**Schema baseline.** Each table has a durable, table-wide baseline at
`<table>/.schema.json`, independent of version data and retained through
pruning:

```json
{
  "table": "orders",
  "mutation_id": "5d8e2b1a-0c3f-4a7e-9b6d-1e2f3a4b5c6d",
  "columns": [
    { "name": "order_id", "type": "string" },
    { "name": "amount", "type": { "decimal": { "precision": 18, "scale": 2 } } },
    { "name": "placed_at", "type": { "timestamp": { "unit": "us", "utc": true } } },
    { "name": "_region_", "type": "string" },
    { "name": "_year_", "type": "string" }
  ]
}
```

Before writing version data, a writer **registers** the logical schema of
its data files:

1. If the baseline is absent, `conditional-create` it with the proposed
   schema. An unsuccessful create proceeds with the existing baseline.
2. Read the baseline. The proposed schema must equal it or extend it by
   appending top-level columns; removing, renaming, re-typing (including
   any change inside a nested type), or reordering an existing column is
   rejected. An extension is installed by CAS with a fresh `mutation_id`;
   on conflict, reread and revalidate.
3. A successful create/CAS, or a read of an equal baseline, authorizes
   that version's schema. A later extension does not revoke this authority:
   the authorized schema remains a prefix of every later baseline.

A baseline column may also carry an `ext` object, keyed by extension id,
recording that extension's properties of the column — for example, that
its values are encrypted (see the companion crypto-shredding RFC). `ext` is
set only by the create or CAS that appends the column, never changes
afterwards, is preserved by every later CAS, and is ignored when schemas are
compared. Writers MUST honor the baseline's `ext` for every column they
write, whatever they proposed; a writer that cannot honor it rejects the
write. Declaring a column's properties in the same compare-and-swap that
adds the column means that no writer can ever see the column without them.

Incompatible concurrent extensions cannot both succeed, even in different
partitions or disjoint revisions. A registered extension is never rolled
back, including when its writer crashes before producing data; subsequent
writers must honor those reserved columns. A change incompatible with the
baseline, or a change to partition keys, requires a new table. Because a
registration cannot be undone, writers SHOULD register only schemas
declared in their code or configuration, never schemas inferred from input
data.

Every selected version's schema must be a prefix of the durable baseline,
and the publisher verifies this (§7). All versions of a table are therefore
prefix-compatible across its entire history. A revision's table schema is
the longest schema actually selected in that revision; consumers fill
missing trailing columns with nulls. Reselecting an existing older version
is legal. Writing a new version with a shorter schema than the baseline at
registration is not.

### 5. Version allocation: `.claim`

Committing a new version of a (table, partition) requires allocating the
next version number. Allocation is mutually excluded by a **claim** file in
the directory that contains the `version={n}` directories for that
(table, partition):

- non-partitioned table: `<table>/.claim`
- partition: `<table>/key1=v1/key2=v2/.claim`

One claim at a time per (table, partition); a claim covers exactly one new
version. A run writing several versions of the same (table, partition)
repeats the cycle below; applying the run publishes its highest-numbered
one (§8). Dataset-level operations use a lease embedded in `LATEST` (§8),
not a separate `.states/.claim`: their lease transitions must also fence
writes of the current-revision pointer.

`.claim` schema — the file's content changes as it moves through the
lifecycle below; **every transition is a `conditional-put` against the
validator of the claim content the writer last read or wrote**; the holder
recognizes its own claim by its `token`:

| field         | type    | present in         | notes                                     |
|---------------|---------|--------------------|-------------------------------------------|
| `holder`      | string  | all states         | the allocating run ULID (§6) |
| `token`       | string  | all states         | random identifier; identifies this claim |
| `version`     | integer | allocated, released| the version reserved by this holder, if any |
| `high_water`  | integer | all states         | greatest number ever reserved; initially 0, never decreases |
| `mutation_id` | string  | all states         | fresh random id for every write (§1) |
| `claimed_at`  | string  | acquired, allocated| RFC 3339 UTC, when the claim was taken     |
| `expires_at`  | string  | acquired, allocated| RFC 3339 UTC, after which the claim is stale; advanced by renewal |
| `released_at` | string  | released           | RFC 3339 UTC, when the holder finished     |
| `outcome` | string | released | `finalized` or `abandoned`; recovery must distinguish them |
| `last_release` | object | after a takeover of a release record | `{token, version, outcome}` of the release record it replaced, if that record had a `version` |

A claim is in one of three states:

| state     | recognized by                     | transitions |
|-----------|-----------------------------------|-------------|
| acquired  | no `version`, no `released_at`    | holder: renew, allocate, or release as `abandoned`; anyone, once expired: take over |
| allocated | `version`, no `released_at`       | holder: renew, or release as `finalized` or `abandoned`; run recovery, once expired: release on the run's behalf (§6); anyone, once expired: take over |
| released  | `released_at` and `outcome`       | anyone: take over (a new acquisition) |

Protocol:

1. **Acquire.** `conditional-create` `.claim` with `holder`, `token`,
   `claimed_at`, `expires_at`, `high_water: 0`, and `mutation_id`.
   If the create fails, `get` the existing claim: if it is unexpired and
   held by another, abort (or wait and
   retry, at the caller's discretion); if it is expired, or is a release
   record, take it over with a `conditional-put` against the validator just
   read, writing a new `holder`, `token`, timers, and `mutation_id`,
   removing `version`, `released_at`, and `outcome`, and preserving
   `high_water` unchanged. A takeover of a release record also records it
   as `last_release`, so the release stays provable through the next
   takeover; every other transition keeps `last_release` unchanged.
   Before taking over a release record, the acquirer SHOULD CAS the
   releasing holder's allocation record (§6) from `allocated` to the
   release's `outcome`, because `last_release` alone covers only the most
   recent release. Acquisition never erases allocation history.
2. **Allocate.** The holder lists the `version={n}/` prefixes (delimited
   listing), takes `n = max(highest listed, high_water) + 1`, and CAS-writes
   both `version: n` and `high_water: n` in the same transition. Every later
   transition preserves or increases `high_water`, including takeover,
   renewal, release, and abandonment before allocation. Thus any number
   reserved by a successful CAS stays reserved across arbitrarily many
   crashes, even if no version object was written. Then create the run's
   allocation record for (table, partition, `n`, token) (§6) before
   writing any version object.
3. **Write data.** Register the table schema (§4). Create each of
   `version={n}`'s data files via `conditional-create`, recording each
   returned validator. A create that finds another writer's content (§1)
   means another writer has objects at this number: abandon the version
   (step 7).
4. **Commit.** **Renew** the claim — a `conditional-put` that advances
   `expires_at` — and, only if it succeeds, create
   `version={n}/manifest.json` (§4) via `conditional-create`, with
   `claim_token` set to this claim's `token`, `data_files` recording each
   file's SHA-256, size and validator, and `derived_from` citing the run's
   inputs (§9).
5. **Confirm.** Verify the version (§4) and check that no `.pruned` is
   present in its directory. If this fails, the version was pruned or
   damaged mid-flight: abandon it (step 7).
6. **Release.** Rewrite `.claim` (`conditional-put`) into a release record:
   `holder`, `token`, `version`, `high_water`, `released_at`,
   `outcome: finalized`, and a fresh `mutation_id`. A version whose release
   succeeded is **finalized**; CAS its allocation record to `finalized`
   (§6). If the release CAS's outcome is unknown (§1), the version is
   finalized only if the reread claim, or its `last_release`, shows this
   token released as `finalized`; otherwise leave the record to
   recovery. The claim file is **never deleted**. Its high-water mark
   survives even when the next acquirer crashes before allocating.
7. **Abandon.** Release the claim with `outcome: abandoned`, preserving
   `high_water`, and CAS the allocation record, if one was created, to
   `abandoned`. Its objects are left for GC (§10). Recovery must not
   salvage an abandoned release record.

**Renewal and expiry.** A holder may renew at any time, and MUST renew at
intervals shorter than its TTL while it works; the TTL (the duration from a
successful acquisition or renewal to `expires_at`) must exceed the interval
between renewals plus `max_clock_skew`, and must not exceed `max_lease_ttl`
(§2). A claim is **expired** when `now ≥ expires_at`, or when an observer
has read the same validator twice at least `max_lease_ttl +
max_clock_skew` apart on its own clock: content never repeats (§1), so no
renewal happened in between. The second rule bounds the effect of a holder
that wrote an excessive `expires_at`. A failed renewal, release, or other claim CAS means
the claim was taken over: the holder is **stale** and MUST stop writing
objects for that version at once; the version is not finalized. The same
renewal and expiry rules apply to run leases (§6) and dataset leases (§8).

**Fence.** A version is **publishable** — may be referenced by a revision
(§7) — iff all of:

- its `manifest.json` exists and matches its path (`table`, `partition`,
  `version`);
- no `.pruned` exists in its directory;
- it verifies (§4);
- its run file (§6) exists, matches its sealed run control, and lists
  (table, partition, version) with the manifest's `claim_token`;
- each `derived_from` reference names one of its run's inputs, the run
  confirmed its holds, the reference resolves, and the cited hold is
  active (§9);
- its schema is a prefix of the table's durable baseline (§4).

The same rule applies to a version entering a revision for the first time
and to one reselected later (e.g. a rollback). A stale holder never gets
its version into a run file — its release fails — so its version can never
be published; and because every version object is created with
`conditional-create`, a stale holder cannot clobber another holder's
version either. The fence assumes conforming writers — every version write
follows the protocol above; a client that writes version objects without
holding the claim is out of contract, and store-level access control, not
the layout, is the mitigation.

A crashed holder's claim is recovered by step 1's takeover once it expires.

### 6. Runs

Adding new versions — for one table, a few tables, the partitions of one
table, or partitions across multiple tables of one dataset — is a **run**.
A run writes into exactly one dataset; a job writing to several datasets
performs one run per dataset. Each run has a unique `run_id` and keeps
three kinds of records:

```
<dataset>/.runs/<run-id>.control.json                     # mutable: run lease and phase
<dataset>/.runs/<run-id>.allocations/<claim-token>.json   # mutable: one per version allocation
<dataset>/.runs/<run-id>.json                             # immutable sealed run file
```

`run_id` is a **ULID** in canonical uppercase form, matching
`^[0-7][0-9A-HJKMNP-TV-Z]{25}$`. The leading character is restricted to
`0`–`7` so the 26-character encoding fits in 128 bits. Run ids are unique
across all datasets. ULIDs embed a 48-bit timestamp, so lexicographic order
follows timestamp order; no stronger cross-writer chronological guarantee
is made (same-timestamp ties and clock skew). Operation ids (§8) use the
same form.

Run file schema:

| field           | type    | required | notes                                    |
|-----------------|---------|----------|------------------------------------------|
| `run_id`        | string  | yes      | the run's ULID; the file name is `<run_id>.json` |
| `created_at`    | string  | yes      | RFC 3339 UTC, when the run started       |
| `base_revision` | integer | yes      | this dataset's `LATEST.revision` when the run started; `0` if none; the base for conflict detection (§8) |
| `inputs`        | array   | yes      | the source revisions the run reads, each `{dataset, revision, retention_id}`; fixed at the start of the run (§9, §11); may be empty |
| `holds_confirmed` | boolean | yes    | true once every input's dependency hold passed its checks (§9); a run cites inputs only if true |
| `sealed_at`     | string  | yes      | RFC 3339 UTC, when the run was sealed    |
| `entries`       | array   | yes      | one entry per finalized version          |
| `metadata`      | object  | no       | map of engine name → engine-specific fields (§11) |

Each entry: `table` (string), `partition` (object; `{}` for
non-partitioned), `version` (integer), `claim_token` (string; the token of
the claim that allocated and released it, §5).

```json
{
  "run_id": "01M3KQA080R6Y8C2D9F0G1H2J3",
  "created_at": "2026-09-28T10:00:00Z",
  "base_revision": 12,
  "inputs": [
    { "dataset": "raw_events", "revision": 7,
      "retention_id": "28f68c81-dbdc-4ab6-a811-81d7b989c693" },
    { "dataset": "ref_data", "revision": 2,
      "retention_id": "fb9b64a3-aeeb-47b4-b60c-4ad5c5e997d0" }
  ],
  "holds_confirmed": true,
  "sealed_at": "2026-09-28T10:20:00Z",
  "entries": [
    { "table": "customers", "partition": {}, "version": 2,
      "claim_token": "3b7d0c2e-1f4a-4e9b-8c6d-2a5e7f9b1c0d" },
    { "table": "orders", "partition": { "region": "eu", "year": "2025" }, "version": 3,
      "claim_token": "9f2c1e4a-7b3d-4c1e-9a02-6d5f8e1b2c3d" }
  ]
}
```

A run only *commits* version directories (via the claim cycle, §5).
Versions enter a dataset's state only when a revision (§7) references
them; the publish step (§8) is what advances `LATEST`. The lifecycle is
therefore run → publish step → `LATEST`, in that order.

**Run control.** `<run-id>.control.json` holds the run's lease and phase.
It carries the run file's `run_id`, `created_at`, `base_revision`,
`inputs`, and `metadata`, fixed at creation, plus:

| field                  | notes                                                    |
|------------------------|----------------------------------------------------------|
| `phase`                | `open`, `recovering`, or `sealed`                        |
| `owner_token`          | token of the current owner: the run's driver, or a recovery worker |
| `expires_at`           | lease expiry (§5 renewal rules); absent once sealed      |
| `mutation_id`          | fresh on every write (§1)                                |
| `holds_confirmed`      | `false` at creation; set to `true` by one CAS while `open`, once every input's hold is confirmed (§9) |
| `sealed_at`, `entries` | present iff sealed; exactly the run file's values        |

| from              | to           | by                                  | when |
|-------------------|--------------|-------------------------------------|------|
| —                 | `open`       | the driver                          | `conditional-create`, before any allocation |
| `open`            | `open`       | the owner                           | lease renewal |
| `open`            | `sealed`     | the owner                           | no allocation record is still `allocated` |
| `open`, expired   | `recovering` | any process                         | with a fresh `owner_token` and lease |
| `recovering`      | `recovering` | the owner; any process once expired | renewal; takeover |
| `recovering`      | `sealed`     | the owner                           | every allocation record is resolved (below) |

Every transition is a CAS, so the transitions race atomically and exactly
one sealed result exists. Sealing is irreversible. The **run file** is the
sealed control without `phase`, `owner_token`, and `mutation_id`. Any
process may materialize it with `conditional-create`; an existing file
must equal it as JSON, otherwise it is a protocol violation. The immutable
run file is the run's commit marker and is never modified. Publishers
require it to match a sealed run control.

**Allocation records.** `<run-id>.allocations/<claim-token>.json` records
one version allocation: `run_id`, `table`, `partition`, `version`,
`claim_token`, `state`, and `mutation_id`. A task creates it with
`conditional-create`, in state `allocated`, after its claim allocation CAS
and before writing any version object (§5 step 2). Its state then changes
by CAS to exactly one terminal value:

| state        | set by                                              | meaning |
|--------------|-----------------------------------------------------|---------|
| `allocated`  | the task                                            | reserved; version objects may exist |
| `finalized`  | the task after its claim release, or recovery after proof | the version is finalized (§5 step 6) |
| `abandoned`  | the task, or recovery                               | the claim was released as `abandoned` |
| `unproven`   | recovery                                            | finalization could not be proven |

Allocation records are separate objects so that parallel tasks never
contend on a shared journal. A record's first terminal CAS wins; a task and
a recovery worker racing on the same record reread and accept the winner.

**Lifecycle.**

1. **Start.** Read this dataset's `LATEST.revision` as `base_revision`,
   choose the source revisions to read as inputs (normally each source's
   current `LATEST.revision`), generate a `retention_id` per input, and
   create the run control (`open`). Then place each input's dependency hold
   and check it (§9), and set `holds_confirmed` by CAS. A run that cannot
   confirm every hold seals without entries and is retried as a new run
   from fresh inputs. No version is allocated before `holds_confirmed`.
2. **Work.** Tasks run the claim cycle (§5) for each version. The owner
   renews the run lease between partition writes as well as during them. A
   task checks that the run control is `open` under its owner's token before
   each allocation; once the owner's lease CAS fails, or the control is no
   longer `open` under that token, the owner and its tasks start no new
   allocations.
3. **Seal.** Once its tasks have finished, the owner lists the run's
   allocation records. A record left `allocated` by a crashed task is
   resolved as recovery does (below). When no record is still
   `allocated`, the owner CAS-seals the control, writing `sealed_at` (its
   current time) and `entries` (the `finalized` records' table, partition,
   version, and claim token), and then materializes the run file.

The sealed `entries` are authoritative. A record created or finalized after
the listing used for sealing is not an entry, and neither is any
allocation by a stale owner or task. Such versions are **orphaned** and
never publishable, as is every version whose run has no run file.

**Recovering a crashed run.** Partition claims being released or absent is
not evidence that a run has finished. Recovery first reads the run control:

- If it is `open` with an unexpired lease, recovery must wait, including
  when there are currently no outstanding partition claims.
- If `open` is expired, recovery CAS-transitions it to `recovering`, with
  a fresh owner token and lease. This transition races atomically with the
  owner's renewals and sealing.
- If `recovering` expires, another recovery worker may take over by CAS.
- If `sealed`, recovery only materializes the run file.

In `recovering`, recovery lists the allocation records and resolves each
one still `allocated`:

- A matching claim release record (same token and version), or a claim
  whose `last_release` matches, proves the outcome: CAS the record to
  `finalized` or `abandoned` to match it. A task whose release CAS had an
  unknown outcome (§1) uses the same proof.
- A matching claim still `allocated` must be allowed to finish or expire;
  recovery cannot assume that taking over the run stopped an in-flight
  partition write. Once the claim has expired, recovery verifies the
  version (§5 step 5) and CAS-releases the claim on the run's behalf with
  `outcome: finalized`, or `abandoned` if it does not verify, then CASes
  the record to match.
- Otherwise — for example, the claim has since been taken over by another
  run — the record becomes `unproven`.

Recovery then CAS-seals the control with the `finalized` records as
`entries` and materializes the run file. A losing owner or recovery worker
can never create a different run file, because only the winning sealed
payload is authorized. Any process, including GC (§10), may recover an
expired run.

### 7. Revisions

The state of a dataset is described by a **revision**. A revision is a
**complete snapshot** of the dataset's state: one entry per (table,
partition) that is *in the state*, each with the current version of that
(table, partition) and the `run_id` that produced it. A state may contain
any subset of the dataset's tables, including none.

Membership is per-revision: a (table, partition) **absent from a revision
is not in that revision's state**. Removing a table or partition from a
state is therefore simply omitting it from the next revision. The table
itself is never deleted by this: its folder, layout, and version data
remain, and a later revision may include it again. A partition whose data
is terminated instead of omitted carries an **empty version** in the state
(§4) — present, but explicitly carrying no rows.

Revisions are chained for history: each revision records its **previous
revision** number, the state it was derived from (`previous_revision = 0`
for the first revision). Resolution is direct: the state at revision `n`
is revision `n`'s own entries.

All historical revisions are stored as parquet:

```
<dataset>/.states/revisions/revision={n}/data.parquet
```

Row schema — one row per (table, partition) in the state:

| column              | type   | notes                                                    |
|---------------------|--------|----------------------------------------------------------|
| `table`             | string | table name                                               |
| `partition`         | string | canonical path form, e.g. `key1=v1/key2=v2`; the empty string (not null) for non-partitioned |
| `version`           | int64  | version of this (table, partition)                      |
| `run_id`            | string | run that produced the (table, partition, version)       |

Revision-level fields are stored in the parquet file's key-value metadata,
so they survive a zero-row revision (an empty state):

| key                      | value                                                |
|--------------------------|------------------------------------------------------|
| `grv.revision`           | this revision's number, in decimal                   |
| `grv.previous_revision`  | the revision this one was derived from; `0` for the first |
| `grv.created_at`         | RFC 3339 UTC time the revision was written           |
| `grv.operation_id`       | the publish operation (§8) that describes this revision |

**Validity** is checked by the publisher when the revision is written
(§8). A revision is valid iff (a) each (table, partition) appears at most
once; (b) the four metadata keys are present and `grv.revision` equals
the path's `n`; (c) every referenced version is publishable (§5); (d) each
referenced table has a `.layout.json`, and each `partition` string is the
canonical path form for that table's `partition_keys`; (e) each row's
`run_id` equals the referenced manifest's `run_id`; (f) the versions of
each table are prefixes of that table's durable schema baseline (§4).

**Inherited validity.** Every fence condition (§5) is monotone for a
version that stays in the current state: manifests, data, and run files are
immutable, the schema baseline only grows, versions of the current state
cannot be pruned (§10), and a hold cited by a publishable version cannot be
released (§9). An entry carried unchanged from the current `LATEST` into
its successor therefore remains publishable, and the publisher validates
only entries that differ from the predecessor (§8).

**Resolvability** is a separate, time-dependent property: a revision is
resolvable while every version it references is still present and
verifies. Validity never changes after the write; resolvability is lost
when GC prunes a version (§10). A valid but unresolvable revision is not a
protocol violation — it is the expected result of pruning.

**Committed revisions.** A revision is **committed** if it is
`LATEST.revision` or lies on the `previous_revision` chain from it; any
other revision is an **orphan** left by a failed publication (§8). A
`.superseded.json` receipt is written only for a revision observed on the
chain (§8), so a receipt proves its revision committed with a single
`head`; hold checks (§9) and pins (§10) use this shortcut and fall back to
walking the chain. A missing receipt proves nothing, because a receipt may
not have been written yet; GC therefore walks the chain (§10).

The `revision={n}` path segment deliberately reuses the same `key=value`
path convention as partitions and versions. Like version numbers, revision
numbers use canonical decimal without leading zeros and satisfy
`1 ≤ n ≤ 2^63−1`; `0` is only the no-predecessor sentinel. Revision numbers
increase along the chain but need not be contiguous: numbers reserved by
failed publications are skipped. Version and revision allocation MUST fail
on exhaustion rather than wrap or reuse a number.

### 8. Publish step and `LATEST`

A dataset's **publish step** applies a change set to its current state,
writes a revision, and advances the state. The dataset lease and current
revision share one CAS object, `<dataset>/.states/LATEST`. There is no
separate dataset claim. `LATEST` is JSON with these required fields:

| field | meaning |
|-------|---------|
| `revision` | current revision number; `0` means no revision published yet |
| `high_water` | greatest revision number ever reserved; initially 0, never decreases |
| `mutation_id` | fresh random id on every mutation, even if `revision` is unchanged |
| `lease` | null, or `{holder, token, claimed_at, expires_at}`; `holder` identifies the publisher, GC, or other tool instance |
| `pending` | null, or the dataset-relative path of a committed operation description, e.g. `.states/operations/<operation-id>.json` |

```json
{
  "revision": 12,
  "high_water": 13,
  "mutation_id": "c1d2e3f4-a5b6-4c7d-8e9f-0a1b2c3d4e5f",
  "lease": {
    "holder": "grv-publisher/0.3.0@worker-7",
    "token": "7a6b5c4d-3e2f-4a1b-9c8d-7e6f5a4b3c2d",
    "claimed_at": "2026-09-28T10:21:00Z",
    "expires_at": "2026-09-28T10:26:00Z"
  },
  "pending": null
}
```

The first caller initializes this object with `conditional-create`
(`revision: 0`, `high_water: 0`, `lease: null`, `pending: null`), then
acquires its lease by CAS. Acquisition, takeover, renewal, reservation,
commit, and release all update this same object, preserving unrelated
fields; consecutive transitions by the same holder may be combined into one
CAS. Acquiring a released lease sets a new token; taking over an active
lease is allowed only once it has expired (§5). A holder uses its latest
successful CAS validator; after a CAS failure it must reread and check that
it still owns the token before doing anything further. Lease expiry permits
takeover; the successful takeover CAS is the fence. Every claimant,
including GC, changes the validator even when it leaves `revision`
unchanged. Lease durations, renewal, and expiry follow §5.

**Durable operations.** A lease alone cannot fence a delayed write or delete
to a different object. Retention effects — pins, hold releases, retirement,
and tombstones — therefore require a committed **operation**, described by
an immutable record at
`.states/operations/<operation-id>.json` with the fields `operation_id`
(ULID), `dataset`, `kind`, `created_at`, `created_by` (tool and version),
and `payload`:

```json
{
  "operation_id": "01M8QEDC00S9T0V1W2X3Y4Z5A6",
  "dataset": "orders_product",
  "kind": "prune",
  "created_at": "2026-12-01T00:00:00Z",
  "created_by": "grv-gc/1.2.0",
  "payload": {
    "targets": [
      { "table": "orders", "partition": { "region": "eu", "year": "2025" }, "version": 3 }
    ]
  }
}
```

The payload holds every fact needed to replay the operation's effects
without the original process:

| `kind`         | `payload`                                              | effects (immutable markers) |
|----------------|--------------------------------------------------------|-----------------------------|
| `release_hold` | `releases`: array of `{consumer_dataset, revision, retention_id}` | each `.states/released-holds/<consumer_dataset>/revision={revision}/<retention_id>.json` (§9) |
| `pin`          | `pin_id`, `scope`, optional `reason`                   | the scope's `.pins/<pin_id>.json` (§10) |
| `unpin`        | `pin_id`, `scope`, optional `reason`                   | the scope's `.pins/<pin_id>.released.json` (§10) |
| `retire`       | optional `reason`                                      | `<dataset>/.retired` (§10) |
| `prune_intent` | `targets`: array of `{table, partition, version}`      | none: a proposal that makes the targets visible to hold checks (§9, §10) |
| `prune`        | `targets`: array of `{table, partition, version}`      | each target's `.pruned` (§10) |

A publish step also writes a `publish` description, with payload
`revision`, `previous_revision`, and `change_set` (below), as its audit
record. It is committed by the revision CAS itself (publish step 5), not
through `pending`, and has no effects of its own.

A pin `scope` is `{revision}` for a revision pin, or `{table}`,
`{table, partition}`, or `{table, partition, version}`. Every marker body
records the `operation_id` that created it, plus the fields given in
§8–§10. Timestamps in marker bodies (`created_at`, `released_at`,
`retired_at`, `pruned_at`, `observed_at`) are sampled when the marker is
written, and `*_by` fields copy the description's `created_by`. The
description's `created_at` is an audit timestamp, never a supersession or
grace timestamp.

1. Under the lease, validate the intended operation and create its
   description with a new operation id.
2. CAS `LATEST.pending` from null to that description's path — for a
   prune decision, from its intent (§10) — retaining the lease. This CAS
   commits the decision. A description not referenced by a successful
   commit is an orphan and authorizes no effects.
3. Materialize the operation's effects with `conditional-create`. An
   existing marker must be confirmed by a successful `get` or `head`,
   including the durability barrier in §1. Clear `pending` only after
   every marker is durable, and only as the lease holder, by a CAS on a
   read that shows `pending` naming this operation. Do not start or commit
   another operation while `pending` is non-null.

**Unknown outcomes.** When the committing CAS of an operation has an
unknown outcome (§1), the caller reacquires the lease — a successful CAS,
which also fences out any delayed request — and completes any pending
operation. The operation committed iff a marker recording its
`operation_id` now exists; a publication is resolved by step 5 below. A
caller that retries a pin whose outcome it resolved as not committed reuses
its `pin_id`.

On takeover, the new holder MUST complete any pending operation before
validating new work; a pending `prune_intent` authorizes nothing and is
simply cleared. Committed decisions are irrevocable. Replay accepts an
existing durable marker iff it records the same fact — the same hold path for a
hold release; the same `pin_id` and `scope` for a pin; the same `pin_id`
for a pin release — ignoring timestamps and `*_by` fields; any existing
`.pruned` or `.retired` is accepted. A helper
that has lost its lease may still create an already committed operation's
markers, but may neither clear `pending` nor commit a new operation.
Operations and their markers are retained.

**Change sets.** A publish step applies a **change set** to the predecessor
state:

- `runs` — sealed runs of this dataset. For every (table, partition) among
  its entries, a run contributes its highest-numbered entry.
- `omissions` — (table, partition) pairs, or whole tables, to remove from
  the state.
- `selections` — explicit (table, partition) → version assignments, for
  rollback and repair.

Each (table, partition) may be changed by at most one element of a change
set. A change set may also carry `expected_revision`, which makes the step
fail unless the predecessor is that revision, and a `reason`. The change
set is recorded in the publish description:

```json
"change_set": {
  "runs": ["01M3KQA080R6Y8C2D9F0G1H2J3"],
  "omissions": [
    { "table": "orders", "partition": { "region": "us", "year": "2024" } },
    { "table": "legacy_totals" }
  ],
  "selections": [
    { "table": "fx_rates", "partition": {}, "version": 4 }
  ],
  "expected_revision": 12,
  "reason": "nightly build; drop legacy_totals; roll back fx_rates"
}
```

An omission without `partition` removes the whole table; omitting
something absent from the predecessor is a no-op. `runs`,
`omissions`, and `selections` default to empty arrays;
`expected_revision` and `reason` are optional.

**Conflicts.** A run's output was computed against its `base_revision`
(§6). The base must be the predecessor or an ancestor on its committed
`previous_revision` chain; `0` denotes the empty initial state. Otherwise
the step fails. For each run, the publisher MUST check every committed
transition after its base through the predecessor, comparing each
revision's state with its immediate predecessor's state:

- For every (table, partition) the run contributes, the selected version
  must stay unchanged throughout these transitions; absence is a distinct
  value, so adding or omitting the pair counts as a change.
- Every table present in the base state that the run contributes to must
  stay present in every intervening state, even when the contributed
  partition was absent from the base.

A violation fails the step with a **conflict** and changes nothing.
Comparing only the base and predecessor is insufficient: version
`1 → 2 → 1`, partition `absent → present → absent`, and a table removed
then restored all still conflict. Orphan revisions are ignored; committed
revision parquets are retained (§10), so these checks do not require the
historical version data to remain available. Publications that leave the
affected versions and table membership unchanged do not conflict.

The first run to publish a change to a (table, partition) therefore wins;
the loser is rebuilt as a new run from a newer base, or its version is
assigned by an explicit selection. Omissions and selections are explicit
decisions and are not checked against run bases; `expected_revision`
guards them against concurrent publications. Their committed changes
still count when checking other runs' bases, even if later reversed.

The publish step is:

1. **Acquire** the lease in `LATEST`, complete any pending operation, and
   reject a retired dataset. Read its `revision` as the predecessor `p`
   (fail if the change set's `expected_revision` differs), and reserve
   `n = max(high_water, revision, highest existing revision number) + 1`
   by storing `high_water: n` — in the acquiring CAS itself when nothing is
   pending. Every transition, including takeover before allocation,
   preserves this durable reservation (§5).
2. **Compute** the next state from `p`'s state (empty if `p = 0`) by
   applying the change set and checking conflicts.
3. **Validate** every entry whose version differs from `p`'s entry for the
   same (table, partition) — new and reselected versions — as §7 requires.
   Entries carried unchanged are valid by inheritance (§7); a publisher MAY
   revalidate them.
4. **Write** the publish description `o` and then
   `revision={n}/data.parquet`, both with `conditional-create`, setting
   `grv.previous_revision` to `p` and `grv.operation_id` to `o`. A
   collision is an error; never overwrite or reuse the number.
5. **Commit** with a single CAS that sets `revision` to `n` and releases
   the lease, using the latest validator belonging to this lease. A
   takeover by GC or any other holder makes this CAS fail, even if no other
   revision was published. On failure, abort and recompute under a new
   acquisition; do not retry the old revision against a fresh validator. If
   the outcome is unknown (§1), reacquire the lease and complete any
   pending operation; the publication succeeded iff `n` is then on the
   chain from `LATEST.revision` (§7).
6. **Record supersession.** If `p ≠ 0`, after observing the commit, create
   `p`'s `.superseded.json` with
   `{operation_id: o, successor: n, observed_at: <current UTC time>}`. This
   needs no lease. If the publisher fails first, the next process to find
   `p` superseded without a receipt writes one; GC always does before
   evaluating `p` (§10). An existing receipt with the same `successor` is
   accepted. For the first revision there is no receipt to write.

**Renewal.** The commit is fenced by compare-and-swap, not by time: if the
lease expired and another holder took over, step 5 fails safely, and if
nobody took over, it succeeds safely. Renewal therefore only protects the
work in progress from takeover. The publisher renews only when the lease
could expire before step 5 — when `expires_at − now` is less than the
expected remaining time plus `max_clock_skew`. A publish that finishes
within its TTL writes `LATEST` exactly twice: in steps 1 and 5.

`observed_at` is always sampled after observing that `p` has been
superseded, never while preparing the revision, so it is never earlier than
the real supersession; a late receipt only extends retention. GC uses the
receipt, plus the clock-skew margin (§10), never the successor parquet's
`grv.created_at`.

Readers read `LATEST.revision` → the revision parquet → manifests → data.
They ignore `lease` and `pending`; `revision = 0` means an empty initial
state. An absent `LATEST` also means an empty state, but only while the
dataset has no revision objects; otherwise it is a protocol violation.
Data and revision objects exist before publication, and a missing
supersession receipt does not delay visibility. A failed publication may
leave an orphan revision, which is legal and does not start a grace period.
Every published revision has the revision it replaced as its predecessor.

A missing/invalid revision named by `LATEST` is a protocol violation.
Readers report the dataset as unavailable; no automatic fallback is defined.
Recovery must preserve allocation high-water marks and operation decisions;
restoring only an old copy of `LATEST` would erase fences and is forbidden.
A valid revision made unresolvable by permitted pruning, for example after
retirement, is instead reported as containing unavailable versions (§10).

### 9. Cross-dataset derivation

A version of a (table, partition) in one dataset may be **derived** from
specific revisions of other datasets (e.g. a table that is a function of
tables in upstream datasets as of particular revisions of each). The
derivation is recorded in the `derived_from` array of the new version's
`manifest.json` (§4), at the granularity the producer knows: a whole source
revision, one of its tables, or one partition. This makes the provenance
chain auditable from the data directory alone.

A run never lists its own dataset as an input. Its own prior state is
recorded as its `base_revision` (§6), which gives provenance without
retention: holding its own revisions would keep a dataset's entire history,
because each revision's versions would hold their predecessor. For the same
reason the **dataset derivation graph** — an edge from each run's dataset
to each of its inputs' datasets — MUST be acyclic: in a cycle such as A →
B → A, every revision of A holds a revision of B that holds an earlier
revision of A, so neither history can ever be pruned. GRV does not detect
cycles; deployments SHOULD enforce the graph in configuration, for example
by assigning datasets to layers and accepting inputs only from lower
layers.

**Inputs and holds.** Derivation is declared per run. At start, a run fixes
its **inputs** — one `{dataset, revision, retention_id}` per source
revision it reads — in its run control (§6), and places a **dependency
hold** on each before allocating any version. A hold protects the entire
source revision, because retention is revision-wide (§10). Placing a hold
never takes the source's lease or writes its `LATEST`: the consumer creates
one record in the source and otherwise only reads it. For each input:

1. Create the hold record
   `<source>/.holds/<consumer-dataset>/revision={n}/<retention_id>.json`
   with `conditional-create`.
2. Read the source's `LATEST`. If `pending` names a `prune_intent` or
   `prune` operation (§10) with a target that revision `n` references, the
   hold has failed.
3. Check that revision `n` is **committed** (§7) and **complete**: every
   version it references is present, unpruned, and verifies. The check is
   immediate when `n` is the `LATEST.revision` read in step 2 of a
   non-retired dataset, or has an active revision pin, because such a
   revision has been protected continuously since it was last verified
   complete. Otherwise check every entry.

When every input's hold has passed both checks, the run sets
`holds_confirmed` in its run control by CAS (§6). A run that cannot confirm
every hold seals without entries, and a new run starts from available
inputs.

A hold record contains `retention_id`, `dataset` and `revision` (the
source), `target_dataset` (equal to the `<consumer-dataset>` folder),
`target_run_id`, and `created_at`:

```json
{
  "retention_id": "28f68c81-dbdc-4ab6-a811-81d7b989c693",
  "dataset": "raw_events",
  "revision": 7,
  "target_dataset": "orders_product",
  "target_run_id": "01M3KQA080R6Y8C2D9F0G1H2J3",
  "created_at": "2026-09-28T10:00:05Z"
}
```

**Why this is safe.** The source's GC first commits a `prune_intent`,
then re-lists holds, and only then commits the final `prune` decision,
without every target that a revision with an unreleased hold references
(§10). Consider when the consumer reads `LATEST` in step 2:

- If the intent was committed before that read and its operation is still
  pending, the consumer sees the intent or the decision, and fails if a
  target touches revision `n`. If the decision has already completed, its
  tombstones are durable and the step 3 check sees them — or `n` is the
  current `LATEST`, whose versions no prune can target.
- If the intent is committed after that read, it is committed after the
  hold record was created, so GC's re-list sees the hold and the decision
  leaves revision `n`'s versions out.

Either way, a confirmed hold's revision is complete, and stays complete
until the hold is released. A consumer needs read access to the source and
create access to `<source>/.holds/<consumer-dataset>/` only. Provided store
permissions confine each consumer to its own subfolder, a consumer cannot
publish to, lock, or block the source, nor affect another consumer's holds;
at worst it makes the source retain data longer.

A crashed GC can leave a `prune_intent` pending until the next lease holder
of the source — a publisher or GC — clears it. Until then, holds on the
revisions it touches cannot be confirmed; the consumer retries later or
reads a newer revision. An intent never targets the versions of a
non-retired dataset's current `LATEST`, so runs reading the current
revision are never blocked. A consumer must not ignore an intent whose
lease has expired: it never writes `LATEST`, so nothing would stop the
stale GC from committing its decision.

**Fence.** Each `derived_from` element of a version names one of its run's
inputs — the same dataset, revision, and `retention_id` — and
**resolves**: its `table`, if present, is in that source revision's state,
and so is its `partition`, if present. The publisher checks that the run's
`holds_confirmed` is true, and that each cited hold record exists at
`<source>/.holds/<this dataset>/revision={n}/<retention_id>.json`, that its
body matches that path and names the version's run as `target_run_id`, and
that it has no release marker (§5). It reads them without the source's
lease: a hold cannot be released while a version that cites it is
publishable (below).

**Release.** A hold is **active** while its record exists and its release
marker — the same relative path under `.states/released-holds/`, i.e.
`<source>/.states/released-holds/<consumer-dataset>/revision={n}/<retention_id>.json`
— does not. Keying the marker by the full path means that releasing one
consumer's record can never release another's, even if both reuse a
`retention_id`. A hold becomes **releasable** when its target run can no
longer produce a publishable version that cites it:

- the target run control does not exist — a conforming run creates its
  control before any hold, so such a record is not a conforming hold; or
- the target run control is sealed, and either its `holds_confirmed` is
  false, or every entry of the sealed run is tombstoned (`.pruned`, §10) or
  has a manifest whose `derived_from` does not cite this `retention_id`.

These conditions are irreversible, so checking them needs no target lease.
Because GC deletes a manifest only after tombstoning its version (§10), the
check never depends on a manifest GC has already deleted. Allocations that
are not entries of the sealed run are never publishable and do not delay
release. Only the source dataset's GC releases holds, with a
`release_hold` operation under its own lease (§8, §10); each release marker
contains `retention_id`, `operation_id`, and `released_at`. Consumers cannot
write release markers. Holds are not released because a lease expired, a
revision was superseded, or a grace period elapsed. A target run that never
seals keeps its holds until the consumer's own recovery seals it (§6); the
source's GC, which may have no write access to the consumer's dataset,
reports such holds rather than recovering the run. A hold record that cannot
be read, or whose body does not match its path, is a protocol error, and GC
keeps the revision it names. Hold ids are never reused.

Derivation links point to revisions committed before the run started, and
holds keep those revisions complete until every version that cites them is
tombstoned.

### 10. Garbage collection: pins and `.pruned`

The layout assumes a GC process that prunes old versions. Protection is
declarative, via records whose *presence* is the signal:

- **Pins**, each a `<pin-id>.json` under a `.pins/` directory, created and
  released only by pin and unpin operations (below):
  - `<table>/.../version={n}/.pins/` — this version must not be pruned.
  - `<table>/<partition>/.pins/` — **no version** of this (table,
    partition) may be pruned.
  - `<table>/.pins/` — **no version** of this table, in any partition, may
    be pruned. (For a non-partitioned table this coincides with the
    previous rule.)
  - `<dataset>/.states/revisions/revision={n}/.pins/` — this revision is
    **pinned** (see the kept revisions below).
- **Dependency holds** (§9), under the dataset's `.holds/`, released by
  markers under `.states/released-holds/`.
- `<dataset>/.retired` — the dataset is **retired**: its `LATEST` is no
  longer unconditionally kept (see the kept revisions below).

A pin or hold is **active** while its record exists and its release
marker does not. A pin record contains `pin_id`, `operation_id`,
`scope` (as in the operation, §8), `created_at`, `created_by`, and an
optional `reason`; a pin release marker contains `pin_id`, `operation_id`,
and `released_at`.

Pin identity is `(dataset, scope, pin_id)`, matching the addressed `.pins/`
directory and operation description. IDs need only be unique within that
scope; no dataset-wide index or scan is required. Reusing an ID in another
scope denotes an independent pin, never a retry or release of the first.

GC reads `pending_grace` and `max_clock_skew` from `grv.json` after
acquiring the dataset's lease (§2).
`pending_grace` is chosen to exceed the expected worst-case time from a
run's seal to its publication, and the longest read of a just-superseded
revision that must succeed. Exceeding it can make a publication fail
validation, so its run must be redone, or a read report unavailable
versions; it never permits an invalid publication.

Retention deadlines include the configured maximum relative clock skew
between participants. A timestamp `t` is not considered expired until
`now >= t + pending_grace + max_clock_skew`. Grace for a pending version
starts at its run's `sealed_at`; grace for a superseded revision starts at
its receipt's `observed_at`, which is sampled only after publication (§8).

A version is **pending** if no committed revision (§7) references it and
its number is greater than every version of its (table, partition) that
any committed revision references — i.e. it is newer than anything ever
published for that (table, partition). Orphan revisions are ignored, so a
failed publication never removes a version's protection. Versions being
written, and finalized versions awaiting their publish step, are pending
unless a newer version of their (table, partition) has been published, in
which case their own publication would conflict anyway (§8).

**Kept revisions and protected versions.** GC evaluates one dataset at a
time, under that dataset's lease and after completing any pending
operation. It determines the committed revisions by walking the
`previous_revision` chain from `LATEST.revision`. A superseded revision on
the chain without a receipt — its publisher failed after committing (§8) —
first gets one, with `observed_at` set to GC's current time. A committed
revision is **kept** if any of:

- it is `LATEST.revision`, unless the dataset is retired;
- it is superseded and its `.superseded.json` has not expired under the
  deadline rule above; the successor's creation time is irrelevant;
- it has an active pin;
- it has an active hold (§9), confirmed or not.

A version is **protected** if any of:

- it, its (table, partition), or its table has an active pin;
- it appears in the state of a kept revision of its dataset;
- it is pending, and either it has no `manifest.json` and its (table,
  partition) `.claim` is still in the allocated state with this `version`, or
  its run (the manifest's `run_id`) is not sealed, or it is an entry of its
  sealed run and the run's `sealed_at` has not expired under the deadline rule
  above.

Every other version is unprotected and may be pruned. In particular, a
pending version that its sealed run does not list is an orphan and never
publishable. GC may first recover an expired run (§6), so that a crashed
run does not protect its versions indefinitely.

A version directory without a manifest is protected only while its claim
still allocates it, because only that claim's holder can still commit and
finalize it. Once the claim has left that state — released, abandoned, or
taken over — it can never return to it: `high_water` is at least `n`, so no
later allocation reuses `n`, and the old token's release CAS can never
succeed again (§1, §5). That condition is therefore stable without the
claim's lease. A late writer that still creates the manifest finds the
tombstone at its confirm step (§5 step 5), and its version is unreadable. If
the claim is allocated but expired, GC may recover the claim's run (its
`holder`) first.

Cross-dataset protection comes only from holds. Every `derived_from`
reference of a publishable version names an active hold, and a hold stays
active until every entry that cites it is tombstoned (§9). Pinning a
product revision therefore keeps the source revisions its versions were
derived from — transitively, through intermediate datasets — without GC
walking other datasets' derivation links. Missing or unreadable retention
records — the revision named by `LATEST`, a pin, a hold, a run control, or a
claim needed for a decision — are protocol errors: GC aborts the affected
deletions rather than guessing.

**Incremental evaluation (non-normative).** A committed revision stays
committed, its parquet is immutable, and the chain grows only at its head. A
GC or publisher may therefore cache facts derived from committed revisions
outside the layout, keyed by (dataset, revision): for example, per (table,
partition), the greatest version that any committed revision up to that one
references, which is what the pending test needs. A later pass reads only the
revisions between the current `LATEST.revision` and the cached head. Kept
status, pins, holds, and grace deadlines change over time and are always
re-evaluated, never cached. A cache must yield exactly what a full walk would
compute; on any doubt, such as an unreadable cache or a head not found on the
chain, fall back to the full walk.

Holds are deliberately coarse: a held revision protects *its entire state*
— every (table, partition, version) it names — even though a downstream
version may have used only a few of them. Provenance dependencies are
revision-wide, not selector-narrow.

**Retirement.** A dataset that is fully superseded — no longer a product,
and no longer needed by any kept state — may be marked
`<dataset>/.retired` by a committed retirement operation under its dataset
lease (§8); the marker records `operation_id`, `retired_at`, and an
optional `reason`. Raw marker writes are not conforming. While the marker
is present, the dataset's `LATEST` is *not* kept: the dataset is retained
only to the extent that its pins and holds still protect it, and
may otherwise lose all version data while retaining revision and
coordination records. Its `LATEST` then remains valid but may become
unresolvable (§7). Retirement is terminal: no new runs or publish steps are
made against a retired dataset. Pins, unpins, and holds on its revisions
remain possible.

**Pinning.** Pins MUST be created by a pin operation under the owning
dataset's `LATEST` lease (§8). Direct record creation followed by a
`.pruned` check is not a valid pin protocol.

1. Acquire the lease and finish any pending operation. A pending prune
   decision must become durable tombstones before pin validation.
2. For a version pin, verify the version is publishable (§5); for a
   revision pin, verify the revision is committed (§7; the receipt test
   suffices) and all versions it references are available, with no
   `.pruned` tombstones. A missing or pruned version makes the pin fail.
   Table/partition pins protect all currently unpruned versions and future
   versions; they do not resurrect earlier tombstones, which the result
   must report as excluded.
3. Generate a `pin_id` (or reuse it on a retry, §8), commit the pin
   operation through `LATEST.pending`, materialize `.pins/<pin-id>.json`,
   and complete the operation. Report
   success only after the commit is known and its record is durable. A
   crashed caller's committed pin is completed on takeover before GC can
   proceed.

**Unpinning.** An unpin operation, committed under the same lease,
releases one active pin by creating `.pins/<pin-id>.released.json`.
Release is terminal for that `(scope, pin_id)`; pinning the same scope again
creates a new pin. What the pin protected becomes prunable at the next GC
pass unless something else protects it. Deleting pin records by hand is
not an unpin.

Pin, unpin, and prune commits compete on the same CAS record. If a pin
wins, GC sees its protection; if a prune wins, a version or revision pin of
a target fails. Checking records just before deletion is insufficient.

**GC coordination and pruning.** A GC pass acquires the dataset's lease in
`LATEST`, completes pending operations, and writes any missing receipts. It
then releases releasable holds (§9) with a `release_hold` operation, and
computes the dataset's protected versions. Publications, pins, unpins, and
retirement for this dataset use this same lease and CAS object, so their
protection is read after acquisition; an earlier candidate listing is only
a hint. Holds are the one protection created without the lease, which the
intent-then-decision steps below account for. New versions may appear
concurrently; GC commits only an explicit set of observed version
directories after checking each one's protection. A reserved number without
objects needs no action.

For each batch of unprotected versions:

1. **Intent.** Write a `prune_intent` description with the candidate
   (table, partition, version) targets, and CAS `LATEST.pending` to it while
   owning the lease. The intent makes the candidates visible to hold checks
   (§9 step 2) and authorizes nothing.
2. **Re-list holds.** List the dataset's `.holds/` again and drop every
   candidate that a committed revision with an unreleased hold references.
3. **Decision.** Write a `prune` description with the remaining targets —
   always a subset of the intent's — and CAS `LATEST.pending` from the
   intent to it. This is the
   **irrevocable prune decision**. A failed CAS authorizes no tombstones or
   deletes; reread and recompute under a new lease.
4. **Tombstones.** Create every decided target's `.pruned` via
   `conditional-create`. A successor holder must finish these markers
   before allowing any new pin or publication. A tombstoned target can
   never become publishable again.
5. **Clear and delete.** Once all tombstones are durable, clear `pending`
   by CAS. Delete only tombstoned targets' data files and then their
   manifests; these physical deletions can finish outside the lease. They
   are the only objects GC deletes: every other object — markers, pins,
   holds, claims, schema baselines, layouts, `LATEST`, `grv.json`, and run,
   allocation, operation, and revision records — is retained.

This ordering also fences a stale GC worker: it may finish deletions only
for an already decided, permanently tombstoned target, which a later
publisher or pin cannot revive. A crash before step 3 leaves at most an
intent, which the next holder clears. A crash afterwards leaves replayable
work with a fixed target set. A delayed
version writer may create residual objects beneath a tombstone; they remain
unreadable and can be deleted by another sweep. No number is ever reused.

A `.pruned` body records `operation_id`, `pruned_by`, `pruned_at`,
`table`, `partition`, and `version`:

```json
{
  "operation_id": "01M8QEDC00S9T0V1W2X3Y4Z5A6",
  "pruned_by": "grv-gc/1.2.0",
  "pruned_at": "2026-12-01T00:00:00Z",
  "table": "orders",
  "partition": { "region": "eu", "year": "2025" },
  "version": 3
}
```

A version directory containing `.pruned` is a tombstone: readers report the
version as unavailable regardless of residual data. Once the tombstone is
durable, holds that the version cited may become releasable; the source
datasets' GC releases them (§9).

**Reader semantics.** Reads are best-effort and point-in-time. A reader
resolves a state from the `LATEST` (or a named revision) it read; no pin
or lease is taken, and a concurrent GC pass may prune a version while the
read is in flight. A reader that encounters a `.pruned` tombstone, or a
missing object, reports that version as unavailable; the read may be
retried against the then-current state.

**Final-product retention.** The intended workflow for data products:

1. Identify the datasets that are *final* products.
2. Pin the revisions of those datasets that back the product states that
   must remain resolvable.
3. The holds cited by those states' versions keep the upstream revisions
   the states were derived from (§9), transitively.
4. Prune everything else: in every other dataset, all versions not
   protected as above. A (table, partition) or table with an active pin is
   passed over entirely. A non-final dataset that is no longer needed at
   all may be retired, making even its `LATEST` state prunable.

**Consequences**

- Pruning a version referenced only by non-kept revisions makes those
  historical states unresolvable — that is the point of pruning (and reads
  of such states are best-effort, above).
- A dataset's `LATEST` state is protected unless the dataset is retired;
  a retired dataset whose revisions are neither pinned nor held may be
  pruned of version data, while revision and coordination records remain.
- Finalized versions that are never published become prunable once their
  run's `sealed_at` grace deadline (including clock skew) has elapsed, or
  once a newer version of their (table, partition) is published. Versions
  that their sealed run does not list are prunable at once. Versions
  without a manifest are prunable once their claim no longer allocates them
  (the writer released, abandoned, or lost the claim), or once a newer version
  of their (table, partition) is published.
- Revisions are not pruned in this model: they are the history, and the
  `previous_revision` chain must stay walkable from `LATEST`.

### 11. Interoperability: transformation engines

GRV is the **ingest layer** — raw/source data lands in GRV datasets
(versions + revisions) as the durable record of what arrived — and the
**release layer**: final product tables are published back into GRV
datasets as new versions and revisions. Transformation engines are the
**build layer** in between: they run in a warehouse, read materialized GRV
state, and write working tables.

```
GRV (raw) → [pull] → warehouse → engine models → [publish] → GRV (product)
```

Warehouse tables are a working medium — rebuildable, overwrite-in-place,
the engine's normal semantics. GRV versions are the record — immutable and
retained per §10. Everything below the warehouse line is recomputable; what
lands in GRV is deliberate. Several engines can play the build-layer role;
dbt is detailed in §11.1.

**Publish path (engine → GRV).** One GRV run (§6) corresponds to one
engine invocation, or a selected subset of its units of work, per target
dataset. The publisher — a thin wrapper that invokes the engine, exports
the changed output to parquet, runs the §5 claim cycle, and runs the §8
publish step — is what touches the layout; the engine itself only produces
warehouse tables. The run's `metadata` object (below) joins the GRV run
back to the engine's own run record. The publisher also fixes the
provenance boundary: at the start of the engine invocation it records the
target dataset's `base_revision` and, in the run's `inputs` (§6), the exact
source revision of each GRV dataset read, and places their holds (§9).
It derives each version's `derived_from` from those inputs — at table
granularity when the engine only knows which tables a model reads (for
dbt, the model's sources) — and the export must be stable: the warehouse
tables exported are the ones produced from exactly that state (e.g. a
dedicated schema per invocation, or a pull that runs to completion before
the run starts, with further pulls blocked until the invocation and export
finish). If the publish step reports a conflict (§8), another run changed
the same partition first, and the invocation is rerun from the new state.

**Pull path (GRV → warehouse).** For an engine to consume a GRV dataset as
source, a pull job materializes the dataset's current state (the `LATEST`
revision's entries, §7) into warehouse tables: one table per GRV table,
with the `_{key}_` partition columns as ordinary columns — the hive-style
duplication is what makes a flat warehouse table sufficient — and the
revision's table schema (§4), with missing trailing columns filled with
nulls.
The consumer stores a durable checkpoint outside the GRV layout. A pull
resolves one target revision and diffs it against `committed_revision`.
Only changed, added, and removed partitions need work in an uninterrupted
attempt. Omitted partitions and empty versions leave no rows; a table
omitted from the target is emptied, not dropped.

**Transactional refresh.** An adapter MAY apply the entire dataset refresh
in one durable warehouse transaction, including all data and schema changes,
table or view creation, membership changes, ownership records, and the
completion checkpoint. The checkpoint includes `committed_revision` and a
fresh `attempt_id`. Failure before commit rolls back every change. The
adapter also writes an immutable successful attempt receipt, keyed by that
ID, in the same transaction, identifying the fixed request and resulting
revision/generation. Resolve an ambiguous commit by reading that receipt
under the serialization that governs refreshes, after fencing the prior
writer and completing warehouse recovery. A matching receipt proves success
even if a later refresh replaced the current checkpoint. Absence proves no
commit only when the receipt history is complete and trustworthy; a different
checkpoint alone never proves rollback. Preserve receipts for the supported
retry lifetime and reject requests that reuse an ID with different inputs.
These receipts are consumer state outside GRV, not publication commit markers.
There is no separately committed dirty-table journal in this case, because
no partial refresh can survive. A full rebuild and an incremental refresh
have the same transaction boundary; per-table commits do not qualify.

**Retry journal for other adapters.** An adapter that cannot provide that
transaction guarantee stores
`{committed_revision, attempt_id, target_revision, dirty_tables}`.
Before the first mutation to any warehouse table, the
pull durably adds that table to `dirty_tables`. This includes creating a
new table, changing its schema, replacing data, and removing rows. A table
stays dirty until dataset-wide completion; marking before mutation makes
an ambiguous warehouse result safe to retry.

On a retry, including a switch to a newer target after input pruning,
rebuild every dirty table completely from the new target, or empty it if
absent. Preserve the union of dirty tables from all incomplete attempts.
Also apply the normal diff from the last committed revision to the new
target, journaling those tables before mutation. Thus a partition introduced
by a failed attempt is removed even if it appears in neither the committed
revision nor the new target. Reconcile the complete target schema when
rebuilding, including after an interrupted schema change or rollback.

Advance `committed_revision` and clear the attempt/dirty set in **one atomic
checkpoint update**, only after all target changes and dirty-table repairs
have completed durably. A crash before that update leaves the repair journal
intact. If consumer journal state is lost, rebuild all managed warehouse
tables from the target, including emptying managed tables absent from it;
never assume the old watermark describes the current warehouse contents.

For either adapter strategy, one pull may mutate a consumer dataset at a
time. Use a warehouse session lock, or transactions that check a fencing
token on every write and on checkpoint commit; an expiring client-side lease
alone cannot stop delayed
writes from an old pull. A non-transactional adapter keeps consumers blocked
while a pull attempt is incomplete. Readers of a transactional adapter may
continue to use the old complete snapshot. In either case, a build selects
one completed snapshot and keeps it stable for its invocation. Per-table
staging and atomic swap are recommended for non-transactional adapters, but
do not make the multi-table refresh atomic. The checkpoint and any journal
are consumer state outside the GRV layout.

**Engine metadata.** `manifest.json` and run files carry an optional
`metadata` object: a map from **engine name** to that engine's own
fields, e.g.

```json
"metadata": {
  "dbt": {
    "model": "fct_orders",
    "run_id": "20260928T100000_1a2b3c",
    "manifest_sha256": "…"
  }
}
```

The engine name is the key, so entries for different engines cannot
collide, and each sub-object uses that engine's own vocabulary. Core fields
define all GRV semantics: GRV tools MUST NOT depend on `metadata`, and
readers MUST ignore engine entries they do not understand. With it,
`derived_from` and `inputs` answer *which upstream state* a version came
from, and `metadata` answers *which code* produced it.

**No double-versioning.** For any table released to GRV, the GRV
version/revision history is the history of record. An engine's own history
mechanisms may exist as warehouse-internal constructs — including as
*inputs* to a model that releases to GRV, where the mechanics are
flattened into ordinary table data at publish — but they must not be
published as if their own history were the GRV history: publish the
current state; history lives in the revision chain.

#### 11.1 dbt

The concrete mapping for dbt:

- **Units of work.** One dbt invocation — or a selected subset of its
  models — is one GRV run per target dataset. `metadata.dbt` records the
  model, the dbt run id, and the dbt manifest hash, so a GRV run joins back
  to its dbt manifest.
- **Microbatch.** A dbt model that processes data in time batches
  (incremental / event-time selection) SHOULD carry the batch's time key as
  a partition key on its GRV table (e.g. `date=`); partition keys are
  fixed per table (§3), so the alignment is a design-time decision. A
  batch's output is then a *partition*, and a new batch is a *new version
  of one or a few partitions* — not of the whole table: each batch run
  publishes only the partitions it touched, and the new revision records
  them as the current versions of those partitions. A batch rerun is safe:
  it is a new run that publishes a *new version* of the same partition,
  and the superseded version is GC-eligible per §10; a rerun racing another
  run on the same partition conflicts instead of overwriting it (§8). A
  version is a full snapshot of its
  partition, so cost per publish is proportional to partition size. Align
  partition boundaries with batches where practical: coarser partitions
  require rebuilding the entire affected partitions, while finer
  partitions increase the number of versions per batch. A table without a
  natural time partition pays full-table snapshot cost per publish,
  acceptable for small or infrequently published tables only.
- **Incremental models.** A model that reads its own previous output
  (`{{ this }}`) reads it from the warehouse. That self-input is not a GRV
  input; the run's `base_revision` records the GRV state it extends (§6,
  §9).
- **Snapshots.** dbt SCD snapshots are a warehouse-internal construct (see
  the no-double-versioning rule above): a snapshot table is published to
  GRV with its full current contents — all rows, historical SCD rows
  included; selecting only active rows is a model-level transformation,
  not a publish-time one.
- **Terminology.** dbt's `--state` (manifest diffing for change
  detection) is a build-layer concept, unrelated to GRV revisions;
  operations and tooling should not conflate the two.

## Properties and trade-offs

**Properties**

- One path scheme for local / S3 / GCS; the backend contract (§1) is the
  only place backends differ, and the layout never requires rename,
  conditional delete, or unconditional overwrite.
- Immutability plus the manifest-as-commit-marker makes version commits
  safe on object stores without rename: create the data files, then
  `manifest.json`, all with `conditional-create`; readers never observe a
  half-written version, and no writer can overwrite another's objects.
- The claim protocol gives single-writer allocation per (table, partition)
  with durable allocation high-water marks. The dataset lease shares a CAS
  object with `LATEST`, fencing stale publications on every takeover.
  Irrevocable operation decisions fence retention effects on other objects;
  run sealing fences recovery against a live or delayed owner, and
  per-allocation records keep parallel tasks off a shared journal.
- Publications are first-committer-wins per (table, partition): concurrent
  runs cannot silently overwrite each other's output.
- Each revision is a self-contained snapshot of the dataset's state (a
  possibly empty subset of its tables); `LATEST` is one small pointer, so
  resolving the current state's entries takes one read of `LATEST` and one
  of its revision parquet, before reading manifests and data.
- The `_{key}_` column duplication makes each non-empty data file
  self-contained — it can be copied, opened, or processed by a
  single-file reader without needing its containing path; a version's data
  is the union of its files' rows, and the manifest's per-file hashes and
  validators make the set cheaply verifiable before combining.
- Schemas are compared in logical form, so data files from different
  Parquet writers are compatible whenever their logical types match.
- Full provenance: version → run → base revision and inputs, and version →
  `derived_from` → (other dataset, revision, optionally table and
  partition), checked at publish.
- GC protection is declarative and per dataset: active pins and dependency
  holds protect; pending versions and recently superseded revisions are
  protected for `pending_grace`; everything else is prunable; a `.retired`
  dataset releases even its `LATEST` retention; pruning is
  tombstone-first; `.pruned` records who pruned and when.

**Trade-offs**

- On object stores, updating `LATEST` is not atomic with writing the
  revision parquet. Convention: write `revision={n}/data.parquet` first,
  then compare-and-swap `LATEST`, so `LATEST` never points at a missing
  revision and orphan revisions are legal. A `LATEST` that nonetheless
  names a missing or invalid revision is a protocol violation; recovery is
  manual (§8) — no reader-side fallback is defined.
- The claim protocol depends on the backend contract's
  `conditional-create`/`conditional-put` (§1) and on bounded clock skew; a
  holder must renew within its TTL, and a crashed claimant's allocation is
  blocked until the TTL (at most `max_lease_ttl`) elapses.
- Cross-dataset GC safety requires durable source holds: each run creates
  one hold record per input and reads the source's `LATEST` once, and the
  source's GC spends one extra `LATEST` write per prune batch on its
  intent. Holds are revision-wide and transitive through pipelines, so they
  can over-retain inputs until every entry that cites them is pruned, and
  the dataset derivation graph must be acyclic, which GRV does not check
  (§9). Grace protects publication latency and readers, but retention
  safety never depends on a run finishing within it.
- A consumer needs create access to its own subfolder of each source's
  `.holds/`, besides read access. It never writes a source's `LATEST`, so,
  provided permissions confine it to that subfolder, it cannot publish to,
  lock, or block a source, or affect other consumers' holds. A faulty
  consumer can only make a source retain data longer, for example with
  unreadable hold records, which GC keeps and reports. Holds of a crashed
  consumer run stay until the consumer's own recovery seals it.
- Pins, GC decisions, and publications serialize on each dataset's
  `LATEST`, and every coordination record is a single object. GCS throttles
  sustained writes to one object at about one per second, and S3 rejects
  concurrent conditional writes to one key. A publish writes `LATEST` twice
  when it finishes within its lease TTL, so each dataset sustains at most a
  few publications per second, and consumers add no `LATEST` traffic. Large
  prune batches can delay other work; bounded batches limit that delay.
  Pending decisions must be recovered before new decisions, so failed
  helpers affect availability, not retention safety.
- Concurrent runs that change the same (table, partition) conflict, and the
  loser must be rebuilt; explicit selections are the override.
- Run conflict checks inspect committed revision states since each run's
  base, including changes later reversed. Their cost grows with the number
  of publications during the run; historical version data is not needed.
  GC's pending test needs facts from every committed revision; caching them
  incrementally (§10) bounds each pass to the revisions published since the
  last one.
- Schema registration is table-wide and append-only in logical form.
  Failed writes may reserve unused columns, and a mistaken registration can
  only be corrected with a new table. Nullability is not part of the
  logical schema, so non-null constraints are data rules, not schema
  rules. Lowercase-only identifiers reject some source names; callers must
  choose explicit mappings.
- Pins last until an explicit unpin; there is no expiry.
- Pull adapters either commit the complete refresh and checkpoint in one
  durable transaction or keep a durable dirty-table journal and may rebuild
  whole tables after partial failure. Without either guarantee, the last
  successful watermark alone is insufficient.
- This is the store's first on-disk format: there is no earlier data and no
  migration path. `grv.json`'s `format_version` lets a later format detect
  this one and lets this one refuse a later format.
- Verification normally compares backend validators, not content: it is a
  metadata read, but it trusts the backend's content validation semantics,
  not a universal per-rewrite counter. A full SHA-256 check is the fallback
  after copies and for audits. On the local backend every validator read
  synchronizes the file and its path, and hashes the whole object unless the
  optional cache for immutable objects (§1) applies.
- Revisions are full snapshots: reading a named revision's entries is a
  single parquet read, but each revision is O(state size), so
  revision-history storage grows with state size times revision count;
  compaction is a separate concern (below). Run controls, allocation
  records, operation descriptions, holds, pins, and release markers also
  accumulate durably.
- Every version allocation lists all historical `version={n}/` prefixes of
  a (table, partition), including pruned tombstones; a long-lived
  (table, partition) grows this listing linearly. Using the durable counters
  without a listing, or compacting prefixes, requires a separately specified
  restore/compaction protocol.
- Pruning a version makes historical states that needed it unresolvable;
  §10 defines the mechanics, while retention policy (how far back a
  dataset keeps) is a separate concern (below).
- A run file is written only after its entry set is durably sealed,
  so a crash can orphan finalized versions; recovery (§6) salvages those
  it can prove finalized, and the rest are re-produced by a new run.
- The fence assumes conforming writers: every version write follows the
  claim protocol. A non-conforming writer that rewrites an old version's
  data, manifest, *and* run file consistently is undetectable; store-level
  access control, not the layout, is the mitigation.

**Out of scope (separate concern)**

- **Retention policy** — how far back a dataset keeps (default keep-
  windows, per-dataset policy). §10 defines what *may* be pruned; choosing
  what *should* be kept is a separate concern.
- **Compaction** — the revision history (a full snapshot per revision)
  grows without bound, as do allocation listings and GC's scan of committed
  revisions; a compaction/checkpoint mechanism is a separate concern.
- **Data erasure** — immutable versions, pins, and holds leave no
  in-protocol way to remove specific rows or versions sooner than GC would,
  for example to honour a legal deletion request. The companion
  [grv-crypto-shredding-v1-rfc.md](grv-crypto-shredding-v1-rfc.md) erases personal data
  without changing this protocol, by encrypting it under per-subject keys
  and destroying those keys, through the extension hooks in §3 and §4.
- **Operations** — runbooks for protocol violations (§8), stuck pending
  operations, and backup and restore are a separate concern. Restoring part
  of a store, such as only `LATEST`, is forbidden (§8); a restore brings
  back a consistent copy of the whole root while all writers are stopped.
