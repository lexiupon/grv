# GRV (Golden Record Vault) v2 specification

|         |            |
|---------|------------|
| Status  | draft      |
| Version | 2          |
| Date    | 2026-09-28 |

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
- Provenance is auditable from the data directory alone: which **run**
  produced a given version, and which revisions of other datasets a version
  was **derived** from.
- The layout is engine-agnostic: GRV is the ingest (raw/input) and the
  release (output) layer, and transformation engines — dbt among them —
  are the build layer in between (§11).

## Specification

### 1. Storage backends

`GRV_DIR` is a single path scheme, rooted at either:

- a local filesystem path, e.g. `/var/grv/data`
- an S3 URI, e.g. `s3://bucket/prefix`
- a GCS URI, e.g. `gs://bucket/prefix`

Everything below is defined in terms of relative paths under `GRV_DIR`.
The layout is identical on all three; backends differ only in how they
fulfil the **backend contract** below.

**Backend contract.** The layout requires seven operations:

| operation            | meaning                                                             |
|----------------------|---------------------------------------------------------------------|
| `put`                | write an object; readers observe either the old or the new content, never a partial one; returns the new validator |
| `get`                | read an object's content and its validator                          |
| `head`               | read an object's validator and size without its content            |
| `list-by-prefix`     | list object names under a prefix; optionally with delimiter `/`, returning only the immediate child names (common prefixes) |
| `delete`             | remove an object (idempotent)                                     |
| `conditional-create` | create only if the object does not exist, atomically with its full content; fail otherwise; returns the new validator |
| `conditional-put`    | replace only if the object's validator matches one read earlier (compare-and-swap); returns the new validator |

A *validator* is an opaque, backend-specific string used for content
validation and conditional writes: the S3 `ETag`, the GCS object
`generation`, or, locally, the serialized file stat (device + inode +
nanosecond mtime + size). It is not portable across backends or copies,
and is not necessarily a rewrite counter: an S3 ETag can repeat when
identical content is rewritten. Every mutable coordination record (`.claim`,
`LATEST`, run control, and schema baseline) MUST carry a fresh random
`mutation_id` on every transition, including renewal, release, and takeover.
Content must never return to an earlier value, even when the revision number
is unchanged. Control records are never deleted or unconditionally rewritten.

The contract requires strongly consistent object reads and conditional
writes, and listings that include completed creations. A listing is not an
atomic snapshot: decisions spanning objects require the coordination below.
Local implementations must make acknowledged control and immutable-record
writes durable (including file and directory synchronization) before reporting
success. A failed or timed-out request can have committed; callers resolve
ambiguous outcomes from durable records before retrying with new identities.

Every prefix passed to `list-by-prefix` in this specification ends in `/`
(e.g. `<table>/region=eu/`), so that `region=eu` never matches `region=eu2`
and `version=1` never matches `version=10`.

Per-backend mapping:

| operation            | local filesystem                          | S3                                            | GCS                                     |
|----------------------|-------------------------------------------|-----------------------------------------------|-------------------------------------------|
| `put`                | write to temp file, then `rename`         | single or multipart `PUT`                    | object write (new generation)             |
| `get`               | read and `fstat` the same open file descriptor | `GET` + `ETag`                               | `GET` + `generation`                      |
| `head`              | `stat`                                    | `HEAD`                                       | object metadata `GET`                     |
| `list-by-prefix`    | directory walk (`readdir` for delimiter)  | `ListObjectsV2` with `prefix` (+ `delimiter`) | object list with `prefix` (+ `delimiter`) |
| `delete`            | `unlink`                                  | `DELETE`                                     | object delete                             |
| `conditional-create`| write to temp file, then `link(2)` to the target (fails if it exists), then unlink the temp | `PUT` (or multipart complete) with `If-None-Match: *` | write with `ifGenerationMatch=0` (`x-goog-if-generation-match: 0`) |
| `conditional-put`   | under an exclusive `flock` on the containing directory: `stat`, compare, write temp, `rename` | `PUT` with `If-Match: <ETag>`   | write with `ifGenerationMatch=<generation>` |

The local backend requires a filesystem with reliable `flock` and `link`
semantics; network filesystems without them are unsupported.

Two deliberate omissions: the layout never requires **rename** as a layout
operation (object stores lack it; publishing is made safe instead by the
manifest-as-commit-marker, §4 — the local backend may use rename
internally), and never requires **conditional delete**. S3 supports
conditional deletion with `If-Match`, but this layout does not use it;
deletions follow the GC protocol (§10) or are idempotent housekeeping.

### 2. Top-level layout

```
GRV_DIR/
└── datasets/
    └── <dataset>/
        ├── <table>/              # one folder per table (§3)
        ├── .retired              # optional; the dataset is retired (§10)
        ├── .runs/                # run files (§6)
        │   ├── <run-id>.control.json  # durable run lifecycle (§6)
        │   └── <run-id>.json          # immutable sealed run file
        └── .states/
            ├── LATEST            # JSON: revision + dataset lease + pending operation (§8)
            ├── operations/       # immutable operation descriptions (§8, §10)
            │   └── <operation-id>.json
            └── revisions/
                └── revision={n}/
                    ├── data.parquet       # the revision (§7)
                    ├── .superseded.json   # post-publication retention receipt (§8)
                    ├── .holds/            # incoming dependency holds (§9)
                    └── .keep              # optional pin (§10)
```

- The top-level folder is always `datasets/`; each dataset gets one folder
  under it.
- A dataset is a collection of tables; each table lives in its own folder
  directly under the dataset folder.
- `.runs/`, `.states/` and `.retired` are per-dataset metadata. The dot
  prefix distinguishes them from table folders.

The `datasets/` level is deliberate, not ceremony: `GRV_DIR` is a
*root*, and the layout reserves the right to place other top-level
structures there later (system state, GC staging, caches) without a
breaking change. It also keeps "list all datasets" a single delimited
prefix listing on object stores, where a root shared with other tools'
objects would make every listing a filter, and it gives every dataset a
uniform path (`<root>/datasets/<name>/...`). Dropping the level now to save
one path segment would turn re-adding it into a data migration; keeping it
costs one segment.

### 3. Table layout and `.layout.json`

Every table folder contains a `.layout.json` describing the table's layout.
There are two cases:

**Case a — non-partitioned table:**

```
<table>/
├── .layout.json
├── .schema.json         # durable table-wide schema baseline (§4)
├── .claim              # version allocation; cycles acquire→release, never deleted (§5)
└── version={n}/
    ├── data.parquet    # plus optional data-1.parquet, data-2.parquet, ... (§4)
    └── manifest.json
```

```json
{ "table": "<table>", "partition_keys": [] }
```

**Case b — partitioned table:**

```
<table>/
├── .layout.json
├── .schema.json        # shared across all partitions (§4)
└── key1=value1/
    └── key2=value2/
        ├── .claim        # version allocation; cycles acquire→release, never deleted (§5)
        └── version={n}/
            ├── data.parquet    # plus optional data-1.parquet, ... (§4)
            └── manifest.json
```

```json
{ "table": "<table>", "partition_keys": ["key1", "key2"] }
```

`.layout.json` schema — two fields, both required:

| field            | type         | notes                                        |
|------------------|--------------|----------------------------------------------|
| `table`          | string       | must equal the table folder name             |
| `partition_keys` | string array | empty = case a; keys in path order = case b  |

Rules:

- Dataset and table names must match
  `^[a-z0-9][a-z0-9_-]{0,63}$`. The leading-alphanumeric requirement
  means no name can start with a dot, so table folders can never collide
  with `.runs/`, `.states/`, or any `.claim` / `.keep` / `.pruned` /
  `.retired` marker.
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
  b, `version={n}` directories and `.claim` appear only at full partition
  depth; no intermediate directory holds versions.
- Partition **key names** and **values** must both match
  `^[a-z0-9][a-z0-9_-]{0,63}$`. Writers validate on write (layout,
  claim, manifest, revision); non-conforming names or values are rejected.
  Values are always written as strings (a numeric year is `2025`). The
  restricted alphabet makes the `key=value` path form unambiguous — no
  escaping is defined or needed.
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
- In case b, each of a version's data files additionally contains one
  column per partition key, named `_{key_name}_` (e.g. `_key1_`,
  `_key2_`), duplicating the partition values as data (hive-style). The
  writer MUST NOT use a name of the form `_{k}_` (for a partition key `k`)
  for a data column, and every row's `_{k}_` value MUST be the non-null
  string equal to the partition value in the path. For a non-empty file
  this makes each data file self-describing; for an empty file the
  partition identity is taken from the path.
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
  version directory are the GC markers `.keep` and `.pruned` (§10), and
  the only later deletions are GC pruning (§10).
- A version consists of one or more **data files** plus its
  `manifest.json`. Every version has `data.parquet`; a large version may
  additionally have `data-1.parquet`, `data-2.parquet`, …, `data-<k>.parquet`
  with contiguous indices from 1 (`<i>` a positive integer without leading
  zeros; no other `data*` files may appear in a version directory). The
  files are **union-safe**: each is a complete set of rows (no row is split
  across files), every row of the version is in exactly one file, and the
  version's data is the union of its files' rows in any order. All data
  files in one version MUST have identical ordered column schemas,
  including the partition columns. The manifest's `data_files` array names
  exactly the files that exist, in the order `data.parquet`,
  `data-1.parquet`, `data-2.parquet`, … (numeric by index, not lexicographic),
  each with its SHA-256, so a consumer can
  verify the set before combining.
- `manifest.json` is the **commit marker** of the version: a version
  directory is valid only when every data file listed in the manifest and
  the manifest itself are present. Writers create the data files first and
  `manifest.json` last, all via `conditional-create` (§5), so readers never
  observe a half-published version and no writer can overwrite another's
  objects. A version whose data files are later lost (pruning,
  abandonment) is unreadable, and readers report it as such.

**Empty versions (partition tombstones).** A version whose data files
contain no rows (a zero-row parquet) is an **empty version**. Publishing an
empty version of a (table, partition) and including it in a revision is
the explicit termination of that partition's data: the partition stays in
the state, but its current version carries no rows — a self-describing
tombstone that any consumer of the version can see without the revision. A
terminated partition may be re-populated by a later non-empty version. This
complements state-level omission (§7): omitting the (table, partition) from
a revision removes it from the state entirely, while an empty version keeps
it present and explicitly empty. A consumer materializing a state (e.g. the
sync job, §11) treats both as "no rows for this partition in this state." A
tombstone is itself a version: like any version it is keepable and
prunable (§10).

`manifest.json` schema:

| field          | type    | required | notes                                         |
|----------------|---------|----------|-----------------------------------------------|
| `table`        | string  | yes      | table name (equals the folder name)           |
| `partition`    | object  | yes      | `{}` for case a; else the key/value map      |
| `version`      | integer | yes      | the version number of this directory          |
| `run_id`       | string  | yes      | ULID of the run that produced it (§6)        |
| `created_at`   | string  | yes      | RFC 3339 UTC timestamp of publication         |
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

| field      | type    | notes                                             |
|------------|---------|---------------------------------------------------|
| `dataset`  | string  | source dataset                                    |
| `revision` | integer | source revision number in that dataset            |
| `table`    | string  | source table                                      |
| `partition`| object  | source partition (`{}` if non-partitioned)       |
| `retention_id` | string | immutable dependency hold id in the source revision (§9) |

Example (case b, derived from two sources):

```json
{
  "table": "orders",
  "partition": { "region": "eu", "year": "2025" },
  "version": 3,
  "run_id": "01J5K8W3N4R6Y8C2D9F0G1H2J3",
  "created_at": "2026-09-28T10:00:00Z",
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
      "table": "regions",
      "partition": {}
    }
  ]
}
```

`derived_from` is absent for versions produced directly; when present it is
a non-empty array, one element per upstream source (§9). In a
non-partitioned table (case a), `partition` is the empty object `{}`. The
manifest does not duplicate the data's column schema: the parquet file is
self-describing about its own schema.

**Verifying a data file.** A data file *verifies* against its manifest
entry if it exists, its size equals the recorded `size`, and either (a) its
current validator (`head`) equals the recorded `validator`, or (b) its
SHA-256 equals the recorded `sha256`.
Check (a) is a metadata read and is the normal path; check (b) reads the
whole file and is the fallback when validators differ legitimately (the
directory was copied or restored to another bucket or backend) and for
full audits. A version *verifies* when every listed data file verifies and
no other `data*` file is present.

The manifest is also part of the **fence** against stale writers:
`claim_token` ties the version to the claim that allocated it, and the run
file must list the version with the same token (§5, §6).

**Schema evolution.** The table has a durable, table-wide baseline at
`<table>/.schema.json`, independent of version data and retained through
pruning. It contains `mutation_id` and `columns`: the ordered column schema,
including partition columns. Each column records its name and full Parquet
schema definition (physical and logical type, repetition/nullability, and
nested fields). Equality includes these properties; nested changes are not
an appended top-level column. New trailing columns MUST be nullable because
consumers fill them with nulls when reading older versions.

Before writing version data, a writer **registers** its schema:

1. If the baseline is absent, `conditional-create` it with the proposed
   schema. An unsuccessful create proceeds with the existing baseline.
2. Read the baseline. The proposed schema must equal it or extend it by
   appending columns; removing, renaming, re-typing, changing nullability,
   or reordering an existing column is rejected. An extension is installed
   by CAS with a fresh `mutation_id`; on conflict, reread and revalidate.
3. A successful create/CAS, or a read of an equal baseline, authorizes
   that version's schema. A later extension does not revoke this authority:
   the authorized schema remains a prefix of every later baseline.

Incompatible concurrent extensions cannot both succeed, even in different
partitions or disjoint revisions. A registered extension is never rolled
back, including when its writer crashes before producing data; subsequent
writers must honor those reserved columns. A change incompatible with the
baseline, or a change to partition keys, requires a new table.

Every selected version's schema must be a prefix of the durable baseline,
and the publisher verifies this (§7). All versions of a table are therefore
prefix-compatible across its entire history. A revision's table schema is
the longest schema actually selected in that revision; consumers fill
missing trailing columns with nulls. Reselecting an existing older version
is legal. Writing a new version with a shorter schema than the baseline at
registration is not. An incomplete or pruned highest-numbered version is
never used as the schema baseline.

### 5. Version allocation: `.claim`

Publishing a new version of a (table, partition) requires allocating the
next version number. Allocation is mutually excluded by a **claim** file in
the directory that contains the `version={n}` directories for that
(table, partition):

- non-partitioned table: `<table>/.claim`
- partition: `<table>/key1=v1/key2=v2/.claim`

One claim at a time per (table, partition); a claim covers exactly one new
version. A run publishing several new versions of the same (table,
partition) repeats the cycle below. Dataset-level operations use a lease
embedded in `LATEST` (§8), not a separate `.states/.claim`: their lease
transitions must also fence writes of the current-revision pointer.

`.claim` schema — the file's content changes as it moves through the
lifecycle below; **every transition is a `conditional-put` against the
validator of the claim content the writer last read or wrote**; the holder
recognizes its own claim by its `token`:

| field         | type    | present in         | notes                                     |
|---------------|---------|--------------------|-------------------------------------------|
| `holder`      | string  | all states         | the allocating run ULID (§6) |
| `token`       | string  | all states         | opaque random string (e.g. UUID); identifies this claim |
| `version`     | integer | allocated, released| the version reserved by this holder, if any |
| `high_water`  | integer | all states         | greatest number ever reserved; initially 0, never decreases |
| `mutation_id` | string  | all states         | fresh random id for every write (§1) |
| `claimed_at`  | string  | acquired, allocated| RFC 3339 UTC, when the claim was taken     |
| `expires_at`  | string  | acquired, allocated| RFC 3339 UTC, after which the claim is stale; advanced by renewal |
| `released_at` | string  | released           | RFC 3339 UTC, when the holder finished     |
| `outcome` | string | released | `finalized` or `abandoned`; recovery must distinguish them |

Protocol:

1. **Acquire.** `conditional-create` `.claim` with `holder`, `token`,
   `claimed_at`, `expires_at`, `high_water: 0`, and `mutation_id`.
   If the create fails, `get` the existing claim: if it is unexpired and
   held by another, abort (or wait and
   retry, at the caller's discretion); if it is expired, or is a release
   record, take it over with a `conditional-put` against the validator just
   read, writing a new `holder`, `token`, and timers, and preserving
   `high_water` unchanged. Acquisition never erases allocation history.
2. **Allocate.** The holder lists the `version={n}/` prefixes (delimited
   listing), takes `n = max(highest listed, high_water) + 1`, and CAS-writes
   both `version: n` and `high_water: n` in the same transition. Every later
   transition preserves or increases `high_water`, including takeover,
   renewal, release, and abandonment before allocation. Thus any number
   reserved by a successful CAS stays reserved across arbitrarily many
   crashes, even if no version object was written. Register this allocation
   in the open run control (§6) before writing any version objects.
3. **Write data.** Register the table schema (§4) and acquire the source
   dependency holds (§9), if derived. Create each of `version={n}`'s data
   files via `conditional-create`, recording each returned validator. A failed create
   means another writer has objects at this number: abandon the version
   (step 7).
4. **Commit.** **Renew** the claim — a `conditional-put` that advances
   `expires_at` — and, only if it succeeds, create
   `version={n}/manifest.json` (§4) via `conditional-create`, with
   `claim_token` set to this claim's `token` and `data_files` recording each
   file's SHA-256, size and validator.
5. **Confirm.** Verify the version (§4) and check that no `.pruned` is
   present in its directory. If this fails, the version was pruned or
   damaged mid-flight: abandon it (step 7).
6. **Release.** Rewrite `.claim` (`conditional-put`) into a release record:
   `holder`, `token`, `version`, `high_water`, `released_at`,
   `outcome: finalized`, and a fresh `mutation_id`. A version whose release
   succeeded is **finalized**; record its finalized entry by CAS in the still-open run
   control (§6). The claim file is **never deleted**. Its high-water mark
   survives even when the next acquirer crashes before allocating.
7. **Abandon.** Release the claim with `outcome: abandoned`, preserving
   `high_water`, and mark the allocation abandoned in the run journal if
   still authorized. Do not list it in the run file. Its objects are left
   for GC (§10). Recovery must not salvage an abandoned release record.

**Renewal.** A holder may renew at any time, and MUST renew at intervals
shorter than its TTL while it works; the TTL (the duration from a successful
acquisition or renewal to `expires_at`) must exceed the interval between
renewals plus the maximum clock skew between writers. A failed
renewal, release, or other claim CAS means the claim was taken over: the
holder is **stale** and MUST stop writing objects for that version at once;
the version is not finalized.

**Fence.** A version is **publishable** — may be referenced by a revision
(§7) — iff all of:

- its `manifest.json` exists and matches its path (`table`, `partition`,
  `version`);
- no `.pruned` exists in its directory;
- it verifies (§4);
- its run file (§6) matches its sealed run control and lists
  (table, partition, version) with the manifest's `claim_token`;
- its `derived_from` references resolve and have active dependency holds
  bound to this version and claim token (§9);
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
A run publishes into exactly one dataset; a job writing to several datasets
performs one run per dataset. Each run has a unique `run_id` and its
metadata is stored at:

```
<dataset>/.runs/<run-id>.control.json   # mutable lifecycle and entry journal
<dataset>/.runs/<run-id>.json           # immutable sealed result
```

`run_id` is a **ULID** in canonical uppercase form, matching
`^[0-7][0-9A-HJKMNP-TV-Z]{25}$`. The leading character is restricted to
`0`–`7` so the 26-character encoding fits in 128 bits. Run ids are unique
across all datasets. ULIDs embed a 48-bit timestamp, so lexicographic order
follows timestamp order; no stronger cross-writer chronological guarantee
is made (same-timestamp ties and clock skew).

Run file schema:

| field        | type   | required | notes                                    |
|--------------|--------|----------|------------------------------------------|
| `run_id`     | string | yes      | the run's ULID; the file name is `<run_id>.json` |
| `created_at` | string | yes      | RFC 3339 UTC                             |
| `entries`    | array  | yes      | one entry per finalized version          |
| `inputs`     | array  | no       | the source revisions the run read, each `{dataset, revision}`; recorded at the start of the run (§11) |
| `metadata`   | object | no       | map of engine name → engine-specific fields (§11) |

Each entry: `table` (string), `partition` (object; `{}` for
non-partitioned), `version` (integer), `claim_token` (string; the token of
the claim that allocated and released it, §5).

```json
{
  "run_id": "01J5K8W3N4R6Y8C2D9F0G1H2J3",
  "created_at": "2026-09-28T10:00:00Z",
  "entries": [
    { "table": "customers", "partition": {}, "version": 2,
      "claim_token": "3b7d0c2e-1f4a-4e9b-8c6d-2a5e7f9b1c0d" },
    { "table": "orders", "partition": { "region": "eu", "year": "2025" }, "version": 3,
      "claim_token": "9f2c1e4a-7b3d-4c1e-9a02-6d5f8e1b2c3d" }
  ],
  "inputs": [
    { "dataset": "raw_events", "revision": 7 },
    { "dataset": "ref_data", "revision": 2 }
  ]
}
```

A run only *publishes* version directories (via the claim cycle, §5).
Versions enter a dataset's state only when a revision (§7) references
them; the publish step (§8) is what advances `LATEST`. The lifecycle is
therefore run → publish step → `LATEST`, in that order.

**Run lifecycle and finalization.** Before any allocation, the owner creates
`<run-id>.control.json` with `conditional-create`. It records `run_id`,
`created_at`, fixed `inputs` and metadata, `owner_token`, `expires_at`,
`mutation_id`, `phase: open`, and an initially empty allocation/entry journal.
All changes and renewals use CAS; parallel tasks must merge journal updates
on conflict and check the owner token and phase before retrying. Only the
current owner of an unexpired `open` record may register new work. The run
lease is renewed between partition writes as well as during them.

Each allocation is registered with (table, partition, version, claim_token)
before data writes. After successful partition-claim release, a CAS marks
that entry finalized. Entries abandoned by the owner are explicitly marked
abandoned. A stale owner cannot register or finalize entries after recovery
changes the run token or phase. Allocation or data writes already in flight
may leave orphan objects, but cannot change the sealed result.

Once its tasks have finished, the owner CAS-transitions `open` → `sealed`,
freezing the exact run file payload: the finalized entries and fixed run
metadata. The normal owner may seal only when every registered allocation
is finalized or explicitly abandoned. Sealing is irreversible. Only this payload may be materialized as
`<run-id>.json`, via `conditional-create`; any helper can do so. An existing
file must match the sealed payload, otherwise it is a protocol violation.
The immutable run file is the run's commit marker and is never modified.
Publishers require it to match a `sealed` control record. A version with no
committed run file is **orphaned** and is not publishable.

**Recovering a crashed run.** Partition claims being released or absent is
not evidence that a run has finished. Recovery first reads the run control:

- If it is `open` with an unexpired lease, recovery must wait, including
  when there are currently no outstanding partition claims.
- If `open` is expired, recovery CAS-transitions it to `recovering`, with
  a fresh owner token and lease, freezing the candidate allocation list.
  This transition races atomically with the owner's renewals and sealing.
- If `recovering` expires, another recovery worker may take over by CAS,
  preserving the frozen list and any recorded finalized entries.
- If `sealed`, recovery only materializes the already-frozen payload.

In `recovering`, no new allocations may be added. Recorded finalized
entries are durable proof of release. For remaining candidates, recovery
may also prove finalization from a matching partition release record with
`outcome: finalized`, or wait for a matching allocated claim to expire,
verify its version, and
CAS-release that claim on the run's behalf (§5 steps 5–6). An unexpired
partition claim must be allowed to finish or expire; recovery cannot assume
that taking over the run record stopped an in-flight partition write.
Candidates whose finalization cannot be proven, including claims already
taken over by another run, stay orphaned. Recovery records each proven
entry in its control record, then CAS-seals the result and materializes the
run file. A losing owner/recovery worker can never create a different run
file, because only the winning sealed payload is authorized.

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

**Validity** is checked by the publisher when the revision is written
(§8). A revision is valid iff (a) each (table, partition) appears at most
once; (b) the three metadata keys are present and `grv.revision` equals
the path's `n`; (c) every referenced version is publishable (§5); (d) each
referenced table has a `.layout.json`, and each `partition` string is the
canonical path form for that table's `partition_keys`; (e) each row's
`run_id` equals the referenced manifest's `run_id`; (f) the versions of
each table are prefixes of that table's durable schema baseline (§4).

**Resolvability** is a separate, time-dependent property: a revision is
resolvable while every version it references is still present and
verifies. Validity never changes after the write; resolvability is lost
when GC prunes a version (§10). A valid but unresolvable revision is not a
protocol violation — it is the expected result of pruning.

The `revision={n}` path segment deliberately reuses the same `key=value`
path convention as partitions and versions. Like version numbers, revision
numbers use canonical decimal without leading zeros and satisfy
`1 ≤ n ≤ 2^63−1`; `0` is only the no-predecessor sentinel. Version and
revision allocation MUST fail on exhaustion rather than wrap or reuse a
number.

### 8. Publish step and `LATEST`

A dataset's **publish step** selects versions, writes a revision, and
advances its current state. The dataset lease and current revision share
one CAS object, `<dataset>/.states/LATEST`. There is no separate dataset
claim. `LATEST` is JSON with these required fields:

| field | meaning |
|-------|---------|
| `revision` | current revision number; `0` means no revision published yet |
| `high_water` | greatest revision number ever reserved; initially 0, never decreases |
| `mutation_id` | fresh random id on every mutation, even if `revision` is unchanged |
| `lease` | null, or `{holder, token, claimed_at, expires_at}`; holders include publishers, GC, and retention operations |
| `pending` | null, or the path of a committed immutable operation description under `.states/operations/` |

The first caller initializes this object with `conditional-create`, then
acquires its lease by CAS. Acquisition, takeover, renewal, reservation,
commit, and release all update this same object, preserving unrelated
fields. Acquiring a released lease sets a new token; taking over an active
lease is allowed only after expiry. A holder uses its latest successful CAS validator; after a CAS
failure it must reread and check that it still owns the token before doing
anything further. Lease expiry permits takeover; the successful takeover
CAS is the fence. Every claimant, including GC, changes the validator even
when it leaves `revision` unchanged. Lease durations and renewal follow §5.

**Durable operations.** A lease alone cannot fence a delayed write or delete
to a different object. Side effects used by retention therefore require a
committed operation. Each description has `operation_id`, `kind` (publish,
pin, hold, release_hold, retire, or prune), and a complete `payload` with
all target paths and facts needed to replay its effects without the original
process. Audit timestamps prepared in this description are not supersession
timestamps.

1. Under the lease, validate the intended operation and create its immutable
   description at `.states/operations/<operation-id>.json` with a unique id.
2. CAS `LATEST.pending` from null to that path, retaining the lease. This
   CAS commits the decision. A description not referenced by a successful
   commit is an orphan and authorizes no effects.
3. Materialize the operation's idempotent effects. Clear `pending` by CAS
   only after its required durable markers exist. Do not start or commit
   another operation while `pending` is non-null.

On takeover, the new holder MUST complete any pending operation before
validating new work. Committed decisions are irrevocable; replay writes
only immutable markers via `conditional-create` and accepts an existing
marker only if it represents the same fact. A helper may finish an already
committed operation after losing its lease, but may not commit a new one.
Operations and their markers are retained. Pin creation, dependency holds,
retirement, and prune batches use this protocol (§9–§10).

The publish step is:

1. **Acquire** the lease in `LATEST`, complete any pending operation, and
   reject a retired dataset. Read its `revision` as the predecessor.
2. **Compute** the next state from that revision (empty if `revision = 0`),
   applying replacements and omissions. Any new source dependency holds
   must already exist (§9); never hold two dataset leases at once.
3. **Allocate** `n = max(high_water, revision, highest existing revision
   number) + 1` and CAS-store `high_water: n`. Every transition, including
   takeover before allocation, preserves this durable reservation (§5).
4. **Validate** the selected versions (§7), including schema baselines,
   sealed run files, tombstones, and source dependency holds.
5. **Renew** the lease and create `revision={n}/data.parquet` with
   `conditional-create`, setting `grv.previous_revision` to the predecessor.
   A collision is an error; never overwrite or reuse the number.
6. **Commit** by a single CAS that changes `revision` to `n` and sets
   `pending` to a previously created publish operation describing `n` and
   its predecessor. Use the latest validator belonging to this lease.
   A takeover by GC or any other holder makes this CAS fail, even if no
   other revision was published. On failure, abort and recompute under a
   new acquisition; do not retry the old revision against a fresh validator.
7. **Record supersession.** If the predecessor is nonzero, after observing
   the committed pointer, create its `.superseded.json` with
   `{successor: n, observed_at: <current UTC time>}`. Clear `pending` only
   after this receipt exists, then release the lease by CAS. For the first
   revision there is no predecessor receipt to write.

`observed_at` is sampled after observing the successful publication CAS,
never while preparing the revision or operation description. A helper after
a crash may record a later time; this conservatively extends retention.
If a predecessor is on the current chain but has no receipt, GC keeps it.
GC uses the receipt with the matching chain successor, plus the clock-skew
margin (§10), never the successor parquet's `grv.created_at`.

Readers read `LATEST.revision` → the revision parquet → manifests → data.
They ignore `lease` and `pending`; `revision = 0` means an empty initial
state, as does an absent `LATEST` before initialization. Data and revision objects exist before publication, and a pending
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
one or more specific revisions of other datasets (e.g. a table that is a
function of tables in upstream datasets as of particular revisions of
each). The derivation is recorded in the `derived_from` array of the new
version's `manifest.json` (§4): one source reference per upstream
dependency — source dataset, source revision, source table, source
partition. This makes the provenance chain auditable from the data
directory alone.

A version's `derived_from` references **resolve** when each named
(dataset, revision) exists, and the named (table, partition) is in that
revision with an available version. A check alone cannot protect a reference
from concurrent upstream GC. Every derived version therefore acquires
**durable dependency holds** before its manifest is written:

1. After reserving the target version, generate a unique `retention_id`
   for each source revision. Acquire that source dataset's `LATEST` lease,
   complete pending operations, and verify the source revision's entire
   state is still available and none of its versions has been committed
   for pruning. This check covers the whole state because retention is
   revision-wide (§10).
2. Commit a hold operation (§8) and materialize
   `.states/revisions/revision={n}/.holds/<retention_id>.json`. It records
   the target dataset, table, partition, version, and claim token. A hold
   keeps the entire source revision even before the target manifest exists.
   Finish the operation and release the source lease before taking another.
3. Record the id in the corresponding `derived_from` entries. The manifest
   and publisher must match each hold to this exact target allocation and
   require that it has not been released. The target publisher holds only
   its own dataset lease; it does not acquire source leases.

An active hold is one whose immutable record exists and whose matching
`<retention_id>.released.json` does not. Only after the target version has
an irreversible `.pruned` tombstone may a source-side operation release the
hold, by creating that release marker under the source dataset lease.
The hold id is never reused. Release does not require holding a target
lease because target pruning cannot be undone. Holds are not released just
because a run lease expired, a revision was superseded, or a grace timer
elapsed. Crashed acquisitions can over-retain sources; GC may release such
holds after the target allocation has been tombstoned, including an
allocation with no data yet. Unreferenced operation descriptions create no
holds. Source-side cleanup can discover the target from the hold itself;
it must not depend on a target manifest that GC may already have deleted.
Automatic cleanup of uncertain ownership must err toward retention.

This ordering prevents concurrent publication in another dataset from
adding an unobserved dependency during upstream GC: hold registration and
pruning commit against the same source `LATEST`. If pruning wins, the hold
fails; if the hold wins, pruning excludes its source state. Sequential
acquisition avoids cycles of dataset locks. Inputs can still disappear
before a hold is acquired, so acquisition may fail and the run must retry
from available inputs. The pending grace period provides time for this
acquisition; it is not the proof of cross-dataset GC safety.

Validated derivation links point to revisions that existed before the target
version was committed. Dependency holds preserve those references through
publication and until the target is pruned.

### 10. Garbage collection: `.keep` and `.pruned`

The layout assumes a GC process that prunes old versions. Protection is
declarative, via marker files whose *presence* is the signal (an optional
JSON body may record `kept_by`, `kept_at`, `reason`):

- `<table>/.../version={n}/.keep` — this version must not be pruned.
- `<table>/<partition>/.keep` — **no version** of this (table, partition)
  may be pruned.
- `<table>/.keep` — **no version** of this table, in any partition, may be
  pruned. (For a non-partitioned table this coincides with the previous
  rule.)
- `<dataset>/.states/revisions/revision={n}/.keep` — this revision is
  **pinned** (see the keep set below).
- `<dataset>/.retired` — the dataset is **retired**: its `LATEST` is no
  longer unconditionally kept (see the keep set below).

GC is configured with a **pending grace** period, `pending_grace`, chosen
to exceed the expected worst-case time from reading inputs to acquiring
source holds, and from committing output to publication. It protects work
that is not yet durably referenced. Exceeding it can cause a run to retry;
it must not permit publication of broken dependencies (§9).

Retention deadlines include the configured maximum relative clock skew
between participants. A timestamp `t` is not considered expired until
`now >= t + pending_grace + max_clock_skew`. A supersession receipt is
sampled only after publication (§8); missing receipts extend protection.
Grace measured from manifest creation protects pending output, while grace
for a previously current revision starts from observed supersession.

A version is **pending** if no revision of its dataset references it and
its number is greater than every version of its (table, partition) that
any revision references — i.e. it is newer than anything ever published
for that (table, partition). Versions being written, and finalized
versions awaiting their publish step, are pending.

**Keep set and protected versions.** The keep set (of revisions, across
all datasets) and the set of protected versions are computed together, to
a fixpoint.

A revision is in the keep set if any of:

- it is the current `LATEST` of its dataset, unless the dataset is
  **retired** (`.retired` marker present);
- it is **recently superseded**: it lies on the `previous_revision` chain
  from `LATEST.revision`, and its `.superseded.json` receipt for the next
  revision on that chain is absent or has not expired under the deadline
  rule above. The successor's creation time is irrelevant;
- it has a `.keep`;
- it has an active incoming dependency hold (§9);
- it is named by any element of the `derived_from` of a protected version
  — the closure walks *backward* across datasets, following derivation
  links.

A version is protected if any of:

- its directory has a `.keep`;
- its (table, partition) directory or its table directory has a `.keep`;
- it appears in the state of a revision in the keep set of its own dataset;
- it is pending, and it either has no `manifest.json` yet or its
  manifest's `created_at` has not expired under the deadline rule above.

Because `derived_from` of every protected version feeds the keep set, a
directly kept derived version, a version of a kept product state, and a
pending derived version all retain their upstream revisions. Incoming
holds conservatively retain sources even for an unprotected target until
that target is actually tombstoned and the holds are released. Missing
referenced revisions, missing required holds, or other incomplete retention
metadata are protocol errors: GC aborts affected deletions, rather than
ignoring a dependency it cannot evaluate.

The closure is a walk over (dataset, revision) nodes. Validated
derivation links point backward in time (§9), so a cycle cannot arise from
conforming writers; if one is found anyway, the walk reports it and GC
aborts the affected deletions rather than proceeding on an incomplete
closure. A node reached twice through different paths is not a cycle.

**Retirement.** A dataset that is fully superseded — no longer a product,
and no longer needed by any kept state — may be marked
`<dataset>/.retired` by a committed retirement operation under its dataset
lease (§8), with an optional body recording `retired_by`, `retired_at`, and
`reason`. Raw marker writes are not conforming. While the marker is
present, the dataset's `LATEST` is *not* in the keep set: the dataset is
retained only to the extent that kept revisions — its own, or other
datasets' via the `derived_from` closure — still reference it, and may
otherwise lose all version data while retaining revision and coordination
records. Its `LATEST` then remains valid but may become unresolvable (§7). Retirement
is terminal: no new runs or publish steps are made against a retired
dataset.

The third protection rule is deliberately coarse: a kept revision of an
upstream dataset protects *its entire state* — every (table, partition,
version) it names — even though a downstream product may have used only a
few of them. Provenance dependencies are revision-wide, not
selector-narrow.

**Pinning.** `.keep` markers MUST be created by a pin operation under the
owning dataset's `LATEST` lease (§8). Direct file creation followed by a
`.pruned` check is not a valid pin protocol.

1. Acquire the lease and finish any pending operation. A pending prune
   decision must become durable tombstones before pin validation.
2. For an individual version or revision, verify all data in the requested
   scope is available, with no `.pruned` tombstones. A missing or pruned
   version makes the pin fail. Table/partition pins protect all currently
   unpruned allocations and future versions; they do not resurrect earlier
   tombstones, which the result must report as excluded.
3. Create the immutable pin description, commit it through `LATEST.pending`,
   materialize `.keep`, and complete the operation. Report success only
   after the commit is known and its marker is durable. A crashed caller's
   committed pin is completed on takeover before GC can proceed.

Pin and prune commits compete on the same CAS record. If pin wins, GC sees
its protection; if prune wins, a specific-version/revision pin fails.
Checking markers just before deletion is insufficient. Explicit `.keep`
pins are append-only in this version of the protocol; deleting them by hand
is not a supported unpin operation. Dependency holds use the separate,
terminal release protocol in §9.

**GC coordination and pruning.** A GC pass acquires the target dataset's
lease in `LATEST`, completes pending operations, and computes its protected
versions. Publications, pins, retirement, and incoming dependency hold
changes for this dataset all use this same lease and CAS object. Every
relevant protection must be read after acquisition; an earlier candidate
listing is only a hint. Source-side holds make the cross-dataset closure
safe even while other datasets publish. New local versions may appear
concurrently; GC only commits an explicit set of known allocations after
checking each one's protection. Known allocations include claim/run journal
reservations without objects; never infer that an unobserved number is free.

For each batch of unprotected versions:

1. Write an immutable prune operation description with the exact
   (table, partition, version) targets and audit fields.
2. CAS `LATEST.pending` to that description while still owning the lease.
   This is the **irrevocable prune decision**. A failed CAS authorizes no
   tombstones or deletes; reread and recompute under a new lease.
3. Create every target's `.pruned` via `conditional-create`. A successor
   holder must finish these markers before allowing any new pin, hold, or
   publication. A committed target can never become publishable again.
4. Once all tombstones are durable, clear `pending` by CAS. Delete only
   tombstoned targets' data files and then their manifests; these physical
   deletions can finish outside the lease. Never delete `.pruned`, `.keep`,
   allocation records, schema baselines, or run/operation/revision records.

This ordering also fences a stale GC worker: it may finish deletions only
for an already committed, permanently tombstoned target, which a later
publisher or pin cannot revive. A crash before step 2 leaves an inert
operation description. A crash afterwards leaves replayable work. A delayed
version writer may create residual objects beneath a tombstone; they remain
unreadable and can be deleted by another sweep. No number is ever reused.

Example `.pruned` body:

```json
{
  "operation_id": "808fcb48-1801-43ba-b069-dfa144570b24",
  "pruned_by": "grv-gc/1.2.0",
  "pruned_at": "2026-12-01T00:00:00Z",
  "table": "orders",
  "partition": { "region": "eu", "year": "2025" },
  "version": 3
}
```

A version directory containing `.pruned` is a tombstone: readers report the
version as unavailable regardless of residual data. GC can release its
outgoing dependency holds after this marker is durable (§9), allowing later
passes to reclaim upstream data. GC never acquires two dataset leases at
once; it releases the target lease before releasing holds in source datasets.

**Reader semantics.** Reads are best-effort and point-in-time. A reader
resolves a state from the `LATEST` (or a named revision) it read; no pin
or lease is taken, and a concurrent GC pass may prune a version while the
read is in flight. A reader that encounters a `.pruned` tombstone, or a
missing object, reports that version as unavailable; the read may be
retried against the then-current state.

**Final-product retention.** The intended workflow for data products:

1. Identify the datasets that are *final* products.
2. `.keep` the revisions of those datasets that back the product states
   that must remain resolvable.
3. Let the keep set close over `derived_from` — the backward walk marks
   the upstream revisions whose states the product was derived from.
4. Prune everything else: in every other dataset, all versions not
   protected as above. A (table, partition) or table with a `.keep` is
   passed over entirely. A non-final dataset that is no longer needed at
   all may be marked `.retired`, making even its `LATEST` state prunable.

**Consequences**

- Pruning a version referenced only by non-kept revisions makes those
  historical states unresolvable — that is the point of pruning (and reads
  of such states are best-effort, above).
- A dataset's `LATEST` state is protected unless the dataset is retired;
  a retired dataset no longer referenced by any kept revision may be
  pruned of version data, while revision and coordination records remain.
- Finalized versions that are never published become prunable once
  the manifest's grace deadline (including clock skew) has elapsed, or once a
  newer version of their (table, partition) is published. Abandoned
  versions without a manifest are reclaimed only in the latter case.
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
GRV (raw) → [sync] → warehouse → engine models → [publish] → GRV (product)
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
provenance boundary: it records in the run's core `inputs` field (§6) the
exact source revision of each GRV dataset read at the start of the engine
invocation, derives each version's `derived_from` from those revisions,
and the export must be stable — the warehouse tables exported are the ones
produced from exactly that state (e.g. a dedicated schema per invocation,
or a sync that runs to completion before the run starts, with further
syncs blocked until the invocation and export finish).

**Sync path (GRV → warehouse).** For an engine to consume a GRV dataset as
source, a sync job materializes the dataset's current state (the `LATEST`
revision's entries, §7) into warehouse tables: one table per GRV table,
with the `_{key}_` partition columns as ordinary columns — the hive-style
duplication is what makes a flat warehouse table sufficient — and the
table's state schema (§4), with missing trailing columns filled with nulls.
The consumer stores a durable checkpoint outside the GRV layout:
`{committed_revision, attempt_id, target_revision, dirty_tables}`. A sync
resolves one target revision and diffs it against `committed_revision`.
Only changed, added, and removed partitions need work in an uninterrupted
attempt. Omitted partitions and empty versions leave no rows; a table
omitted from the target is emptied, not dropped.

**Retry journal.** Before the first mutation to any warehouse table, the
sync durably adds that table to `dirty_tables`. This includes creating a
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

One sync may mutate a consumer dataset at a time. Use a warehouse session
lock, or transactions that check a fencing token on every write and on
checkpoint commit; an expiring client-side lease alone cannot stop delayed
writes from an old sync. Keep consumers blocked while a sync attempt is
incomplete. Per-table staging and atomic swap are recommended, but do not
make the multi-table refresh atomic. Engine invocations wait for the atomic
completion checkpoint and keep their inputs stable as described above.
Both the watermark and journal are consumer state outside the GRV layout.

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
  it publishes a *new version* of the same partition, and the superseded
  version is GC-eligible per §10. A version is a full snapshot of its
  partition, so cost per publish is proportional to partition size. Align
  partition boundaries with batches where practical: coarser partitions
  require rebuilding the entire affected partitions, while finer
  partitions increase the number of versions per batch. A table without a
  natural time partition pays full-table snapshot cost per publish,
  acceptable for small or infrequently published tables only.
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
  only place backends differ, and the layout never requires rename or
  conditional delete.
- Immutability plus the manifest-as-commit-marker makes publishing safe on
  object stores without rename: create the data files, then
  `manifest.json`, all with `conditional-create`; readers never observe a
  half-published version, and no writer can overwrite another's objects.
- The claim protocol gives single-writer allocation per (table, partition)
  with durable allocation high-water marks. The dataset lease shares a CAS
  object with `LATEST`, fencing stale publications on every takeover.
  Irrevocable operation decisions fence retention effects on other objects;
  run sealing fences recovery against a live or delayed owner.
- Each revision is a self-contained snapshot of the dataset's state (a
  possibly empty subset of its tables); `LATEST` is one small pointer, so
  resolving the current state's entries takes one read of `LATEST` and one
  of its revision parquet, before reading manifests and data.
- The `_{key}_` column duplication makes each non-empty data file
  self-contained — it can be copied, opened, or processed by a
  single-file reader without needing its containing path; a version's data
  is the union of its files' rows, and the manifest's per-file hashes and
  validators make the set cheaply verifiable before combining.
- Full provenance: version → run → entries and inputs, and version →
  `derived_from` → (other dataset, revision), checked at publish.
- GC protection is declarative and cross-dataset: committed `.keep` pins,
  active dependency holds, and the closure over `derived_from` protect;
  pending versions and
  recently superseded revisions are protected for `pending_grace`;
  everything else is prunable; a `.retired` dataset releases even its
  `LATEST` retention; pruning is tombstone-first; `.pruned` records who
  pruned and when.

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
  blocked until the TTL elapses.
- Cross-dataset GC safety requires durable source holds. They add source
  metadata writes and can over-retain inputs until target pruning and hold
  release. Grace protects acquisition/publication latency, but safety no
  longer depends on every run finishing within that bound.
- Pins, incoming holds, GC decisions, and publications serialize on each
  dataset's `LATEST`. Large prune batches can delay other work; bounded
  batches limit that delay. Pending decisions must be recovered before
  new decisions, so failed helpers affect availability, not retention safety.
- Schema registration is table-wide and append-only. Failed writes may
  reserve unused columns. Lowercase-only identifiers reject some names
  accepted by earlier drafts; callers must choose explicit mappings.
- Explicit `.keep` pins are permanent in this version. Reversible pins
  would need their own generations and fenced removal protocol.
- Sync recovery stores a durable dirty-table journal and may rebuild whole
  tables after partial failure; the last successful watermark alone is
  insufficient.
- This draft changes the storage protocol: `LATEST` becomes JSON, claim
  records gain durable counters, runs gain control records, tables gain
  schema baselines, and derived references require holds. Writers from
  earlier drafts must not operate concurrently on this layout. Migration
  must preserve reservations and reconstruct/validate durable metadata
  while old writers are stopped; automatic migration is not specified.
- Verification normally compares backend validators, not content: it is a
  metadata read, but it trusts the backend's content validation semantics,
  not a universal per-rewrite counter. A full SHA-256 check is the fallback
  after copies and for audits.
- Revisions are full snapshots: reading a named revision's entries is a
  single parquet read, but each revision is O(state size), so
  revision-history storage grows with state size times revision count;
  compaction is a separate concern (below). Run journals, operation
  descriptions, holds, and release markers also accumulate durably.
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
  grows without bound, as do allocation listings; a compaction/checkpoint
  mechanism is a separate concern.
