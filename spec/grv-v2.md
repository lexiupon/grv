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

A *validator* is the backend's identity for an object's content: the S3
`ETag`, the GCS object `generation`, or, locally, the file stat (device +
inode + nanosecond mtime + size). A validator changes whenever the object is
rewritten; it is not portable across backends or copies.

Every prefix passed to `list-by-prefix` in this specification ends in `/`
(e.g. `<table>/region=eu/`), so that `region=eu` never matches `region=eu2`
and `version=1` never matches `version=10`.

Per-backend mapping:

| operation            | local filesystem                          | S3                                            | GCS                                     |
|----------------------|-------------------------------------------|-----------------------------------------------|-------------------------------------------|
| `put`                | write to temp file, then `rename`         | single or multipart `PUT`                    | object write (new generation)             |
| `get`               | read; validator from `stat`               | `GET` + `ETag`                               | `GET` + `generation`                      |
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
internally), and never requires **conditional delete** (S3 lacks it; every
deletion in this layout is either guarded by a held claim (§5, §10) or is
idempotent housekeeping).

### 2. Top-level layout

```
GRV_DIR/
└── datasets/
    └── <dataset>/
        ├── <table>/              # one folder per table (§3)
        ├── .retired              # optional; the dataset is retired (§10)
        ├── .runs/                # run files (§6)
        │   └── <run-id>.json
        └── .states/
            ├── .claim            # dataset claim: publish steps and GC passes (§8, §10)
            ├── LATEST            # current revision number (§8)
            └── revisions/
                └── revision={n}/
                    ├── data.parquet   # the revision (§7)
                    └── .keep          # optional pin (§10)
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
  `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`. The leading-alphanumeric requirement
  means no name can start with a dot, so table folders can never collide
  with `.runs/`, `.states/`, or any `.claim` / `.keep` / `.pruned` /
  `.retired` marker.
- Names and values are case-sensitive, but on **every** backend sibling
  names — datasets, tables, and the values of one partition key under the
  same parent — MUST NOT differ only by case. This keeps every layout
  portable to case-insensitive local filesystems, so a directory valid on
  S3 is valid everywhere. Writers reject such a collision.
- `partition_keys` is the discriminator between the two cases: empty means
  case a, non-empty means case b. Keys appear in the declared order in the
  path, and entries are unique.
- The path segment `key1=value1/key2=value2` is the **partition**. In case
  b, `version={n}` directories and `.claim` appear only at full partition
  depth; no intermediate directory holds versions.
- Partition **key names** and **values** must both match
  `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`. Writers validate on write (layout,
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
  (`manifest.json`, run files). The two encodings carry the same
  information; tools must treat them as one identifier.

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
  version's data is the union of its files' rows in any order. The
  manifest's `data_files` array names exactly the files that exist, in the
  order `data.parquet`, `data-1.parquet`, `data-2.parquet`, … (numeric by
  index, not lexicographic), each with its SHA-256, so a consumer can
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
      "table": "orders",
      "partition": { "region": "eu", "year": "2025" }
    },
    {
      "dataset": "ref_data",
      "revision": 2,
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
entry if it exists and either (a) its current validator (`head`) equals the
recorded `validator`, or (b) its SHA-256 equals the recorded `sha256`.
Check (a) is a metadata read and is the normal path; check (b) reads the
whole file and is the fallback when validators differ legitimately (the
directory was copied or restored to another bucket or backend) and for
full audits. A version *verifies* when every listed data file verifies and
no other `data*` file is present.

The manifest is also part of the **fence** against stale writers:
`claim_token` ties the version to the claim that allocated it, and the run
file must list the version with the same token (§5, §6).

**Schema evolution.** A new version of a (table, partition) may **add**
columns relative to its predecessor — the backward-compatible case — and
added columns are appended after the existing ones. Any other change to the
column schema (removing, renaming, re-typing, or reordering columns) is not
an evolution of the table: it is a **new table**. Changing the partition
keys is likewise a new table, since the `_{key}_` columns are part of the
layout, not the data. The rule extends across a state: the column lists of
all versions of one table in one revision must be **prefix-compatible**
(for any two, one is a prefix of the other, with identical types). The
table's schema in that state is the longest list; consumers fill trailing
columns missing from a version with nulls. Because older schemas are
prefixes of newer ones, reselecting an older version (a rollback) remains
legal. Writers check a new version against the highest-numbered existing
version of its (table, partition); the publisher enforces
prefix-compatibility across the state (§7).

### 5. Version allocation: `.claim`

Publishing a new version of a (table, partition) requires allocating the
next version number. Allocation is mutually excluded by a **claim** file in
the directory that contains the `version={n}` directories for that
(table, partition):

- non-partitioned table: `<table>/.claim`
- partition: `<table>/key1=v1/key2=v2/.claim`

One claim at a time per (table, partition); a claim covers exactly one new
version. A run publishing several new versions of the same (table,
partition) repeats the cycle below. The same mechanism also serializes
**dataset-level** operations: the publish step (§8) and GC passes (§10)
both hold `<dataset>/.states/.claim`, so they never run concurrently for one
dataset.

`.claim` schema — the file's content changes as it moves through the
lifecycle below; **every transition is a `conditional-put` against the
validator of the claim content the writer last read or wrote**; the holder
recognizes its own claim by its `token`:

| field         | type    | present in         | notes                                     |
|---------------|---------|--------------------|-------------------------------------------|
| `holder`      | string  | all states         | a run ULID (§6), or `publish:<ULID>` / `gc:<ULID>` for a publish step or GC pass |
| `token`       | string  | all states         | opaque random string (e.g. UUID); identifies this claim |
| `version`     | integer | allocated, released| the reserved number (version or revision)  |
| `claimed_at`  | string  | acquired, allocated| RFC 3339 UTC, when the claim was taken     |
| `expires_at`  | string  | acquired, allocated| RFC 3339 UTC, after which the claim is stale; advanced by renewal |
| `released_at` | string  | released           | RFC 3339 UTC, when the holder finished     |

Protocol:

1. **Acquire.** `conditional-create` `.claim` with `holder`, `token`,
   `claimed_at`, `expires_at`. If the create fails, `get` the existing
   claim: if it is unexpired and held by another, abort (or wait and
   retry, at the caller's discretion); if it is expired, or is a release
   record, take it over with a `conditional-put` against the validator just
   read, writing a new `holder`, `token`, and timers. On takeover, remember
   the previous content's `version`, if any, as `prior`.
2. **Allocate.** The holder lists the `version={n}/` prefixes (delimited
   listing), takes `n = max(highest listed, prior, 0) + 1`, and rewrites
   `.claim` (`conditional-put`) adding `version`. Including `prior` means a
   number allocated by a taken-over claim is never reallocated, even if its
   holder had not yet written any object.
3. **Write data.** Create each of `version={n}`'s data files via
   `conditional-create`, recording each returned validator. A failed create
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
   `holder`, `token`, `version`, `released_at`. A version whose release
   succeeded is **finalized**; the run lists it in its run file (§6). The
   claim file is **never deleted**: a release record is the durable
   allocation record for the last version, and the next acquirer takes it
   over.
7. **Abandon.** Release the claim as in step 6 (so the number stays
   allocated) but do not list the version in the run file. Its objects are
   left for GC (§10).

**Renewal.** A holder may renew at any time, and MUST renew at intervals
shorter than its TTL while it works; `expires_at` must exceed the interval
between renewals plus the maximum clock skew between writers. A failed
renewal, release, or other claim CAS means the claim was taken over: the
holder is **stale** and MUST stop writing objects for that version at once;
the version is not finalized.

**Fence.** A version is **publishable** — may be referenced by a revision
(§7) — iff all of:

- its `manifest.json` exists and matches its path (`table`, `partition`,
  `version`);
- no `.pruned` exists in its directory;
- it verifies (§4);
- its run file (§6) exists and lists (table, partition, version) with the
  manifest's `claim_token`;
- its `derived_from` references resolve (§9).

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
<dataset>/.runs/<run-id>.json
```

`run_id` is a **ULID**: 26 characters of Crockford base32
(`[0-9A-HJKMNP-TV-Z]`, uppercase). Run ids are unique across all
datasets. ULIDs embed a 48-bit timestamp, so lexicographic order follows
timestamp order; no stronger cross-writer chronological guarantee is made
(same-timestamp ties and clock skew).

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

The run file is written via `conditional-create` **after** all of the
run's versions are finalized, and is the run's own commit marker: it lists
exactly the versions the run finalized (abandoned and stale versions are
omitted) and is never modified afterwards. A version whose `run_id` names
a run with no run file (a crash between release and the run file) is
**orphaned** and is not publishable.

**Recovering a crashed run.** A recovery tool acting for the run may write
its run file, but only once every claim held by the run is released or
expired, so it cannot race a run that is still alive. It lists exactly the
versions it can prove finalized:

- a version whose (table, partition) claim still holds the run's release
  record for that `version` and `token`; or
- a version whose claim is still the run's expired, allocated claim for
  that `version` and whose manifest carries that `token`: the recovery tool
  performs steps 5–6 of §5 on the run's behalf, and lists it if the
  release succeeds.

Versions whose claim has since been taken over by another holder cannot be
proven finalized and stay orphaned. The recovery tool writes the run file
with `conditional-create`; if a run file already exists, it leaves it.

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
each table are prefix-compatible (§4).

**Resolvability** is a separate, time-dependent property: a revision is
resolvable while every version it references is still present and
verifies. Validity never changes after the write; resolvability is lost
when GC prunes a version (§10). A valid but unresolvable revision is not a
protocol violation — it is the expected result of pruning.

The `revision={n}` path segment deliberately reuses the same `key=value`
path convention as partitions and versions.

### 8. Publish step and `LATEST`

A dataset's **publish step** is what advances its state. It is distinct
from a run: a run publishes version directories (§6); a publish step
selects versions, writes the next revision, and advances `LATEST`.

The publish step:

1. **Acquire** the dataset claim `<dataset>/.states/.claim` (same
   mechanism as §5, including renewal; on takeover remember `prior`). The
   claim is shared with GC passes (§10): a publish and a GC pass never hold
   it at the same time.
2. **Read** `LATEST` with `get`, keeping its validator (or noting that it
   is absent). **Compute the next state**: start from the `LATEST` state
   (or the empty state if `LATEST` is absent) and apply the selected
   changes: new versions replace the (table, partition) entry, and tables
   or partitions may be dropped by omission (§7).
3. **Allocate** the revision number: `max(LATEST, highest existing
   revision number, prior) + 1` — allocation is above *all* existing
   revision objects and any number reserved by a taken-over claim, so a
   crashed publisher's orphan revision (written but never pointed at) is
   never overwritten, and gaps in the numbering are legal. Record the
   number in the claim.
4. **Validate** the next state as a revision (§7): in particular every
   selected version is publishable (§5).
5. **Renew** the dataset claim, then **write** `revision={n}/data.parquet`
   (§7) via `conditional-create` (the number is fresh by construction; a
   failed create means a collision — abort and re-allocate), with
   `grv.previous_revision` = the `LATEST` read in step 2 (`0` if absent).
6. **Advance** `LATEST` to `n` with a `conditional-put` against the
   validator read in step 2 (`conditional-create` if `LATEST` was absent).
   If it fails, `LATEST` moved under a stale publisher: abort; revision `n`
   is an orphan, which is legal.
7. **Release** the dataset claim.

Step 6's compare-and-swap is what keeps `LATEST` monotonic: a publisher
whose claim expired mid-step cannot move `LATEST` back over a revision a
later publisher already advanced to, and every revision `LATEST` ever
names has, as its `previous_revision`, the `LATEST` it replaced.

`<dataset>/.states/LATEST` is a single-line text file containing the number
of the latest agreed revision. It is the one pointer readers use to
resolve the current state of a dataset: read `LATEST` → read
`revision={n}/data.parquet` → for each (table, partition) in the state,
read the referenced version's `manifest.json` → read the data files it
lists.

Ordering matters: the revision parquet is written before `LATEST` is
updated, so `LATEST` never points at a missing revision. A crash between
steps 5 and 6 leaves `LATEST` unchanged and valid, and revision `n` an
orphan. A `LATEST` that nevertheless names a missing, unreadable, or
invalid revision can arise only from corruption or an out-of-contract
writer, and is a protocol violation; recovery is a manual operation —
inspect the revision objects and, if necessary, restore `LATEST` to the
highest valid revision below it. No reader-side fallback is defined: a
reader of a corrupt `LATEST` reports the dataset as unavailable. (A
`LATEST` that is valid but no longer resolvable — e.g. of a retired
dataset, §10 — is not a violation; readers report the pruned versions as
unavailable.)

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
revision's state with a version that is present and not pruned. The
publisher checks this at publish (§5 fence), so a version whose upstream
state was pruned before it was published is rejected rather than released
with broken provenance. Since a referenced revision must exist before the
version that references it is published, validated derivation links always
point backward in time.

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

GC is configured with a **pending grace** period, `pending_grace`, which
must exceed the worst-case time from a run reading its inputs to the
publish step that includes its versions. It covers the windows in which a
version or revision is needed but not yet referenced by anything kept.

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
  from `LATEST`, and the revision that followed it on that chain has
  `grv.created_at` less than `pending_grace` ago (so a run that read it as
  `LATEST` shortly before it was superseded can still publish);
- it has a `.keep`;
- it is named by any element of the `derived_from` of a protected version
  — the closure walks *backward* across datasets, following derivation
  links.

A version is protected if any of:

- its directory has a `.keep`;
- its (table, partition) directory or its table directory has a `.keep`;
- it appears in the state of a revision in the keep set of its own dataset;
- it is pending, and it either has no `manifest.json` yet or its
  manifest's `created_at` is less than `pending_grace` ago.

Because `derived_from` of every protected version feeds the keep set, a
directly kept derived version, a version of a kept product state, and a
pending derived version all retain their upstream revisions. References
to revisions that do not exist are reported and ignored.

The closure is a walk over (dataset, revision) nodes. Validated
derivation links point backward in time (§9), so a cycle cannot arise from
conforming writers; if one is found anyway, the walk reports it and GC
aborts the affected deletions rather than proceeding on an incomplete
closure. A node reached twice through different paths is not a cycle.

**Retirement.** A dataset that is fully superseded — no longer a product,
and no longer needed by any kept state — may be marked
`<dataset>/.retired` (same presence-is-signal convention; an optional JSON
body may record `retired_by`, `retired_at`, `reason`). While the marker is
present, the dataset's `LATEST` is *not* in the keep set: the dataset is
retained only to the extent that kept revisions — its own, or other
datasets' via the `derived_from` closure — still reference it, and may
otherwise be pruned to nothing but its (small) revision records. Its
`LATEST` then remains valid but may become unresolvable (§7). Retirement
is terminal: no new runs or publish steps are made against a retired
dataset.

The third protection rule is deliberately coarse: a kept revision of an
upstream dataset protects *its entire state* — every (table, partition,
version) it names — even though a downstream product may have used only a
few of them. Provenance dependencies are revision-wide, not
selector-narrow.

**GC coordination.** A GC pass that prunes dataset `D` holds `D`'s dataset
claim `<D>/.states/.claim` for its entire duration (same mechanism as §5,
including renewal; the holder is a GC pass). While it holds the claim, no
publish step of `D` can run, so `D`'s own revisions cannot change
mid-pass. Other datasets can still publish concurrently; the
recently-superseded and pending rules cover their in-flight needs as long
as `pending_grace` holds, and the publisher's `derived_from` check (§9)
rejects a version whose upstream was pruned anyway. GC re-checks each
version's protection immediately before deleting its objects (a `.keep` may
have appeared); a version that is protected at the moment of GC's re-check
is not deleted. A writer that adds a `.keep` to a version should check
afterwards that no `.pruned` exists; if one does, the version was already
lost.

**Pruning.** GC may prune any unprotected version. Pruning is
**tombstone-first**: GC writes the `.pruned` file, then deletes the
version's data files, then `manifest.json`. All steps are idempotent, so a
crashed pass is completed by re-running GC:

```json
{
  "pruned_by": "grv-gc/1.2.0",
  "pruned_at": "2026-12-01T00:00:00Z",
  "table": "orders",
  "partition": { "region": "eu", "year": "2025" },
  "version": 3
}
```

A version directory containing `.pruned` is a tombstone: readers report the
version as unavailable, and it is never publishable (§5), regardless of
which of its other objects still exist. `.pruned` is never deleted, so a
pruned number stays visible to allocation (§5).

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
  pruned to nothing but its revision records.
- Finalized versions that are never published become prunable once
  `pending_grace` has elapsed since their manifest was written, or once a
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
or a sync that runs to completion before the run starts).

**Sync path (GRV → warehouse).** For an engine to consume a GRV dataset as
source, a sync job materializes the dataset's current state (the `LATEST`
revision's entries, §7) into warehouse tables: one table per GRV table,
with the `_{key}_` partition columns as ordinary columns — the hive-style
duplication is what makes a flat warehouse table sufficient — and the
table's state schema (§4), with missing trailing columns filled with nulls.
The job diffs the current `LATEST` state against its last-synced
revision's state and refreshes only the (table, partition) entries that
changed, were added, or were removed; a partition that is absent from the
new state, or whose current version is empty (§4), is truncated in the
warehouse table, and a table absent from the new state is emptied (not
dropped, since it may re-enter a later state). The job advances its
watermark only after every entry of the new state has been applied; if a
version turns out unavailable mid-sync (a newer `LATEST` was published and
the old state pruned), it restarts against the then-current `LATEST`.
Where the warehouse allows, each table's refresh should be applied
atomically (staging table and swap) so engines never read a mix of states.
The sync watermark (last-synced revision per dataset) is *consumer* state
and deliberately lives outside the layout.

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
  partition, so cost per publish is proportional to partition size — keep
  partition granularity at or coarser than batch granularity; a table
  without a natural time partition pays full-table snapshot cost per
  publish, acceptable for small or infrequently published tables only.
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
  — and per-dataset serialization of publish steps and GC passes — with no
  central coordinator: every claim transition is a compare-and-swap, a
  stale holder fails its next renewal or release and never gets a version
  into a run file, and `LATEST` only moves by compare-and-swap.
- Each revision is a self-contained snapshot of the dataset's state (a
  possibly empty subset of its tables); `LATEST` is one small pointer, so
  resolving the current state is a single read.
- The `_{key}_` column duplication makes each data file self-contained —
  any single file of a version can be copied, opened, or processed by a
  single-file reader without needing its containing path; a version's data
  is the union of its files' rows, and the manifest's per-file hashes and
  validators make the set cheaply verifiable before combining.
- Full provenance: version → run → entries and inputs, and version →
  `derived_from` → (other dataset, revision), checked at publish.
- GC protection is declarative and cross-dataset: `.keep` markers plus the
  keep-set closure over `derived_from` protect; pending versions and
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
- Cross-dataset GC safety is time-based: it holds only if `pending_grace`
  exceeds the worst-case run-to-publish latency. When the bound is broken,
  the publisher's `derived_from` check rejects the affected version
  instead of releasing it with broken provenance — a liveness cost, not a
  safety one.
- Verification normally compares backend validators, not content: it is a
  metadata read, but it trusts the backend to change the validator on
  every rewrite. A full SHA-256 check is the fallback after copies and
  for audits.
- Revisions are full snapshots: resolution is a single read, but each
  revision is O(state size), so revision-history storage grows with state
  size times revision count; compaction is a separate concern (below).
- Every version allocation lists all historical `version={n}/` prefixes of
  a (table, partition), including pruned tombstones; a long-lived
  (table, partition) grows this listing linearly. Bounding it (allocation
  counters, prefix compaction) is part of the compaction concern.
- Pruning a version makes historical states that needed it unresolvable;
  §10 defines the mechanics, while retention policy (how far back a
  dataset keeps) is a separate concern (below).
- A run file is written only after all of a run's versions are finalized,
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
