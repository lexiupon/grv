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

**Backend contract.** The layout requires six operations:

| operation            | meaning                                                             |
|----------------------|---------------------------------------------------------------------|
| `put`                | write an object; readers observe either the old or the new content, never a partial one |
| `get`                | read an object's content and its validator                          |
| `list-by-prefix`     | list object names under a prefix                                  |
| `delete`             | remove an object (idempotent)                                     |
| `conditional-create` | create only if the object does not exist; fail otherwise          |
| `conditional-put`    | replace only if the object's validator matches one read earlier (compare-and-swap) |

A *validator* is the backend's identity for an object's content: the S3
`ETag` (MD5 for normal uploads), the GCS object `generation`, or, locally,
the file stat (inode + mtime + size).

Per-backend mapping:

| operation            | local filesystem                          | S3                                            | GCS                                     |
|----------------------|-------------------------------------------|-----------------------------------------------|-------------------------------------------|
| `put`                | write to temp file, then `rename`         | single `PUT`                                 | object write (new generation)             |
| `get`               | read; validator from `stat`               | `GET` + `ETag`                               | `GET` + `generation`                      |
| `list-by-prefix`    | directory walk                            | `ListObjectsV2` with prefix                   | bucket list with prefix                   |
| `delete`            | `unlink`                                  | `DELETE`                                     | object delete                             |
| `conditional-create`| `open(O_CREAT\|O_EXCL)`                    | `PUT` with `x-amz-if-none-match: *`          | `PUT` with `if-generation: 0`             |
| `conditional-put`   | read-modify-write under a directory lock  | `PUT` with `if-match: <ETag>`                | `PUT` with `if-generation-match`          |

Two deliberate omissions: the layout never requires **rename** (object
stores lack it; publishing is made safe instead by the manifest-as-commit-
marker, §4), and never requires **conditional delete** (S3 lacks it; every
deletion in this layout is either guarded by a held claim (§5, §10) or is
idempotent housekeeping).

### 2. Top-level layout

```
GRV_DIR/
└── datasets/
    └── <dataset>/
        ├── <table>/          # one folder per table
        ├── .runs/            # run metadata
        └── .states/          # revisions + LATEST
```

- The top-level folder is always `datasets/`; each dataset gets one folder
  under it.
- A dataset is a collection of tables; each table lives in its own folder
  directly under the dataset folder.
- `.runs/` and `.states/` are per-dataset metadata folders. The dot prefix
  distinguishes them from table folders.

The `datasets/` level is deliberate, not ceremony: `GRV_DIR` is a
*root*, and the layout reserves the right to place other top-level
structures there later (system state, GC staging, caches) without a
breaking change. It also keeps "list all datasets" a single prefix listing
on object stores, where a root shared with other tools' objects would make
every listing a filter, and it gives every dataset a uniform path
(`<root>/datasets/<name>/...`). Dropping the level now to save one path
segment would turn re-adding it into a data migration; keeping it costs one
segment.

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

- Dataset and table names must also match
  `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`. The leading-alphanumeric requirement
  means no name can start with a dot, so table folders can never collide
  with `.runs/`, `.states/`, or any `.claim` / `.keep` / `.pruned` /
  `.retired` marker. Names are case-sensitive; on a case-insensitive local
  filesystem, sibling names MUST NOT differ only by case.
- `partition_keys` is the discriminator between the two cases: empty means
  case a, non-empty means case b. Keys appear in the declared order in the
  path, and entries are unique.
- The path segment `key1=value1/key2=value2` is the **partition**.
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
  `conditional-create` and is **immutable** thereafter: a different
  `partition_keys` is a different table (§4).
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
  Versions are **immutable**: once published, a version directory is never
  modified.
- A version consists of one or more **data files** plus its
  `manifest.json`. The single-file form is `data.parquet`; for large
  partitions a version may additionally have `data-1.parquet`,
  `data-2.parquet`, ... (names: `data.parquet` or `data-<i>.parquet`,
  `<i>` a positive integer without leading zeros; no other `data*` files
  may appear in a version directory). The files are **join-safe**: each is a
  complete set of rows (no row is split across files), rows are disjoint
  across files, and the version's data is the union of its files' rows in
  any order. The manifest's `data_files` array names exactly the files
  that exist, each with its SHA-256, so a consumer can verify the set
  before joining.
- `manifest.json` is the **commit marker** of the version: a version
  directory is valid only when every data file listed in the manifest and
  the manifest itself are present. Writers upload the data files first and
  `manifest.json` last, so readers never observe a half-published version.
  A version whose data files are later lost (pruning, abandonment) is
  unreadable, and readers report it as such.

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
| `data_files`   | array   | yes      | the version's data files in name order; each element `{name, sha256}`; the fence |
| `row_count`    | integer | yes      | rows across all data files; 0 iff the version is empty |
| `derived_from` | array   | no       | source references; present iff derived (§9)   |
| `metadata`     | object  | no       | map of engine name → engine-specific fields; readers MUST ignore entries they do not understand (§11) |

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
    { "name": "data.parquet", "sha256": "…" },
    { "name": "data-1.parquet", "sha256": "…" }
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

The manifest is also the **fence** against stale writers: `claim_token`
and the `data_files` hashes let the publisher verify that a version was
written by the claim holder that allocated it, and that its data has not
been clobbered since (§5, §8).

Schema evolution: a new version of a (table, partition) may **add**
columns relative to its predecessor — the backward-compatible case. Any
other change to the column schema (removing, renaming, re-typing, or
reordering columns) is not an evolution of the table: it is a **new
table**. Changing the partition keys is likewise a new table, since the
`_{key}_` columns are part of the layout, not the data.

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
lifecycle below; **every transition is a `conditional-put` on `token`**:

| field         | type    | present in         | notes                                     |
|---------------|---------|--------------------|-------------------------------------------|
| `holder`      | string  | all states         | a run ULID (§6), a publisher, or a GC pass |
| `token`       | string  | all states         | opaque random string (e.g. UUID); the CAS key |
| `version`     | integer | allocated, released| the reserved number (version or revision)  |
| `claimed_at`  | string  | acquired, allocated| RFC 3339 UTC, when the claim was taken     |
| `expires_at`  | string  | acquired, allocated| RFC 3339 UTC, after which the claim is stale|
| `released_at` | string  | released           | RFC 3339 UTC, when the holder finished     |

Protocol:

1. **Acquire.** `conditional-create` `.claim` with `holder`, `token`,
   `claimed_at`, `expires_at`. If the create fails, read the existing
   claim: if it is unexpired and held by another, abort (or wait and
   retry, at the caller's discretion); if it is expired, or is a release
   record, take it over with a `conditional-put` on the token just read,
   writing a new `holder`, `token`, and timers.
2. **Allocate.** The holder lists the `version={n}` directories, takes
   `max + 1` (or `1` if none), and rewrites `.claim` (a
   `conditional-put` on its own token) adding `version`.
3. **Publish.** Write `version={n}`'s data files, then
   `version={n}/manifest.json` (§4), whose `claim_token` is this claim's
   `token` and whose `data_files` records each file's SHA-256.
4. **Confirm.** Re-read each of the version's data files to confirm they
   are still present and hash as recorded. If any is missing or changed,
   the version was clobbered or pruned mid-flight: abandon it (step 5) —
   it can never be published.
5. **Release.** Rewrite `.claim` (a `conditional-put` on its own token)
   into a release record: `holder`, `token`, `version`, `released_at`.
   The claim file is **never deleted**: a release record is the durable
   allocation record for the last version, and the next acquirer takes it
   over.

**Fence.** Because every claim transition is a compare-and-swap on the
token, a stale holder — one whose claim was taken over — fails its next CAS
and MUST abort the run without writing any version objects. A version is
fenced at its manifest: the publisher (§8) accepts a *new* version (one
not referenced by any kept revision) only if its manifest's `claim_token`
matches the token currently in the (table, partition) claim file and each
of its data files hashes as the manifest records. A version that was never
validly fenced can never enter a revision. A version that *was* fenced at
its first publication stays valid: when it is selected again (e.g. a
rollback to an older version), only the data hash is re-checked, since its
historical token is no longer in the claim file. The fence assumes
conforming writers — every version write is preceded by a successful claim
CAS; a client that writes version objects without holding the claim is out
of contract, and store-level access control, not the layout, is the
mitigation.

A crashed run's claim is recovered by step 1's takeover; `expires_at` must
exceed the worst-case publish duration for that (table, partition).

### 6. Runs

Adding new versions — for one table, a few tables, the partitions of one
table, or partitions across multiple tables — is a **run**. Each run has a
unique `run_id` and its metadata is stored at:

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
| `entries`    | array  | yes      | one entry per published version          |
| `metadata`   | object | no       | map of engine name → engine-specific fields (§11) |

Each entry: `table` (string), `partition` (object; `{}` for
non-partitioned), `version` (integer).

```json
{
  "run_id": "01J5K8W3N4R6Y8C2D9F0G1H2J3",
  "created_at": "2026-09-28T10:00:00Z",
  "entries": [
    { "table": "customers", "partition": {}, "version": 2 },
    { "table": "orders", "partition": { "region": "eu", "year": "2025" }, "version": 3 }
  ]
}
```

A run only *publishes* version directories (via the claim cycle, §5).
Versions enter a dataset's state only when a revision (§7) references
them; the publish step (§8) is what advances `LATEST`. The lifecycle is
therefore run → publish step → `LATEST`, in that order.

The run file is written **after** all of the run's manifests are committed,
and is the run's own commit marker: it lists exactly the versions the run
published and is never modified afterwards. A version whose `run_id` names
a run with no run file (a crash between the manifest and the run file) is
**orphaned**: the publisher rejects orphaned versions, and the publisher —
or a recovery tool acting for the run — may write the run file to finalize
the run. A version is publishable only if its manifest passes the fence
(§5) and its run file exists.

### 7. Revisions

The state of a dataset is described by a **revision**. A revision is a
**complete snapshot** of the dataset's state: one entry per (table,
partition) that is *in the state*, each with the current version of that
(table, partition) and the `run_id` that produced it. A state may contain
any subset of the dataset's tables.

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

Row schema — one row per (table, partition) in the state,
`previous_revision` constant across rows:

| column              | type   | notes                                                    |
|---------------------|--------|----------------------------------------------------------|
| `table`             | string | table name                                               |
| `partition`         | string | e.g. `key1=v1/key2=v2`; empty for non-partitioned       |
| `version`           | int64  | version of this (table, partition)                      |
| `run_id`            | string | run that produced the (table, partition, version)       |
| `previous_revision` | int64  | the revision this one was derived from; `0` for the first |

Validation: a revision is valid iff (a) each (table, partition) appears at
most once; (b) `previous_revision` is constant across rows; (c) every
referenced version's manifest exists and passes the fence (§5); (d) the
referenced tables exist in the dataset. A `LATEST` that names a missing or
invalid revision is a protocol violation (see §8).

The `revision={n}` path segment deliberately reuses the same `key=value`
path convention as partitions and versions.

### 8. Publish step and `LATEST`

A dataset's **publish step** is what advances its state. It is distinct
from a run: a run publishes version directories (§6); a publish step
selects versions, writes the next revision, and advances `LATEST`.

The publish step:

1. **Acquire** the dataset claim `<dataset>/.states/.claim` (same
   mechanism as §5). The claim is shared with GC passes (§10): a publish
   and a GC pass never hold it at the same time.
2. **Compute the next state.** Start from the current `LATEST` state (or
   the empty state if the dataset has no revisions yet) and apply the
   selected changes: new versions replace the (table, partition) entry,
   and tables or partitions may be dropped by omission (§7).
3. **Allocate** the revision number: `max(current LATEST, highest existing
   revision number) + 1` — allocation is above *all* existing revision
   objects, so a crashed publisher's orphan revision (written but never
   pointed at) is never overwritten, and gaps in the numbering are legal.
   Record the number in the claim.
4. **Validate** every selected version: its manifest exists, passes the
   fence (§5), and its run file exists (§6).
5. **Write** `revision={n}/data.parquet` (§7) via `conditional-create`
   (the number is fresh by construction; a failed create means a
   collision — abort and re-allocate), with
   `previous_revision = current LATEST`.
6. **Advance** `LATEST` to `n`.
7. **Release** the dataset claim.

`<dataset>/.states/LATEST` is a single-line text file containing the number
of the latest agreed revision. It is the one pointer readers use to
resolve the current state of a dataset: read `LATEST` → read
`revision={n}/data.parquet` → for each (table, partition) in the state,
read the referenced version's `data.parquet`.

Ordering matters: the revision parquet is written before `LATEST` is
updated, so `LATEST` never points at a missing revision. A `LATEST` that
nevertheless names a missing or invalid revision (a crash between steps 5
and 6, or corruption) is a protocol violation; recovery is a manual
operation — inspect the revision objects and, if necessary, restore
`LATEST` to the highest complete revision below it. No reader-side
fallback is defined: a reader of a corrupt `LATEST` reports the dataset as
unavailable.

### 9. Cross-dataset derivation

A version of a (table, partition) in one dataset may be **derived** from
one or more specific revisions of other datasets (e.g. a table that is a
function of tables in upstream datasets as of particular revisions of
each). The derivation is recorded in the `derived_from` array of the new
version's `manifest.json` (§4): one source reference per upstream
dependency — source dataset, source revision, source table, source
partition. This makes the provenance chain auditable from the data
directory alone.

### 10. Garbage collection: `.keep` and `.pruned`

The layout assumes a GC process that prunes old versions. Protection is
declarative, via marker files whose *presence* is the signal (an optional
JSON body may record `kept_by`, `kept_at`, `reason`):

- `<table>/.../version={n}/.keep` — this version must not be pruned.
- `<table>/<partition>/.keep` — or `<table>/.keep` for a
  non-partitioned table — **no version** of this (table, partition) may
  be pruned.
- `<dataset>/.states/revisions/revision={n}/.keep` — this revision is
  **pinned** (see the keep set below).
- `<dataset>/.retired` — the dataset is **retired**: its `LATEST` is no
  longer unconditionally kept (see the keep set below).

**Keep set.** A revision is in the keep set if any of:

- it is the current `LATEST` of its dataset, unless the dataset is
  **retired** (`.retired` marker present);
- it has a `.keep`;
- it is named by any element of the `derived_from` of a version appearing
  in the state of a revision already in the keep set — the closure walks
  *backward* across datasets, following derivation links, to a fixpoint;
- a version with a `.keep` whose `derived_from` is non-empty appears in its
  state: the revisions that `derived_from` names enter the keep set, so a
  directly kept derived version retains its upstream dependencies too.

The closure is a walk over (dataset, revision) nodes. A cycle in that
graph is a modeling error — the walk detects and reports it, and GC
aborts the affected deletions rather than proceeding on an incomplete
closure. A node reached twice through different paths is not a cycle.

**Retirement.** A dataset that is fully superseded — no longer a product,
and no longer needed by any kept state — may be marked
`<dataset>/.retired` (same presence-is-signal convention; an optional JSON
body may record `retired_by`, `retired_at`, `reason`). While the marker is
present, the dataset's `LATEST` is *not* in the keep set: the dataset is
retained only to the extent that kept revisions — its own, or other
datasets' via the `derived_from` closure — still reference it, and may
otherwise be pruned to nothing but its (small) revision records. Retirement
is terminal: no new runs or publish steps are made against a retired
dataset.

**Protected versions.** A version is protected if any of:

- its directory has a `.keep`;
- its (table, partition) directory has a `.keep`;
- it appears in the state of a revision in the keep set of its own dataset.

The third rule is what makes cross-dataset protection work, and it is
deliberately coarse: a kept revision of an upstream dataset protects *its
entire state* — every (table, partition, version) it names — even though a
downstream product may have used only a few of them. Provenance
dependencies are revision-wide, not selector-narrow.

**GC coordination.** A GC pass holds the dataset claim
`<dataset>/.states/.claim` for its entire duration (same mechanism as §5;
the holder is a GC pass). While it holds the claim, no publish step can
run, so the keep set computed after acquisition cannot change from
publication mid-pass. GC still re-checks each version's protection
immediately before deleting its objects, because a *run* (which holds only
a (table, partition) claim) may be concurrently writing a new,
not-yet-referenced version: if that version's objects disappear under it,
the run's confirm step (§5) fails and the run abandons the version. A
version that is protected at the moment of GC's re-check is not deleted.

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
version as unavailable, regardless of which of the other two objects still
exist.

**Reader semantics.** Reads are best-effort and point-in-time. A reader
resolves a state from the `LATEST` (or a named revision) it read; no pin
or lease is taken, and a concurrent GC pass may prune a version while the
read is in flight. A reader that encounters a `.pruned` tombstone reports
that version as unavailable; the read may be retried against the
then-current state.

**Final-product retention.** The intended workflow for data products:

1. Identify the datasets that are *final* products.
2. `.keep` the revisions of those datasets that back the product states
   that must remain resolvable.
3. Let the keep set close over `derived_from` — the backward walk marks
   the upstream revisions whose states the product was derived from.
4. Prune everything else: in every other dataset, all versions not
   protected as above. A (table, partition) with a `.keep` is passed over
   entirely. A non-final dataset that is no longer needed at all may be
   marked `.retired`, making even its `LATEST` state prunable.

**Consequences**

- Pruning a version referenced only by non-kept revisions makes those
  historical states unresolvable — that is the point of pruning (and reads
  of such states are best-effort, above).
- A dataset's `LATEST` state is protected unless the dataset is retired;
  a retired dataset no longer referenced by any kept revision may be
  pruned to nothing but its revision records.
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
engine invocation, or a selected subset of its units of work. The
publisher — a thin wrapper that invokes the engine, exports the changed
output to parquet, runs the §5 claim cycle, and runs the §8 publish step
— is what touches the layout; the engine itself only produces warehouse
tables. The run's `metadata` object (below) joins the GRV run back to the
engine's own run record. The publisher also fixes the provenance boundary:
it records in the run's `metadata` the exact source revision of each GRV
dataset read at the start of the engine invocation, and the export must be
stable — the warehouse tables exported are the ones produced from exactly
that state (e.g. a dedicated schema per invocation, or a sync that runs to
completion before the run starts).

**Sync path (GRV → warehouse).** For an engine to consume a GRV dataset as
source, a sync job materializes the dataset's current state (the `LATEST`
revision's entries, §7) into warehouse tables: one table per GRV table,
with the `_{key}_` partition columns as ordinary columns — the hive-style
duplication is what makes a flat warehouse table sufficient. The job diffs
the current `LATEST` state against its last-synced revision's state and
refreshes only the (table, partition) entries that changed, were added, or
were removed; a partition that is absent from the new state, or whose
current version is empty (§4), is truncated in the warehouse table. The
sync watermark (last-synced revision per dataset) is *consumer* state and
deliberately lives outside the layout.

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
`derived_from` answers *which upstream state* a version came from, and
`metadata` answers *which code* produced it.

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
  models — is one GRV run. `metadata.dbt` records the model, the dbt run
  id, and the dbt manifest hash, so a GRV run joins back to its dbt
  manifest.
- **Microbatch.** A dbt model that processes data in time batches
  (incremental / event-time selection) SHOULD carry the batch's time key as
  a partition key on its GRV table (e.g. `date=`); partition keys are
  fixed per table (§4), so the alignment is a design-time decision. A
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
  object stores without rename: write `data.parquet`, then `manifest.json`;
  readers never observe a half-published version.
- The claim protocol gives single-writer allocation per (table, partition)
  — and per-dataset serialization of publish steps and GC passes — with no
  central coordinator: every claim transition is a compare-and-swap, a
  stale holder aborts, and the manifest fence rejects a version whose
  writer no longer holds the claim.
- Each revision is a self-contained snapshot of the dataset's state (a
  possibly proper subset of its tables); `LATEST` is one small pointer, so
  resolving the current state is a single read.
- The `_{key}_` column duplication makes each data file self-contained —
  any single file of a version can be copied, opened, or processed by a
  single-file reader without needing its containing path; a version's data
  is the union of its files' rows, and the manifest's per-file hashes make
  the set verifiable before joining.
- Full provenance: version → run → entries, and version → `derived_from` →
  (other dataset, revision).
- GC protection is declarative and cross-dataset: `.keep` markers plus the
  keep-set closure over `derived_from` protect; everything else is
  prunable; a `.retired` dataset releases even its `LATEST` retention;
  pruning is tombstone-first; `.pruned` records who pruned and when.

**Trade-offs**

- On object stores, updating `LATEST` is not atomic with writing the
  revision parquet. Convention: write `revision={n}/data.parquet` first,
  then update `LATEST`, so `LATEST` never points at a missing revision. A
  `LATEST` that nonetheless names a missing or invalid revision is a
  protocol violation; recovery is manual (§8) — no reader-side fallback is
  defined.
- The claim protocol depends on the backend contract's
  `conditional-create`/`conditional-put` (§1), and a claim's
  `expires_at` must exceed the worst-case publish time; a crashed
  claimant's allocation is blocked until the TTL elapses.
- Revisions are full snapshots: resolution is a single read, but each
  revision is O(state size), so revision-history storage grows with state
  size times revision count; compaction is a separate concern (below).
- Every version allocation lists all historical `version={n}` prefixes of a
  (table, partition), including pruned tombstones; a long-lived
  (table, partition) grows this listing linearly. Bounding it (allocation
  counters, prefix compaction) is part of the compaction concern.
- Pruning a version makes historical states that needed it unresolvable;
  §10 defines the mechanics, while retention policy (how far back a
  dataset keeps) is a separate concern (below).
- The fence assumes conforming writers: every version write is preceded by
  a successful claim CAS. A non-conforming writer that rewrites an old
  version's data *and* manifest consistently is undetectable; store-level
  access control, not the layout, is the mitigation.

**Out of scope (separate concern)**

- **Retention policy** — how far back a dataset keeps (default keep-
  windows, per-dataset policy). §10 defines what *may* be pruned; choosing
  what *should* be kept is a separate concern.
- **Compaction** — the revision history (a full snapshot per revision)
  grows without bound, as do allocation listings; a compaction/checkpoint
  mechanism is a separate concern.
