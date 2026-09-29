# GRV Client (v1: `sync` + `publish`)

|         |            |
|---------|------------|
| Status  | draft      |
| Version | 1          |
| Date    | 2026-09-29 |
| Extends | GRV v2     |

## Relationship to the GRV spec

This document specifies the **GRV client**: a single command-line tool with two commands,
`grv sync` and `grv publish`, that move a dataset's state between GRV and a local
transformation engine in each direction. It is an **extension**, not a change: it defines no
new storage layout and no new state — it only defines how a *consumer* resolves, verifies,
materializes, and publishes state that the GRV spec already produces and accepts.

It is a **draft** and is intended to be **reviewed together with the GRV spec**. The GRV
constructs it depends on are itemized in [§ Dependencies](#dependencies-on-the-grv-spec) —
spanning the *read* constructs `sync` uses and the *write* constructs `publish` uses — so
joint review has a concrete checklist. Nothing here should be read as freezing those
constructs; where this document and the GRV spec disagree, the GRV spec wins and this one is
amended.

## Overview

GRV is the durable record: raw (ingest) and product (release) datasets, as versions and
revisions, on a backend (local / S3 / GCS). A transformation engine (dbt, among others) is
the **build layer** in between: it reads materialized state, runs models, and its results are
published back to GRV.

The **client** is the bridge on *both* sides of that build layer:

```text
GRV (raw) → [ grv sync ] → engine (DuckDB) → [ build ] → engine → [ grv publish ] → GRV (product)
```

- **`grv sync`** (GRV → engine): resolve a GRV dataset's state and materialize it into flat
  engine tables, so the engine consumes ordinary tables and never sees GRV's version/revision
  machinery.
- **`grv publish`** (engine → GRV): take the engine's built tables and write them back to GRV
  as new versions and a new revision, through the GRV write protocol.

Three properties hold for the client as a whole:

- **The engine stays version-blind.** An engine source is a flat table; the client is the only
  GRV-aware component. No model reaches into `version={n}/` paths or joins a revision.
- **The two commands are one bridge, two directions.** They share the interface, the
  declaration format, the Arrow standard, and the backend contract; only the arrow reverses.
- **It is generic and external.** One tool, parameterized per dataset by a small declaration.
  It is not owned by any single consumer; a consumer only *declares* and *invokes*.

## Scope

### In scope

- The shared client core: the interface, the two-layer declaration, the Arrow→engine adapter,
  and the backend contract.
- `grv sync`: resolve a dataset's current (`LATEST`) or a named revision; select the version
  per (table, partition); verify; materialize into the engine; record `sync_meta`.
- `grv publish`: read the engine's built tables; run the GRV write protocol (claim → versions
  → mint revision → `LATEST` CAS); record `derived_from`.

### Out of scope

- Transforming data (the engine's job).
- Populating raw datasets (the ingest layer's job).
- Garbage collection / retention (the GRV spec's concern; the client only *reads* it, and
  `sync` may optionally take a read-lease, [§ Backends](#backends-and-the-s3-view-optimization)).
- Other client operations (GC tooling, recovery, plain inspection / `ls`). The `grv` CLI is
  the durable home for such commands, but **this spec (v1) specifies only `sync` and
  `publish`**; the rest are future or separate.

## The shared core

### The interface

Both commands share a narrow waist — three explicit inputs, everything else derived. Only the
arrow reverses:

```console
grv sync     --decl=<yaml-or-dir> --grv=<GRV_DIR> --engine=<store> [--revision N] [--target <t>]
grv publish  --decl=<yaml-or-dir> --grv=<GRV_DIR> --engine=<store> [--target <t>]
```

- **`--grv`** — the GRV root: a local path, `s3://bucket/prefix`, or `gs://bucket/prefix`.
  Declarations name datasets *relative* to this root, so one variable repoints every
  declaration between a local and a remote GRV. (A per-declaration `source.root` override is
  the escape hatch for two roots in one run.) For `sync` this is the *source*; for `publish`
  it is the *target*.
- **`--decl`** — one declaration file, or a directory of them.
- **`--engine`** — the local engine store (e.g. a DuckDB database path). For `sync` it is
  the *target*; for `publish` it is the *source*.
- **`--revision N`** *(sync, optional)* — resolve revision `N` instead of `LATEST`.
- **`--target`** *(optional)* — override the destination for this run.
- **Backend credentials** — environment only (standard `AWS_*` / `GOOGLE_*`), required iff a
  remote backend is involved. Never hardcoded.

### The declaration: two layers

One declaration file per dataset. It has **two layers** — do not conflate them:

1. **The type schema is Apache Arrow** — a real, external standard. Arrow is chosen because
   it is *already* the type vocabulary of both ends: the GRV data (Parquet) is Arrow-backed,
   and DuckDB reads/writes Arrow natively. So a schema expressed as a `pyarrow.Schema` needs
   almost no translation to *validate* or to *generate*, and it is what makes the schema
   portable to other engines.
2. **The declaration envelope is this client's convention** — around the Arrow schema, the
   consumer says *where it comes from, where it goes, and what to check or select*. This layer
   is not a universal standard; it is the config schema the client accepts. Each command's
   envelope is shown in its section.

The Arrow schema may be **inlined** or **referenced** as a serialized artifact
(`schema: { file: schemas/<name>.arrow }` / `.ipc`); both are the same standard.

### The type mapping (Arrow → engine)

The mapping is a **per-engine adapter inside the tool**; the Arrow schema is the portable
part. Reference mapping for the DuckDB adapter:

| Arrow        | DuckDB      |
|--------------|-------------|
| `date32`     | `DATE`      |
| `int64`      | `BIGINT`    |
| `double`     | `DOUBLE`    |
| `utf8`       | `VARCHAR`   |
| `decimal128` | `DECIMAL`   |
| `timestamp`  | `TIMESTAMP` |
| `bool`       | `BOOLEAN`   |

"Other storage" is the same Arrow schema realized by a **different adapter** (Postgres,
Snowflake, …). The seam is the Arrow schema; the dialect is swappable. Build the DuckDB
adapter first; keep the seam, and do not build the others speculatively.

## `grv sync`

`grv sync` materializes a GRV dataset's state into flat engine tables. It is the *reader* side
of the bridge: safe to re-run, and the one command that must never hand the engine a partial
state.

### The sync declaration

```yaml
# <decl>/mt_month_stats.yml
dataset: mt_month_stats
source:
  grv: dwh.mt_month_stats        # dataset name under --grv (or a full path/URI)
target:
  engine: duckdb                  # which adapter realizes it
  table: raw.mt_month_stats       # the flat table the engine reads
  mode: local                     # local | s3-view
tables:
  - name: mt_month_stats
    schema:                       # ← the Arrow layer (shown as a readable projection)
      - { name: period,     type: date32 }
      - { name: impressions, type: int64 }
      - { name: clicks,     type: int64 }
      - { name: spend,      type: decimal(18,4) }
    partition_keys: [period]      # contract: must equal the GRV .layout.json
checks:
  - not_null: [period, impressions, clicks, spend]
```

`partition_keys` is a *contract assertion* — the authoritative keys come from the GRV
`.layout.json`; a mismatch is a validation failure.

### The sync algorithm

Four steps. "Which revision" is its own isolated step, which is what keeps the command generic
and makes pinned syncs a parameter rather than a fork.

```text
1. RESOLVE
   read LATEST (or --revision N)  →  revision={n}/data.parquet
   → the (table, partition, version) list for that state
   (a named revision skips the LATEST read entirely → fully deterministic)

2. VERIFY
   (a) integrity: for each selected version, the data file is present and its
       SHA-256 matches the manifest  →  pruned/corrupt ⇒ ABORT (do not materialize)
   (b) contract:  the columns conform to the declaration's Arrow schema
                  (and partition_keys == .layout.json)  ⇒  mismatch ⇒ ABORT

3. MATERIALIZE
   local:   load the selected versions' rows into the target table
             (_{key}_ columns flattened to ordinary columns)
   s3-view: register a view over the resolved S3 URIs (no byte copy)

4. RECORD
   write sync_meta (dataset, target_table, grv_revision, synced_at, …)
   — last, and only on success: it is the commit marker for the materialization
```

**Why RECORD is last.** Writing `sync_meta` is the commit marker for the local
materialization — the same role `manifest.json` plays for a GRV version. A reader of the
target table sees either the previous complete state or the new one, never a half-updated
table, and `sync_meta` lands only when VERIFY + MATERIALIZE both succeeded.

**Idempotency / diff.** The sync is re-runnable. In `local` mode the simplest correct
behavior is a full reload of the target table from the resolved revision (drop + recreate);
an incremental diff — refresh only the (table, partition) entries that changed, were added,
or were removed, truncating the rest — is an optimization that reads the previous
`sync_meta.grv_revision` as its baseline. A partition *absent* from the resolved revision, or
whose current version is empty, is truncated (the engine sees it as gone).

### `sync_meta`

The sync keeps a small table of consumer state — **in the target engine, not in GRV** (it is
a consumer's view, deliberately outside the layout). Reference shape (DuckDB):

```sql
_grv.sync_meta(
  dataset        VARCHAR,   -- GRV dataset (source)
  target_table   VARCHAR,   -- the engine table it was materialized into
  grv_revision   BIGINT,    -- which revision this materialization came from
  synced_at      TIMESTAMP,
  -- optional, for full provenance: one row per (partition, version)
)
```

Two roles, one record:

- **Provenance anchor (load-bearing).** `grv_revision` is the raw-side half of the
  `derived_from` chain `publish` records. It must be captured **at materialization time**,
  not read later: `LATEST` can advance between the sync and the publish, so "which revision
  the build actually used" is only knowable if it was pinned when the table was written.
- **Diff baseline (optional).** The same `grv_revision` is the last-synced marker an
  incremental diff compares against.

Minimal is one row per materialized table with `grv_revision`. Store the per-partition
detail too only if you want `derived_from` to remain expandable after the source revision is
GC-pruned.

### Modes

- **Tracking (default).** No `--revision`: resolve `LATEST`, materialize into the declared
  `target.table`, advance that table's `sync_meta`. This is "the current raw" the engine
  reads for a normal build.
- **Pinned.** `--revision N [--target <t>]`: materialize that historical state. It writes to
  a **distinct target** (e.g. `raw.mt_month_stats@5`) and does **not** move the tracking
  watermark — pinning is not re-tracking. Pointing a pin at the same table as a tracking
  target overwrites the current state; the tool warns. A pinned run is more deterministic
  than a tracking run (it never reads the live `LATEST` pointer).

Pinned syncs are what make `derived_from` a *capability* — reproducing a product against the
exact raw revisions it was built from, backfilling "as-of <date>", or bisecting a regression
to a specific raw publish.

### The completeness guarantee

This is the property the sync exists to give the build layer. For a dataset at a resolved
revision `R`:

- The sync materializes the **complete** state of `R` — one version per (table, partition)
  that `R` names.
- A (table, partition) **absent** from `R` is *deliberately dropped* (the upstream revision
  omitted it); the sync truncates it, and the engine correctly sees it as gone.
- A (table, partition) **named by `R`** whose version is pruned or corrupt **aborts the
  sync** — the engine is never handed a partial state.

Consequently a downstream rule that drops "a partition absent from the current raw" is sound:
"absent" is an authoritative, revision-level fact, not an artifact of an incomplete local
cache. The failure mode this prevents is a build deleting history because its local raw
happened to be partial.

### Backends and the S3-view optimization

The sync speaks the GRV **backend contract** (`get` / `list-by-prefix` / …), so it is
backend-agnostic: the same code reads a local path, S3, or GCS.

- **`mode: local`** copies the selected versions' bytes into the engine. One download, then
  cheap local reads; the build is offline and the state is frozen at sync time.
- **`mode: s3-view`** registers a view over the resolved S3 URIs — **no byte copy**. The
  trade: the engine reads S3 live, so (a) each query re-fetches (cost/latency, no offline
  build), and (b) a concurrent GC pass can prune a version mid-build (the GRV spec defines
  reads as best-effort). The byte-level integrity check is weaker here (you cannot hash what
  you do not read).

**Read-lease (optional).** The GRV spec defines no reader lease: a pin is created only
by a pin operation under the owning dataset's `LATEST` lease, and creating a pin record
directly is not a valid pin protocol. A consumer that has no write access to the source
(the §9 permission model: read, plus create in its own `.holds/` subfolder) therefore
cannot pin the versions it reads, and the no-copy trade above stands as documented: a
concurrent GC pass may prune a version mid-build. A consumer that *does* hold write access
to the source may pin the versions it uses (and unpin them after the build) through the
pin operation, which fences the GC for the build's duration; whether to make that the
default for `s3-view` is an open question below.

## `grv publish`

`grv publish` is the *writer* side of the bridge, and the heavier of the two: it mutates a
shared, monotonic GRV state, so the safety machinery (claim, fence, `LATEST` CAS) lives here.
It reads the engine's built tables and writes them back to GRV as new versions and a new
revision.

### The publish declaration

```yaml
# <decl>/mt.yml
dataset: mt                       # the product GRV dataset to publish into
source:
  engine: duckdb                  # where the built tables live
  tables:
    - { table: fct_x, grv_table: fct_x }   # engine table → GRV table
    - { table: dim_y, grv_table: dim_y }
target:
  grv: mt                          # created on first publish (layout is create-if-absent)
selection:                         # ← the one thing sync does not have
  policy: changed                  # changed | all | explicit
  drop: []                         # (table, partition) omitted from the new revision
  hold: []                         # tables kept at their current version
schema:
  - { name: fct_x, columns: [ … ], partition_keys: [period] }
  - { name: dim_y, columns: [ … ] }
derived_from: auto                 # filled from sync_meta (the raw revisions the build used)
```

### The publish algorithm

Six steps; the write protocol is the mirror of the sync's, but it *commits to GRV* rather
than to a local table.

```text
1. READ
   read the engine's built tables (source.tables) + their schemas
   (the engine is a read-only source; the client never invokes it)

2. PLAN
   per (grv_table):
   - dedup:     new content hash == current version hash ⇒ no new version (no-op)
   - selection: which (table, partition) enter the new revision (policy / drop / hold)
   - evolve:    a schema change is add-columns only, else it is a NEW table (abort)
   - fingerprint: one code fingerprint per table

3. CLAIM
   per (table, partition): the .claim cycle (acquire → allocate version)
   + the dataset lease in LATEST for the publish step (serializes it)

4. WRITE
   each version's data files, then manifest.json LAST (the commit marker);
   the manifest carries claim_token (the fence) + per-file sha256 + derived_from

5. MINT
   write revision={n}/data.parquet (one row per (table, partition) in the new state),
   then compare-and-swap LATEST to n  (the CAS is the commit point)

6. RECORD
   derived_from in each new version's manifest ← the raw revisions from sync_meta
```

### The safety machinery

This is the part that has no sync counterpart, because `publish` is the only command that
writes a shared, monotonic state:

- **The fence.** A version is accepted only if its `manifest.json` `claim_token` matches the
  claim that allocated it and each data file hashes as the manifest records. A stale or
  clobbered writer fails the CAS and is rejected.
- **Manifest-last.** Data files are written first, `manifest.json` last, so a reader never
  sees a half-published version — the same commit-marker rule as a raw version.
- **`LATEST` CAS.** The revision parquet is written before `LATEST` is advanced, so `LATEST`
  never points at a missing revision; the compare-and-swap is the single commit point.
- **Dataset lease.** The publish step holds the dataset lease in `<dataset>/.states/LATEST`
  for its duration, so a publish and a GC pass never run concurrently for one dataset.

The contrast with `sync` is the whole point: `sync` only writes local engine tables, so a
failed run is free to re-run; `publish` mutates state others share, so a lost race or a
clobbered version can corrupt a dataset. The machinery is the price of that.

### Selection policy

- **`policy: changed | all | explicit`** — what enters the new revision. `changed` is a
  *GRV-side* comparison (new content hash ≠ the target table's current version hash), so the
  engine need not report changes and the engine assumption stays thin.
- **`drop`** — omit a (table, partition) from the new revision; it leaves the state (its
  folder and versions remain, and a later revision may re-include it).
- **`hold`** — keep a table at its current version (no bump).
- **Dedup** — if the new content's hash equals the current version's hash, no new version is
  minted; a run that mints no new units mints no revision and leaves `LATEST` unchanged.
- **Tombstones** — an *empty version* (present, zero rows) versus *omission* (absent) are both
  expressible; a consumer materializing the state treats both as "no rows for this
  partition."

### Schema evolution

- A new version may **add** columns (the backward-compatible case). Any other change —
  removing, renaming, re-typing, or reordering columns — is a **new table**, not an evolution;
  changing the partition keys is likewise a new table.
- **One code fingerprint per table** identifies the code that produced it.
- **No double-versioning.** The GRV revision history is the history of record; the engine's
  own history (e.g. dbt `--state`) is a build-layer construct, flattened into ordinary table
  data at publish.

### `derived_from`

Each new version's `manifest.json` records `derived_from` — the raw datasets and revisions
the build consumed — read from `sync_meta` (the raw revisions the sync materialized). This is
what makes a product auditable back to its raw inputs.

## The provenance coupling

`sync` and `publish` are independent in code but linked through the engine store:

```text
sync_meta (raw revision → local table)  →  [ build ]  →  derived_from (those raw revisions → product version)
```

- `sync` writes `sync_meta`: which GRV revision each local engine table came from.
- `publish` reads it to write `derived_from`: which raw revisions each product version was
  built from.

Because both ends live in this one spec, the handoff cannot drift the way it could across two
separate tool specs — the raw-side record and the product-side record are defined together.

## Dependencies on the GRV spec

The constructs this client consumes, listed for joint review. **If any of these change in the
GRV spec, this document must be re-reviewed.**

*Read* (used by `sync`):

- `LATEST` pointer and `revision={n}/data.parquet` (the state snapshot) — RESOLVE.
- Version directories `version={n}/data.parquet` + `manifest.json` (per-file SHA-256) —
  VERIFY.
- `.layout.json` (partition keys) — VERIFY (contract), MATERIALIZE (`_{key}_` columns).
- The **backend contract** (`get`, `list-by-prefix`, `conditional-create`) — Backends.
- **Empty versions** and **omission** semantics (empty version vs. dropped partition) —
  the completeness guarantee.

*Write* (used by `publish`):

- The **`.claim` cycle** (per (table, partition) and dataset-level) — CLAIM.
- **`manifest.json` as commit marker** + the `claim_token` fence — WRITE.
- The **`LATEST` compare-and-swap** — MINT.
- **`derived_from`** in the version manifest — RECORD.
- **Pins** (`.pins/` records created by the pin/unpin operations) — the optional
  read-lease (`sync`) and retention.

## Properties and trade-offs

### Properties

- One generic client for all datasets; the per-dataset part is a declaration, not code.
- The engine is version-blind; the client is the single GRV-aware seam on both sides.
- The two commands share a core (interface, declaration, Arrow, backend) written once.
- The Arrow schema is a real, portable standard; the dialect is a swappable adapter.
- Provenance is explicit and captured at the right time (`sync_meta` at sync, `derived_from`
  at publish).
- The completeness guarantee is intrinsic to `sync` materializing a full revision.

### Trade-offs

- The client is an **external dependency coupled to the (draft) GRV spec** — only as stable
  as the constructs in § Dependencies. The *publish* half is more tightly coupled: it must be
  a conforming GRV *writer* (claim / manifest / CAS), the part of the spec most likely to
  move.
- `s3-view` (`sync`) trades a frozen, offline, one-read materialization for no-copy; the GC
  race is the cost, and the read-lease is the mitigation.
- The declaration **envelope** is this client's convention, not a universal standard; only the
  Arrow layer is portable on its own.
- v1 is scoped to two commands; the CLI is the durable home, so future commands extend it
  rather than fork it.

## Open questions

- **Schema declaration form** — inline in the YAML vs. a referenced `.arrow`/`.ipc`
  artifact; and whether the *same* schema artifact is shared with the engine's own source
  contract (declare-once) to prevent drift.
- **Target ownership** — does a command create/own its target (the engine table for `sync`,
  the GRV dataset/table for `publish`), or must the consumer pre-create it; and where does a
  reserved meta schema live?
- **Read-lease default** — where the consumer holds write access to the source, is the
  pin-based lease opt-in per run, or on by default for `s3-view`?
- **Multi-dataset invocation** — one call = one dataset, or a manifest of many (and how
  failures in one dataset interact with the others in the batch)?
- **Publish selection default** — is `changed` the default policy; and can `publish`
  bootstrap a brand-new product dataset's layout on its first run?
