# S3 evolved and typed shapes — scope plan (planning only, not executed)

Status: **plan for review. No live run, no code change and no agent work has been
performed against this plan.**

## Question

Package 26 still lists evolved and typed shapes as unqualified for S3. Before
touching the harness or the cloud I wanted to establish, from the actual code and
from behaviour rather than assumption:

1. What the pinned read options actually do when the files in one read do not
   share one schema.
2. Whether the current manifest path can express an evolved fixture set at all,
   and therefore whether qualifying evolution here needs a design change rather
   than a new fixture.

## What the code says

- The manifest view is built with `hive_partitioning=false,
union_by_name=false` and one fixed projection — `id` plus `payload0..payload7` —
  applied to every file in the list.
- The Python side gates the same way: `aggregate_expectation` requires every file's
  columns to equal `SCHEMA` exactly and raises `fixture-schema-drift` otherwise,
  and `manifest_document` admits only the closed five-field file entry with no
  per-file schema field.
- `grv_native_verified_file_query` still admits one descriptor per call, so for any
  multi-file shape the expectation stays Python-computed and disclosed.

So on this path an evolved set is not merely untested: **it is refused by the
harness's own gates before anything reaches the cloud.** Nothing in production
source refuses it.

## What the pinned settings actually do (offline probe)

Local parquet files, local reads only, no cloud and no credential. The probe used
DuckDB **1.4.2**, so this is an indicator of the rule and **not** an attestation of
the pinned 1.5.6 build.

| case                                                         | result                            | expected             |
| ------------------------------------------------------------ | --------------------------------- | -------------------- |
| two files, same schema                                       | `8`                               | 8                    |
| second file has an extra column                              | `7`                               | 7 — reads correctly  |
| second file has the projected column in a different position | `5`                               | 5 — resolved by name |
| second file lacks the projected column                       | **refused** (Invalid Input Error) | fail closed          |

Two things here inverted my premise and are the reason this plan exists:

- `union_by_name=false` did **not** refuse a differing column set, and did **not**
  mis-map the reordered column — it resolved by name on this build.
- The case that does fail closed is a file **missing** a projected column.

So "evolved schemas fail by design on this path" would have been a wrong
conclusion, and the reordered case — the one I expected to be dangerous — behaved
correctly. I am reporting the observation, not generalising it: it is one build,
small files, and it says nothing about statistics pruning or pruning behaviour.

## Consequence for qualification

Qualifying evolution on the multi-file path requires a decision, not just a fixture:

- **Option A — declare per-file schema in the manifest** (closed fields, bounded,
  no free SQL), let the projection vary per file, and compute the expectation in
  Python across the differing sets. This is harness-only, but it enlarges the
  manifest grammar that was just fixed, so it needs its own offline tests before any
  live round.
- **Option B — qualify evolution as it is actually reachable today**: the existing
  single-file path, ten closed trials, one shared schema per observation. That
  already passed, and its `null` and `string` trials cover null and non-matching
  predicate behaviour — **within** one schema, not across files. Under this option
  cross-file evolution stays an open item rather than being called qualified.
- **Option C — pin the missing-column refusal only**: add one bounded two-file shape
  where the second file omits a projected column and require the refusal, which is
  the one evolved-adjacent behaviour the probe showed the path actually exercising.

My recommendation is **B plus C**: B states honestly what is covered, and C converts
the one observed fail-closed behaviour into a qualification instead of a probe
observation. A is real design work on a grammar that has just changed twice and should
not absorb a shape family by accretion.

## If execution is approved

- Harness file and runner only; no production source, no new settings, no new
  dependencies, no config channel, no default change.
- Offline first: the existing 22 tests stay green, new refusal paths get real tests,
  `py_compile` clean, harness target rebuilt, default invocation still refuses approval.
- A fresh protected freeze before any live run — the current freeze no longer matches
  `spec/s3-shipping-scope-plan.md`, which was edited after the passing round.
- One bounded live round on a new owned bucket under the explicit selected profile and
  region, no ambient fallback, new-bucket-only scaffold, absence before creation,
  complete inventories, exact-generation cleanup and verified bucket absence.
- No edits, builds or agents overlapping a live run. No effects retry. Failures retained
  as failures; no relabeling and no inherited qualification.

## What this plan does not claim

It does not qualify evolution for the shipping path, does not close package 26, does
not constitute the full shipping matrix, and does not authorise publication,
activation, commit or push. Offline results on DuckDB 1.4.2 are indicators only. The
existing qualified results — four single-file slices at ten trials each and the
three-file shared-schema aggregate at one trial — stand on their own evidence and are
not extended by this document.

## Refusal shape executed (option C), option B unchanged

Option C has now been executed and passed: the two-file shape whose second file omits
`payload7` was **refused at first guarded execution (212, HTTP status 206)** on the pinned
build — a bounded read had already succeeded, so the refusal is at execution, not before
transport. The expected code was corrected to 212 only after that number was observed; the
round that produced the observation is retained as a failure and was not relabeled.

Option B stands as written: **cross-file evolution in general is not reachable on this
path**, because the runner's own gate requires one shared schema per observation. It stays
open rather than being called qualified.

Evidence: `~/grv-s3-shipping-9wpuyoga` (six slices, failure null), freeze
`~/grv-task26-work/freeze-round-close-c`, parent account in
[s3-shipping-qualification-blocker.md](s3-shipping-qualification-blocker.md). Offline suite
23 tests. The home-directory consolidation remains parked.

## Typed shapes — scope conclusion, and the one shape reachable without a design change

Verified from code before proposing anything:

- `pull.inc:126–143` — the single-file encoder emits per-value validity: `null =
!validity.RowIsValid(index)`, and `ToString()` is called only for non-null values. So the
  null path is a real per-value path, not an abstraction.
- On the single-file path, ground truth is **fully native**: the harness holds the local
  descriptor and compares against it. This is stronger evidence than the multi-file case,
  where the expectation is Python-computed because the local oracle admits one descriptor.
- `s3_limits.hpp` contains **no type or column-count bound** — those bounds belong to the
  GCS-side reader. So they cannot be claimed as S3 refusals.

**Reachable now, runner-side only:** one shape identical in settings to the first single-file
shape — `NONE`, no dictionary, statistics on, 8192 rows written in four 2048-row row groups
(the single-file path writes the full table; only the multi-file shapes use single groups) —
with `id` real and
**all eight payload columns all-NULL**. The column set and declared types stay exactly
`SCHEMA`, so no gate, trial spelling or projection changes.

Why it is worth a shape rather than a footnote: nine of the ten trials then produce rows in
which the payload aggregates are typed NULLs **alongside** a real `sum(id)` — mixed validity
inside one bounded record. Existing shapes touch the null encoding only through the
empty-result case, and the `null` trial currently returns nothing on them; here it returns
every row. So it exercises the documented per-value validity path with non-empty results, on
native ground truth.

**Not reachable without a design change:** a column whose declared type contradicts a trial's
SQL — a BLOB or integer payload under `length()`, for example. The ten closed trials assume
`id` integral and `payloadN` strings, so such a shape would surface only as a binder or
execution refusal, and the numeric codes are per call-site, not per mechanism: I could say a
refusal happened, not **which** mechanism refused. I am not fabricating a shape whose cause I
could not attribute, and this is the same class of change I recommended against absorbing by
accretion in option A.

**Execution if approved:** runner only. Build a second fixture table for this shape with the
eight payload columns all-null; run the existing single-file invocation unchanged, so ground
truth stays native; expect ten trials correct and all four denials refused. Offline tests: the
shape is planned with all-null payload values, and a fixture containing a non-null payload
value stops the run; the existing 23 tests stay green. Fresh protected freeze before the live
round, new owned bucket, standard absence-before-creation and exact-generation cleanup.

Type variety on the **local** proof path (`fixture.cpp` typed table, `proof.cpp` type
scenario) is existing coverage and is not cloud coverage; this document does not merge the two.

## Typed all-null shape executed

Executed as planned, runner-side only. Result: **10/10 trials correct, all four denials
refused**, on fully native local-oracle ground truth, fixture 8192 rows / 70,148 bytes /
sha256 `d5dcff4a8907f01baeab8162797b18bdbfce957894111ec528b32f5af59cf7ec`.

Round evidence `~/grv-s3-shipping-owqpd7iv`, freeze `~/grv-task26-work/freeze-round-typed`
(34 artifacts, independently re-hashed, no mismatch, runner before/after matched). Seven
slices, failure null: five single-file shapes at 10/10 with all four denials, the three-file
aggregate correct, and the refusal shape refusing at 212 / HTTP 206 as established. Cleanup
accounted for all ten uploaded objects (five by cleanup, three by the aggregate slice, two by
the refusal slice), all four inventories empty, bucket verified **404**, marker matched, no
IAM change, no credential creation, no existing-data change.

The offline gate that guards this shape is pure and pyarrow-free so it runs offline: the
planned shape's settings must match the first shape's, an all-null readback passes, and a
non-null payload value, a null id, or schema drift each stop the run before upload. Suite:
**24 tests**. The wording above was corrected after the round to describe the full-table write,
so this plan file no longer matches the freeze taken for the run it describes.
