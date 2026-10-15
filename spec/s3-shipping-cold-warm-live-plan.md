# S3 cold versus warm observation — plan

Scope done from code before proposing anything. It changed the answer in two places.

## What the code actually shows

- Both paths run their trials on **one retained connection-session inside one process**, and both
  take a traffic measurement per trial (`Take` per trial in `cloud_qualification.cpp`).
- The **manifest path records** those per-trial counters. The **single-file path does not**: its
  trial records carry only `trial_index, trial, correct, query, explain`, and traffic is recorded
  only for the `view_binding` phase. Checked directly against passing-round evidence.
- No durable cache exists on this path across processes. So **warm only exists inside one retained
  connection-session**; a second run is cold again. That is the definition this plan uses, and no
  other reading of "warm" is available on this path.

So cold versus warm is not currently measurable, even though the measurement is being taken. The
gap is recording, not behaviour.

## Shape

One manifest entry, **two trials against the same object**, in the order that defines the
observation:

- file: 2048 rows, one row group, `NONE`, no dictionary, statistics on — the settings pair the
  package already uses.
- trial 1 `wide` — the **cold** read, first guarded execution against that object.
- trial 2 `narrow` — the **warm** read, second guarded execution on the same retained connection.

Two different trial names are required because the manifest grammar refuses duplicate trial names
(`manifest-duplicate-trial`). That is fine: the observation needs two sequential guarded reads of
the same object, not the same SQL twice, and `narrow` (`sum(id)`) and `wide` (the eight-payload
aggregate) are both closed trials already in the fixed set.

The four standing denials (direct, unlisted, nested glob, secrets) run against the same object and
must still refuse — including after the warm read, which is where a warm-path authority leak would
show.

## What is asserted, and what is not

- Asserted: both trials **correct**; all four denials **refused**; per-trial counters recorded.
- **Not asserted:** which way the counters fall. The documented position is that the metadata budget
  allows per-family finite retained copies and that holds and preparation give **no per-range
  generation pinning**. So warm reuse reducing fetches would be consistent with the design, and
  warm re-fetching everything is also consistent. I will report whichever way it falls. What is
  asserted about the warm read is that it gains **no new authority**: same owner, same guard, same
  denials refusing.
- Coverage limit restated because it bounds the claim: counters cover **accepted protected-reader
  GET bodies and HEAD/GET attempts** — not headers, rejected bodies, TLS, or total wire. A cold/warm
  statement on this path is bounded to that, and saying more would overstate it.

## Ground truth, honestly labelled

The manifest path's expectation is **Python-computed** from the local fixture, because the verified
local seam admits one descriptor. This is the weaker of the two available ground truths and it is a
disclosed limitation, not a native oracle. It is made independently checkable by arithmetic the
parent re-derives on its own, the way the three-file aggregate was. If this shape ever needs to
carry a correctness claim on its own, the honest answer is that it cannot — the single-file path
carries correctness on native ground truth, and this shape carries the traffic observation.

## Execution

1. Runner only. No harness change, no native rebuild, no new settings, no production source.
2. Offline tests first: manifest accepts one file with two trials; duplicate trial name still
   refused; both trial expectations computed; counters recorded per trial; the existing three-file
   aggregate and refusal shapes unchanged; existing 27 tests stay green.
3. Fresh protected freeze before the live round.
4. **One live round running only this shape**, using the selector. That is its payoff — this whole
   observation costs one bounded slice instead of a full re-verification pass.
5. New owned bucket, absence before creation, exact-generation cleanup, verified absence after.

Estimated: runner change plus offline tests in one working pass, one live round of a few minutes,
one report. If the observation comes back ambiguous, it is reported as ambiguous rather than
promoted into a conclusion.

## As executed

Implemented by one agent in the runner file only; parent verified the code independently rather
than accepting the agent's summary. Parent checks: the Python expectation mirror was recomputed
from first principles and decoded by hand — `narrow` = sum(id) 0..2047 = 2,096,128; `wide` = that
plus eight payload sums of 2048 × 256 = 524,288 each — and matched. The mirror was also compared
against the actual `ManifestTrialSql` text in `cloud_qualification.cpp`, not against trial names.
File mtimes confirmed the runner was the only file the agent touched; **the harness binary was not
rebuilt** and the guard dylib was untouched. Offline suite **35 tests** green (27 prior + 8 new),
`py_compile` clean.

Live round `~/grv-evidence/grv-s3-shipping-i1_n_kqp` — the first round written under the new
evidence root. Freeze `~/grv-evidence/grv-task26-work/freeze-round-coldwarm` (61 artifacts,
independently re-hashed, runner reported `artifacts_unchanged: true`). One slice, exit 0, failure
null, elapsed 20.206 s.

**Observation, on one object with two trials in the code-fixed order:**

| trial    | role | GETs | body bytes | correct |
| -------- | ---- | ---- | ---------- | ------- |
| `wide`   | cold | 67   | 4,297,089  | yes     |
| `narrow` | warm | 2    | 32,841     | yes     |

All four denials refused, including after the warm read; `warm_read_gains_no_new_authority: true`;
`counter_direction_not_asserted: true`, as designed.

**What it shows:** the warm read still performed **fresh bounded GETs** rather than serving
everything from retained state — so there is no full-result or full-file cache on a second read.

**What it cannot show, stated plainly:** the two trials are different queries, so the cold-to-warm
drop is confounded by projection size — `narrow` only needs `id`, `wide` needs all eight payload
columns. This shape therefore cannot attribute how much of the reduction was warm reuse. That is
exactly why the plan refused to assert direction, and no stronger claim is made here.

**An unintended but solid cross-check:** the cold single-file figures are **exactly one third** of
the three-file aggregate round — 67 × 3 = 201 GETs and 4,297,089 × 3 = 12,891,267 bytes, both
exact. Per-file read cost is identical whether the file is read alone or as one of three, so no
cross-file sharing benefit appears on that path.

Cleanup: the slice removed its own object, so cleanup reported zero remaining; all four inventories
empty, bucket verified **404**, marker matched, no IAM change, no credential creation, no
existing-data change.

Two operator errors, recorded so the round is read correctly. A first start was made with the bare
`python3` on PATH, which is Homebrew and has no pyarrow; it died on import, before any bucket, any
cloud call or any checkpoint write, so nothing was mis-recorded — but it is an error, not a
non-event. The freeze script was also handed a relative path and failed first time. The documented
live interpreter is `/usr/bin/python3`, and the re-run used it.

The selector earned its keep: this was one bounded slice of about twenty seconds instead of a full
re-verification pass.

## Correction to the premise of this plan

The plan says the single-file path does not record per-trial counters. **That is wrong.** It does
record them, nested inside each trial as `query` and `explain`. The claim came from inspecting the
trial record at the wrong level and is corrected here rather than left standing.

The consequence is stated plainly: the cold-versus-warm observation was **already available from
existing evidence**, so the live round was not strictly required. What the round still added is a
pair where both statements were chosen deliberately, and it confirmed the steady-state figure: the
warm read cost 2 GETs and 32,841 bytes, which is exactly what every other `narrow` execution in the
existing data costs. The observation stands. Its stated limit — two different queries, so no
attribution to warm reuse — stands too, and applies equally to the existing data.

Per-trial data from the all-null shape, for the record (8192 rows, 70,148 bytes, `view_binding`
1 HEAD / 1 GET / 16,384 bytes):

| #   | trial            | GETs | body bytes |
| --- | ---------------- | ---- | ---------- |
| 1   | wide             | 5    | 83,172     |
| 2   | narrow           | 5    | 83,172     |
| 3   | selective_wide   | 3    | 33,081     |
| 4   | narrow           | 2    | 32,841     |
| 5   | and_range        | 2    | 32,841     |
| 6   | or_range         | 2    | 32,841     |
| 7   | null             | 5    | 83,172     |
| 8   | string           | 1    | 16,384     |
| 9   | wrapper          | 2    | 32,841     |
| 10  | nonidentity_cast | 5    | 82,212     |

Read this as data, not as a conclusion. The counters move with what each statement needs. They are
consistent with the documented design — retained per-family copies, no per-range generation pinning
— and they are equally consistent with a straightforward re-read. Nothing here separates the two,
which is the same limit the plan recorded.
