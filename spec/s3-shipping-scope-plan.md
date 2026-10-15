# Package26 — S3 shipping scope plan

Parent-authorised continuation of the protected S3 shipping qualification. The fixed
baseline remains: initial transaction-context blocker fixed, two 65536-row shipping slices
and two 8192-row diagnostic slices passed, twelve runner safety tests passing. Those results
keep their own evidence and freezes and are not re-run here.

## Scope for this round

Only the two omitted fixture shapes that the existing single-location shipping plumbing can
already carry:

1. **Special coordinate** — one file whose object key carries the approved characters (one
   space, one literal `+`, one percent-encoded sequence, one non-ASCII letter, one apostrophe),
   read through the exact shipping reader with the guard, named profile and fixed TLS host.
2. **Empty** — a zero-row fixture with the unchanged schema, read through the same path.

For each shape the runner records absence before creation, creation intent, unique bucket tag
and object owner/hash metadata, exact generation cleanup, and the post-native identity recheck.
Counters continue to cover accepted protected-reader GET bodies and HEAD/GET attempts only.

## Deferred in this round, still open

Multifile and evolved shapes need multi-location support in the proof binary; they are not
claimed here. Typed-shape variation, cold versus warm observation, full preparation traffic,
headers, TLS and total wire bytes, and the broad fault, cancellation and resource parity gates
remain open. No-statistics savings are not statistics-pruning evidence. No savings claim is
generalised.

## Multi-location round

`grv_native_s3_query` already accepts a file list of up to 4096 entries, so multi-location
reading is production-supported; only the qualification harness is fixed to one file. This round
adds a multi-file shape through a new manifest invocation of the harness and changes no
production source.

The existing twelve-argument single-file invocation stays exactly as it is, so the already
qualified slices keep their own evidence and are not re-run here. The new invocation takes a
manifest listing the files (local path, coordinate, size, sha256, validator) and the
expected results. For multi-file shapes the ground truth is computed independently in Python
from the local fixtures, because the harness local oracle admits only one descriptor per call;
this difference is disclosed and the guarded cloud path is unchanged. Per-file size, sha256 and
validator must all be present and distinct; duplicate coordinates within a manifest are refused.

The first multi-file shape uses three single-row-group files with one shared schema and one
aggregated expectation. Typed, evolved, cold/warm and the fault, cancellation and resource
parity gates stay open. A fresh protected freeze of the rebuilt harness binary, runner, plan
and pinned extension is required, and prior freezes and their evidence are not reused or
relabelled.

## Parent review of the implementation

Reviewed in code, then verified independently by the parent: the harness manifest mode enforces
the closed grammar, the depth, file, trial and expected-value bounds, exact member sets, refusal
of unknown fields, missing fields, empty lists and duplicate coordinates or identities, and
requires each coordinate to sit inside the admitted scope. The guarded cloud call runs over the
whole file list and the result is compared byte-exactly against the Python-computed expectation.
The twelve-argument single-file invocation is unchanged. The decision point that multi-file
ground truth cannot come from the local oracle is commented where it matters: that seam admits one
descriptor per call.

Independently re-run by the parent: runner byte-compile clean; offline safety suite **20 passed,
0 failed**; offline default mode still refuses to act without explicit approval and prints no
credentials; `cmake --build ... --target grv_cloud_qualification` reports the target already
built. The delegated agent hit its turn limit once and was resumed to finish its tests; its work
was not accepted on report.

Live round authorised with a fresh protected freeze covering the rebuilt harness binary, the
modified runner and harness source, the pinned extensions and this plan. A live run executes the
four existing single-file shapes plus the new multi-file shape as one observation; earlier
single-file results keep their own evidence and are not inherited.

## Execution rules

Runner changes stay offline-verified and their existing twelve safety tests stay green. New
offline tests must not contact AWS. Parent reviews the actual diff and re-runs validation.
Live execution uses the explicit user-selected profile `lexiupon-aws-cli-s3`, region
`eu-west-1`, account `590184134474`, ARN
`arn:aws:iam::590184134474:user/lexiupon-aws-cli-s3`, with no ambient credential fallback, no
ADC or profile defaults, no impersonation, no keys, no existing application data, and no
IAM/ACL/policy/versioning changes on existing resources. Only newly owned synthetic probe
buckets are used, with the reviewed new-bucket-only scaffold. A fresh protected freeze of the
runner, binary, extension and notices is required before any live run. No edits, builds, agents
or other lifecycles overlap a live run.

Failing stages and cleanup errors are retained; no effects retry, no relabeling, no inherited
qualification. This plan does not close package 26, does not constitute the full shipping
matrix, and does not authorise publication, activation, commit or push.

## As executed

The offline safety suite is now 22 tests. Two harness-only defects were found and
fixed during execution, both recorded in
[s3-shipping-qualification-blocker.md](s3-shipping-qualification-blocker.md):
the closed JSON parser did not skip whitespace after a comma inside an object, and
the multi-file `read_parquet` call was built with comma-separated positional
arguments instead of a list literal. Production source was not changed by either
fix. The three-file shared-schema aggregate, one trial, one settings pair, now
passes; evolved and typed shapes, cold/warm, full preparation and end-to-end
traffic, and broad fault, cancellation and resource parity remain open.

## Decisions taken before the remaining families

**Shared-path gates are closed once.** The guard, the pinned signed HTTPFS and the in-process
execution path are shared between S3 and GCS. What is S3-specific — URI encoding, exact-file
metadata and the shape matrix — is done. So fault, cancellation and resource parity are closed
**once on the shared path** and recorded for both this task and the in-process transition task,
rather than paying for a separate S3 live round of the same fault. S3-specific measurement that
remains is cold versus warm observation, full preparation and end-to-end traffic accounting,
and the headers/TLS/total-wire definitions.

**A slice selector now exists** (`--only`, repeatable; default is every slice). It cannot widen
the universe: an unknown name stops the run before any cloud work, and cleanup stays in the
`finally` block so a single-slice run still owns and removes exactly what it created. Offline
coverage asserts default-equals-all-seven, fixed order, unknown-name refusal, and that the
selector guards sit inside the `try` and never wrap cleanup. Suite: **27 tests**.

Because of this, a passing round no longer automatically means every shape was re-verified. Each
round must keep naming the slices it actually executed; the evidence already records them
individually and that record is what any acceptance reads.

**Home-directory consolidation executed** at the top of this block, by decision, so the freeze
already due for the next block covers both it and the selector.

All 99 top-level `~/grv-*` directories were renamed into **`~/grv-evidence/`** — same filesystem,
inodes preserved, none lost, none left behind. `~/grv-evidence/MAPPING.md` is the resolver:
historical checkpoints and documents keep their old paths and were **not rewritten**, and they
resolve by prefix under the new location.

Two locations were deliberately **not** moved, because code points at them: `~/.grv-native-work`
(pinned DuckDB 1.5.6 source, cmake outputs, the guard dylib — SHA re-verified unchanged at
`2709cc619eb125d1…` — and the owner-validation target) and `~/.grv-httpfs-prototype-extensions`
(the pinned signed HTTPFS and AWS extensions, which are the runners' default artifact arguments).
The rule is exactly: move `~/grv-*`, leave dot-prefixed infrastructure alone.

Both qualification runners now write new evidence under `~/grv-evidence/`, and a test asserts
that, so a second writer cannot quietly start dropping evidence in the home root or in `/tmp`
again — `/tmp` is volatile and has already deleted working files once. During this edit I typed
one digit of the selected ARN wrong and caught it by comparing against the identity recorded by
the last passing round; it is fixed and verified, and it is noted here because a near-miss on an
authority constant deserves a record rather than silence.

Build infrastructure stays where it is for now. `~/.grv-native-work` and
`~/.grv-httpfs-prototype-extensions` are cited inputs — runner defaults plus 23 checkpoint files
— and the guard dylib and qualification binary there are what the next round's freeze compares
against, so moving them now would break matching for no benefit. Retirement is scheduled with the
immutable release-candidate work, which rebuilds and re-hashes everything anyway (task #56).
`~/grv-evidence/` stays evidence-only, and its `MAPPING.md` records the arrangement so historical
citations still resolve.

## Preparation and traffic accounting — closed within stated coverage

**What the counters cover.** Accepted protected-reader GET bodies, and HEAD/GET attempts. Per-trial
counters exist on both paths, nested inside each trial, plus a separate binding phase. Existing
passing rounds therefore already contain the traffic data for every statement executed.

**What they do not cover.** Response headers, rejected bodies, TLS, and total wire. Also not
covered: the runner's own end-to-end operation traffic — put, head, inventory and delete calls.

**Preparation.** The mandatory full SHA256, size, schema and row checks run inside the verified
reader. Their traffic is charged to whichever statement triggers the open. It is visible at the
statement level and is not separable below it.

**Closure.** The recorded per-trial data in the qualification record and in the cold-versus-warm
plan is the answer, **within the coverage stated above and not beyond it**. No claim is made about
headers, TLS, total wire, or parent-side operation traffic, because nothing measures them.

**Widening the counters** — headers, TLS, total wire, and preparation as its own phase — is a
native change in the pinned path, not a harness change. It is deferred by decision into the
release-candidate rebuild window, together with the shared-path fault, cancellation and resource
highwater closure that serves both this task and the in-process transition task. The expensive
native step is paid once, in that window, where the hash discipline already lives.

What remains in this package after this closure is the security signoff.

## Decision: application-layer estimate

The traffic item closes on an **application-layer estimate**, computed at the report level. No
production source is touched to obtain it, so the read path stays exactly as qualified.

Definition, as agreed and as it must be written wherever the number appears:

> **Application-layer estimate** = measured accepted body bytes + parsed-layer header sizes.
> TLS handshake and record framing are **excluded and not measured**. This is an estimate, not
> wire truth.

Method, stated so the number is reproducible: measured requests multiplied by a stated nominal
header allowance, plus the measured accepted body bytes. Both inputs come from counters already
recorded by the qualification harness.

What this does **not** license: no wire savings claim, and no bill-matching claim. The savings
percentages already recorded remain body-byte figures within stated coverage. The primary objective
— exact identity, exact authority, mandatory preparation, fail-closed refusal, protected ownership
and cleanup — does not depend on any traffic number, and nothing here changes it.

An estimate was chosen over instrumenting the reader because a real header count would require
editing `verified_s3.inc` or `gcs_filesystem.inc`, and a change to the read path must be reviewed as
production code. The value here does not justify that. The rebuild already approved for the
shared-path work stands; it is not needed for this.
