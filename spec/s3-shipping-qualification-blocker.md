# Protected S3 shipping qualification blocker

Status: initial transaction-context blocker fixed; two65536-row shipping
scanner slices and two smaller diagnostic slices passed. Full matrix and broad
acceptance remain separate gates. No publication/activation approval.

## Authorized coordinates

Explicit user-selected profile `lexiupon-aws-cli-s3`, region `eu-west-1`, account
`590184134474`, ARN `arn:aws:iam::590184134474:user/lexiupon-aws-cli-s3`.
Only newly owned synthetic probe buckets were used. No ambient credential
fallback, existing application data, IAM/ACL/policy/versioning changes or keys.

## Executable result

`native/run_s3_qualification.py` has twelve passing offline safety tests. The
initial signed protected-reader scan failed after successful reader setup and
local fixture evaluation, before a remote HEAD. Private bounded diagnostics
identified protected read phase **20**, HTTP status **0**:

`VerifiedS3State::Request` → `HTTPUtil::InitializeParameters(ClientContext)` →
`HTTPFSUtil::InitializeParameters` → `KeyValueSecretReader::Initialize` →
`TransactionContext::ActiveTransaction`.

Known exception: `TransactionContext::ActiveTransaction called without active
transaction`. Both uncompressed/statistics and ZSTD/dictionary/no-statistics
attempts failed, with **zero qualifying slices**. A passing mock or a valid AWS
profile is not evidence that this shipping reader works.

Attempts to initialize parameters using `RunFunctionInTransaction` inside the
request path, then around pre-query verification, and a separate BEGIN/COMMIT
around verification were not accepted. Runs either exceeded the native process
budget or failed; no shipping matrix passed. All three experimental changes
were reverted. The final production S3 source is unchanged from the prior
checkpoint. Fix the catalog/parameter initialization boundary without invoking
context-lock-taking transaction helpers in parallel scanner callbacks; retain
exact metadata, conditional reads, named-profile isolation and guard policy.
Subsequent source inspection confirmed signed clients require the pinned
`HTTPFSParams` subtype (a plain base `HTTPParams` is unsafe). The accepted fix
constructs fresh pinned `HTTPFSParams(util)` without an opener or request-time
settings/secret discovery. GRV already acquired explicit profile credentials at
configuration and signs exact requests itself. Fixed TLS, no redirects/proxies/
retries, bounded conditional reads and signed stock clients remain. No catalog
transaction helper is entered from scanner callbacks.

Two **8192-row diagnostic slices** now pass ten local value-consuming oracles
and direct/unlisted/glob/secret denials each. Uncompressed/statistics wide/narrow/
selective-wide accepted content: **17,155,588 / 98,596 / 4,313,473 bytes**;
projection/selective-wide savings **99.4253% / 74.8567%**. ZSTD/dictionary/no-statistics:
**2,486,031 / 36,832 / 649,218 bytes**, **98.5184% / 73.8854%** savings. No-statistics
savings are not statistics pruning evidence. Private evidence
`~/grv-s3-shipping-10u5altj`; all owned generations deleted, empty all inventories
and bucket404. The previous larger timeouts are retained, not counted as passes.
The subsequent **65536-row slices also passed**, ten local correctness oracles
and direct/unlisted/glob/secret denials each. Uncompressed/statistics wide/narrow/
selective-wide: **137,244,704 / 788,768 / 4,542,849 bytes** (2116/36/71 GETs),
**99.4253% / 96.6900%** savings,511.921s. ZSTD/dictionary/no-statistics:
**19,790,639 / 193,188 / 805,574 bytes** (321/33/43 GETs),
**99.0238% / 95.9295%** savings,142.285s. Private evidence
`~/grv-s3-shipping-vku_mrsd`; two owned generations deleted, all live/version/
delete-marker/multipart inventories empty and bucket404. Evidence digests checked;
source/artifact hashes remained unchanged across the run. Full preparation,
wire overhead and broader matrix acceptance are still not measured/proven.

## Special-coordinate and empty shapes

Two more single-file shapes now read through the exact shipping path on a freshly owned
probe bucket: **special-coordinate** (one object whose real key carries a space, a literal
`+`, a non-ASCII letter, an apostrophe and a literal `-`) and **empty** (zero rows, unchanged
schema). Both completed with the usual ten local value-consuming oracles and the direct,
unlisted, glob and secret denials; accepted-content counters were 16384 bytes over 1 GET and
1 HEAD for the special coordinate and 1292 bytes over 1 GET and 1 HEAD for the empty fixture.
Offline runner safety tests remain green at sixteen, including the new canonical-coordinate,
byte-exact round-trip, empty-shape and generation-mismatch checks.

Two attempts failed first and are retained. The first sent a literal space and a literal
non-ASCII letter with an over-encoded `-`, and the reader refused it before any remote read
(phase 0, HTTP status 0). That is correct behaviour: `S3Parse` re-encodes the decoded key and
requires the URI to match, so space and non-ASCII must be percent-encoded and an unreserved
`-` must stay literal. The second attempt sent the canonical coordinate but stored the
_encoded_ string as the object key, so the decoded key did not name the stored object and the
read returned **404** at protected read phase 22. The runner now stores the real decoded key
and derives the canonical coordinate from it, which is what the passing run did. Nothing here
qualifies multifile, evolved or typed shapes, and no savings claim is generalised.

Cleanup for the passing run was recorded by the runner as `bucket-absence-unproved`; a direct
post-run `HeadBucket` on that bucket, and on both failed-attempt buckets, now returns **404**.
The transient is reported as observed and the runner's own record is not relabeled. Offline
checks, the byte-exact mirror of the native encoder, and this post-run verification are parent
work; the runner record stands as written.

## Cleanup and evidence

Each probe run uses absent-before-creation evidence, creation intent, a unique
bucket tag and object owner/hash metadata. Cleanup refuses unknown inventory or
validator mismatch. `ListObjectVersions` string `"null"` and omitted PUT/HEAD
VersionId denote the same unversioned generation **only after unchanged
never-versioned configuration is proved**. Conditional deletes omit null
VersionId (S3 rejects conditional DELETE with explicit `VersionId="null"`) and
require exact `IfMatch`. Real version IDs are never collapsed.

An initial validator mismatch and later transient AWS cleanup error were
retained as failures and recovered with marker/identity checks. All created
buckets were subsequently proved empty across live objects, versions, delete
markers and multipart uploads, deleted, and verified absent (404). Private
journals/initial immutable failure archives remain. Sanitized evidence:
[release-validation/s3-shipping-scanner-checkpoint.json](release-validation/s3-shipping-scanner-checkpoint.json).

That evidence identifies actual attempted source/binary hashes, with prior
failed attempts archived privately. It is not a source-to-binary attestation,
frozen candidate or complete successful matrix. Counters measure accepted GET content, not full
preparation, headers/TLS/wire bytes. Cold/warm, special/evolved/multifile,
resource/fault and parent lifecycle gates remain open.

## Multi-file aggregate closed by a harness fix

Three live rounds were needed. The first two failures are retained and were not
relabeled.

Round one failed with `harness-failed` and no per-stage detail, because the
manifest path had no fixed diagnostic codes. Fixed numeric codes were added to
the harness and the runner records them (numbers only; no manifest content and no
provider text is retained), together with an offline guard that runs the real
binary on a `json.dump`-spaced manifest and requires the pre-network refusal.

Round two reported code **201**. A ladder over the actual parser showed that the
closed JSON object loop read the next key immediately after a comma without
skipping whitespace, while `json.dump` always emits `", "`: the compact form was
accepted and the spaced form was refused. That was a genuine harness parser bug
and one line fixed it. The same round then reported code **211**, the first
guarded call: `CREATE VIEW` over three files.

A one-file discriminator through the same manifest path passed end to end, which
showed the refusal was specific to a file count greater than one. An offline
binder test then gave the exact cause: `read_parquet(a, b, c)` passes three
_positional_ arguments and fails to bind (`No function matches
'read_parquet(VARCHAR, VARCHAR, VARCHAR)'`), while `read_parquet([a, b, c])`
works. The harness was building the comma form. The fix uses the list literal
uniformly and lives in the harness file only; production source was not
implicated and is unchanged. The binder test ran on DuckDB 1.4.2, so it is an
indicator of the binder rule and not an attestation of the pinned 1.5.6 build.

The passing round ran at 8192 rows with three 2048-row files of 4,286,477 bytes
sharing one schema, one `wide` trial, and reported the aggregate correct. Parent
arithmetic agrees byte for byte: `sum(id)` 18,871,296 = 6143*6144/2 over ids
0..6143, and each payload length 1,572,864 = 6144 rows x 256 characters. The
`view_binding` phase recorded 3 HEADs, 1 GET and 16,384 body bytes; the trial
12,891,267 bytes over 201 GETs; all four denials (direct, unlisted, nested glob,
secrets) refused. The four single-file slices re-passed on the unchanged path at
10/10 with all denials refused.

Evidence `~/grv-s3-shipping-6wqmewsb`: five completed slices, failure null. All
owned objects were deleted individually, inventories were empty across live
objects, versions, delete markers and multipart uploads, the bucket was deleted
and verified **404**, and the owner marker matched. Offline gate before the run:
22 tests, clean byte-compile, clean harness target rebuild, default invocation
still refuses approval. A 32-artifact freeze was re-hashed independently with no
mismatch, and the runner's own before/after comparison matched. The freeze written
for the code-attribution round was broken (1 entry, a parent filter bug) and
stands as written.

This qualifies only the three-file shared-schema aggregate, one trial, one
settings pair. Evolved and typed shapes, cold/warm behaviour, full preparation
and end-to-end traffic, headers/TLS/total wire, and broad fault, cancellation and
resource parity remain open, and no publication or capability promotion is
implied. Parent verification: `/tmp/grv-task26/parent-s3-multifile-verification.json`.
No agent ran in this segment; diagnosis, edits, tests and validation were parent
work.

## Repeat round: aggregate failed, recovery verified, refusal shape has no live observation

The refusal shape (option C) was then implemented in the runner only — the harness
needed no change, because the numeric codes already distinguished its own call sites.
The shape writes two files, the second missing one projected column, and requires a
refusal at the harness first guarded call (code 211); any other outcome, including an
unexpected success, stops the run before any delete. Offline: **23 tests** green,
covering refusal-as-planned, a wrong code, an unexpected success and an unparsable
diagnostic; byte-compile clean; default invocation still refuses approval.

The next live round did not reach it. Evidence `~/grv-s3-shipping-8ij96ls3`: the four
single-file slices completed, then the **three-file aggregate** — which had passed on
the previous round with identical native code — failed at decision code **212**, the
trial query, after 48.759s with HTTP status 0. Cleanup then failed at
`aws-get-caller-identity-failed`, so nothing was cleaned by the run.

Both symptoms sit next to each other in time, which suggests a credential or CLI
transient, but the runner records no underlying reason for a 212 refusal and no HTTP
status was captured, so the **cause is unclassified and is not being called a
transient**. The prior pass keeps its own evidence and is not relabeled; this round
stands as failed.

Recovery used the runner's own tested cleanup path, on a copy of the recorded state,
after its creation fence was checked (intent, prior absence, account, profile, region)
against the seven tracked owned objects with their exact PUT identities: **7 owned
generations deleted, live/version/delete-marker/multipart inventories empty, bucket
deleted and verified 404, owner marker matched, no IAM change, no credential creation,
no existing-data change.** The original evidence directory is byte-identical before and
after (the recovery workspace was kept separate because `Probe.__init__` journals into
its own directory).

Working files previously held under `/tmp/grv-task26` — including the multi-file
verification record and the artifact freezes — **were removed by OS cleanup and no
longer exist on disk**; the passing round remains traceable through this checkpoint,
whose own before/after artifact comparison matched at the time. The recovery workspace
now lives under `~/grv-task26-work/`. The refusal shape is implemented and offline-tested
but has **no live observation yet**.

## Refusal shape closed; expectation corrected by observation

After a round in which the refusal shape refused at **212** while the test still expected
211 — and the actual number survived only because the recording gap had just been fixed —
the expected code was corrected to **212** on the strength of that observation: a
`CREATE VIEW` binds lazily, so a missing projected column surfaces at the first guarded
**execution**, after a bounded read had already succeeded (HTTP 206). This is the
difference between correcting an expectation from evidence and editing it until it goes
green, and the correction was made only after the number had been observed on the pinned
build.

The closing round passed end to end, exit 0, `~/grv-s3-shipping-9wpuyoga`, freeze
`~/grv-task26-work/freeze-round-close-c` (34 artifacts, independently re-hashed, no
mismatch), runner's own before/after comparison matched, prior failed round chained:

| slice                                                               | outcome                                                                  |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| uncompressed-statistics                                             | 10/10, four denials                                                      |
| zstd-dictionary-no-statistics                                       | 10/10, four denials                                                      |
| special-coordinate                                                  | 10/10, four denials                                                      |
| empty                                                               | 10/10, four denials                                                      |
| multi-file-aggregate (3 files, shared schema)                       | correct; `view_binding` 3 HEAD, 1 GET, 16,384 bytes                      |
| evolved-missing-column-refusal (2 files, second without `payload7`) | **refused as required** at 212, HTTP 206, native exit 1, reaped, 20.437s |

The refusal shape compares no value; the manifest carries a closed placeholder because its
grammar requires one, and the placeholder digest is recorded so that fact is auditable.
Cleanup accounted for all nine uploaded objects (four by cleanup, three by the aggregate
slice, two by the refusal slice); all four inventories empty, bucket verified **404**,
marker matched, no IAM change, no credential creation, no existing-data change.

**So what is now actually qualified:** a two-file set whose second file lacks a projected
column is **refused at first execution** — fail-closed behaviour, demonstrated on the pinned
build. Cross-file evolution in general remains **not reachable** on this path: the runner's
own gate requires one shared schema per observation. That is the option-B scope statement,
unchanged.

**Retained as observed, single occurrences, causes unclassified, neither called a pattern:**
the three-file aggregate failing at 212 with HTTP status 0 on the round before last (it has
since passed twice), and `zstd-dictionary-no-statistics` failing at phase 32 status 0 on
one round (green before and after). The aggregate and the refusal shape are each green on
their own rounds; they do not inherit each other's evidence.

One wording item deferred deliberately: the runner's refusal record still reads "refusal
required at the harness first guarded call" where execution is now understood. Fixing it
would change the runner source hash and break freeze matching against the round that just
passed, so it waits for the next legitimate code change. Offline suite: 23 tests, including
the strengthened assertion that every mismatch path records what it observed.

Working files for this package now live under `~/grv-task26-work/`; earlier `/tmp` working
copies were removed by OS cleanup. The home-directory consolidation is **parked** by
decision, not forgotten — checkpoints reference evidence by home path, so a move needs a
mapping and a decision about where the runner writes future evidence.

## Typed shape closed on native ground truth

The all-null shape was the one typed case reachable without a design change: settings
identical to the first single-file shape, same column names and declared types, every payload
value NULL. It completed **10/10 trials with all four denials refused**, on the harness's own
local-oracle comparison — the strongest ground truth available on this path, since the
multi-file expectation is Python-computed. Nine of those trials carry typed NULLs alongside a
real `sum(id)` inside one bounded record, which is the per-value validity path in
`pull.inc:126–143` exercised with non-empty results rather than only through the empty-result
case.

Round `~/grv-s3-shipping-owqpd7iv`: seven slices, failure null. Five single-file shapes at
10/10 with all four denials each, the three-file aggregate correct, and the refusal shape
refusing at 212 / HTTP 206 as established. Freeze `~/grv-task26-work/freeze-round-typed`,
34 artifacts, independently re-hashed with no mismatch; runner before/after matched. Cleanup
accounted for all ten uploaded objects (five by cleanup, three by the aggregate slice, two by
the refusal slice), all four inventories empty, bucket verified **404**, marker matched, no IAM
change, no credential creation, no existing-data change.

Runner-side only: one shape entry, a pyarrow-free pure gate so the check can run offline
(`require_null_payload` — all-null passes; a non-null payload, a null id, or schema drift each
stop the run before upload), and a readback gate on the fixture on disk. Offline suite
**24 tests**.

**Not claimed:** a column whose declared type contradicts a trial's SQL — a BLOB or integer
payload under `length()` — cannot be expressed by the closed trial set, so such a shape would
yield a refusal whose **mechanism could not be attributed**; `s3_limits.hpp` carries no type or
column-count bound (those are GCS-side). Type variety on the local proof path
(`fixture.cpp` typed table, `proof.cpp` type scenario) stays separate coverage and was not
merged into cloud coverage.

What is left in this package: cold versus warm observation, full preparation and end-to-end
traffic accounting, headers/TLS/total wire, broad fault, cancellation and resource parity, and
security signoff. The home-directory consolidation remains parked.

## Correction: per-trial counters were already recorded

A factual claim in the cold-versus-warm plan was wrong and is corrected here. The single-file path
**does** record per-trial traffic, nested inside each trial as `query` and `explain`. The claim to
the contrary came from inspecting the trial record at the wrong level.

The cold-versus-warm round stands on its own result — it chose both statements deliberately and
confirmed the steady-state cost of a second read on a retained connection: 2 GETs, 32,841 bytes,
which matches every other narrow execution in the existing data. But it was not the only way to see
that, and the plan now says so.
