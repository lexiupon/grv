# GCS-selected aggregate metadata accounting and recovery

Status: agreed GCS-specific logical metadata/transfer scope complete; paused.
See [completion checkpoint](gcs-memory-qualification-completion.md) for evidence
and accepted boundaries; not total RSS or release qualification. User explicitly
approved proceeding with aggregate accounting, session-recovery regression and
artifact-specific validation using the **GCS-specific metadata policy**. No
allocator, new helper, universal heap/RSS cap, public version or silent generic
local/S3 refusal is authorized. Policy: `gcs-metadata-policy.md`.

## Accounting units and nonclaims

Let L=8MiB **logical JSON charge**, D=8MiB raw document size, F=1MiB ordinary raw
frame, I=16MiB generic identified/bootstrap frame, N=2MiB native encoded output.
L weights nonstring bytes256 and string/escape bytes1, depth64. It bounds content
and container complexity before parse/copy wherever selected GCS identity is
known. It is not allocated capacity, an allocator charge, peak RSS, or the
32MiB transfer scratch reserve. Do not add L/D/F into an invented physical-memory
maximum. Simultaneous independently bounded objects are recorded below rather
than claiming one L covers the entire channel/host.

## Simultaneous ownership inventory

| Family                 | Owners / lifetime / admission                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Reader → dispatcher    | sync_channel4: ≤4 queued packets +1 reader-owned blocked send +1 dispatched packet; post-binding metadata each L, raw F. Reader activates on GCS binding before reading next packet; main separately activates at dispatch. During parse raw + RawObject key/set scratch + Value + typed conversion copies coexist. Generic registration/bootstrap/binding itself precede selection and retain I/F limits.                                    |
| Document upload/state  | one upload +one completed document per sender (two senders), each D/L after activation. Chunk decode≤512KiB plus base64 string/frame/canonical comparison temporaries; upload Vec capacity can exceed len. Document-end borrows raw for lexical admission before parse and canonicalization. These capacities are not numerically identical to D/L.                                                                                           |
| Reference resolution   | one raw frame Value + cumulative document expansions +typed frame; reserve includes frame and every referenced document before any expansion clone. `observe` repeats resolution for authority/consumption, but earlier expanded frame may still live. No count of references bypasses cumulative L.                                                                                                                                          |
| Outbound               | one Outbound frame, optional canonical D bytes, current chunk frame +Codec Value/serialized F; before frame→Value/JCS copy, original frame passes L. One worker reply in sync queue plus producer's blocked send/current reply can overlap; they are distinct admitted/generated metadata owners, not one shared allocation.                                                                                                                  |
| Active worker          | Active.request clone plus dispatch/worker frame and typed operation argument; at most one active SDK operation. Native workers retain one queued command plus one executing closure/result; caller operations serialized under per-worker mutex. Existing borrowed caller-copy admission charges the actual discovery/files/contracts/session overlap before adapter copies.                                                                  |
| Pull SDK authority     | one prepared PullPlan, one authorization fixed request/recovery, one resolution request, one bound identity and fixed lookup, plus one prepared CommandCall. Each derives from admitted frame/response, cardinality constant, no unbounded history map. These overlap later operations; do not call per-item L an aggregate whole-host cap.                                                                                                   |
| Build SDK authority    | one discovery request/discovery/session/candidate/outcome family; retained+incoming transition has aggregate L admission before authorization or successful result cloning. No silent session replacement.                                                                                                                                                                                                                                    |
| Hook / stream evidence | hook map retains≤1024 attempts (preexisting generic count); GCS adds aggregate L retained+incoming request. Stream snapshots/pending checkpoints aggregate L; checkpoint IDs≤1024 per stream. Flow checkpoint acknowledgements must be accounted as the same stream lifetime.                                                                                                                                                                 |
| Wire persistent state  | one build_session plus one build_counts and accepted digest; each derives from admitted response, counts/tables bounded by session selected outputs. Stream expected/covered/completed/sequences/counts/current/required_counts derive from admitted request/session; slots≤negotiated64. One active Operation.request Value can overlap Active.request/worker/decoded request. No historical collection of all operations.                   |
| Channel ID history     | document IDs≤1024 total both senders, no reset/reuse after payload consumed. IDs have closed UUID shape; request correlation retains only active/retired/latest IDs, not every request.                                                                                                                                                                                                                                                       |
| Direct runtimes        | one GCS connection per build/pull Runtime (including mixed additional binding refusal); one retained build session identity with same exact-identity reopen allowed. Other independently constructed runtimes and host-owned objects are outside a process-wide cap.                                                                                                                                                                          |
| Native metadata        | encoded allocation N stays live during native result/chunk and Rust decode; decoded GCS row storage≤8MiB via explicit preallocation calculation. GCS relation_schema now uses that decoder. Engine collector64KiB soft and4MiB postallocation chunk adoption are accepted partial controls, not prevent-before-allocation proof.                                                                                                              |
| Staged GCS export      | old invocation Engine is destroyed/reopened before acquisition/fetch. Local staged source reserves full Rust encoded capacity +native decoded source within negotiated source unit; Arrow conversion scratch separately admitted before allocation. No HTTPFS read-buffer stats renamed into scratch. Engine-owned old handles/context separated by the destruction barrier; escaped arbitrary native owners are not assumed absent globally. |

This is a logical, per-family model with bounded ownership multiplicity, not
measured allocator capacity. Parsed/canonical copies and container slack are
explicit; no invented arithmetic claiming8MiB or32MiB is total adapter memory.
Scoped independent inventory review `52e0d60a-b079-4f7` completed its bounded
inspection without finding concrete unbounded retained collections, reference
expansion or numeric canonicalization amplification in the reviewed paths. It
supports a documented logical model, not one aggregate8MiB/RSS cap, and did not
rerun tests. Its conservative summands: six queued/reader/dispatch metadata units;
retained SDK pull slots at most five independently admitted logical units because
authorization combines request and recovery from distinct admissions; two uploads
at most16MiB byte **contents**, not capacity; canonical Value/output/sort scratch
separate, finite numeric rendering/exact integer scratch≤309digits. Source,
bootstrap and engine limits remain separate.

Parent accounting additionally identified durable JSON pre-parse admission
missing despite the native2MiB encoded limit. GCS BuildStore stored session reads,
input receipt/contract reads and stored-record canonical writes now use the same
policy before typed decode/canonical copy. Pull omitted-target contract reads
and receipt canonical writes also use it; even paths that ignore malformed
checkpoint JSON must refuse amplified GCS records before parsing, not hide them. GCS PullStore binding, lookup, resolve,
prior-contract receipt and checkpoint preview admit borrowed raw before decode;
refusal remains Unknown/OutcomeUnknown, not absence or repair. Two persisted
node-amplification regressions are below. Focused verification
`d58c3257-1621-4c0` completed8source inspections: other gates/cardinality looked
consistent but its sampled omitted-target contract path lacked admission. Parent
had added that gate while review ran, then aligned the read with guarded durable
projection and added exact omitted-view refresh regression. Final focused
verification `1d8a915b-8a03-477` completed4source calls with no blocker for that
specific gate: durable projection → root-conditioned raw admission → typed decode
precedes schema inspection/drop/view replacement. Local/S3 original path remains.
Reviewer inspected regression but did not rerun; no all-memory approval.

## Actual recovery regression

New source-free test
`gcs_selected_sdk_session_reopen_preserves_exact_identity_and_durable_candidate`
in `crates/grv-adapter-duckdb/tests/build.rs` runs actual DuckDbAdapter through
SDK and native library with GCS root automatic policy selection. It:

1. binds/discovers/prepares a fixed local-only managed build under a gs:// root;
2. reopens the exact session twice without eviction/replacement;
3. executes once, closes the served channel and stops owners;
4. starts a new channel, reopens identical durable session/candidate;
5. accepts completion; changed identity refuses and closes normally;
6. independently reopens BuildStore and verifies exact session/completion/digest.

Direct GCS Runtime discovery now refuses after retaining a session (exact reopen
rather than starting a new discovery transaction); post-read open also verifies
the durable returned session UUID matches the retained identity before map
insertion, refusing OutcomeUnknown instead of retaining a second identifier.
Generic/local/S3 Runtime behavior is unchanged.

No credentials, GCS calls or synthetic bucket are needed. This is genuine
served SDK/native recovery coverage for the selected policy, not cloud auth,
held-parent lifecycle, crash/WAL or general rollback qualification.

Persisted build session options and pull receipt arbitrary JSON with40000empty
strings fit the2MiB raw result but exceed the logical structural reserve. Tests
assert refusal **before typed decode** reports decoded allowance, then independently
compare unchanged database and ownership-witness bytes. No truncation/eviction,
JSON repair, false NotCommitted or authentication occurs. Native suite104/12ignored
passes after this fix. Third omitted-target contract regression refuses
node-amplified persisted contract before typed decode, preserves original view/
immutable receipt replay and database bytes after rollback. Final native suite
105/12ignored passes after all three history admission tests.

Latest offline stage: native105/12ignored, build22, extraction9; wire5library+
20protocol, SDK18library+3runtime; strict native adapter/conformance/wire/SDK
Clippy (native and nonnative), fmt/diff, stale-peerCLI1 and12ctest pass.
Stale-peer rerun and signed mixed-local source-free checks remain recorded in
their stage logs; standalone ctest counterexamples passing do not imply upstream
allocation acceptance. Logs `/tmp/grv-gcs-final-accounting-native.txt`,
`/tmp/grv-gcs-final-accounting-wire-sdk.txt`, initial specific
`/tmp/grv-gcs-session-recovery.txt`. Stable prebuilt offline final105stage
`~/grv-gcs-memory-offline-rdvyuo0g`: seven test binaries pass, eleven pre/post
artifact hashes unchanged. Earlier `gab585t_` is passing104stage; `lr3ctckw`
failed extraction only because direct driver omitted required native fixture env,
all nine extraction tests failed before fixture execution. Failed logs retained,
no product change; corrected driver includes env and hashes adapter/fixture too.

## Artifact-specific validation gate

Final omitted-contract verification landed before stable live validation. Current
conformance held test/CLI/GCS fixture built before any live lifecycle; build log
`/tmp/grv-gcs-memory-held-build.txt`. No build is allowed during actual live/held
execution; drivers invoke prebuilt binaries and verify hashes before/after. Keep
old failed evidence and pre/post held hashes. Mandatory full preparation remains;
GCS capability remains unadvertised. Select narrow live/synthetic requalification
for paths affected by policy and inventory fixes, capture test/library/extension
hashes before+after, generation-qualified owned cleanup. Narrow final-stage live pair
`~/grv-gcs-metadata-stable-kiaj7dhn` used direct prebuilt library-test binary and
unchanged native library/signedHTTPFS (three pre/post SHA equal); both live pull/
external self-preparation tests passed. One owned generation deleted, all-version
inventory empty and bucket404; no IAM/key/policy changes. This direct library pair
alone does not qualify served SDK wire policy or real parent holds.

Final unchanged-artifact actual held lifecycle completed at
`~/grv-held-gcs-y_2_qyrt`: exit0,1passed/0failed,1870.18seconds. Parent verified
result/summary, five saved/current pre/post hashes equal, all-version inventory
empty and bucket404. Direct prebuilt current conformance test/CLI/GCSfixture/
library/HTTPFS; no build or artifact edit during lifecycle. Final cleanup0
generation deletions follows earlier prefix cleanup, not total deletion count.
Published holds intentionally retained, not abort/prune/release/expiry proof.
This independently qualifies current artifacts, not promotion of an older run.
Agreed memory scope accepted under documented logical/engine limitations; pause
now before unrelated expiry/security/S3/package/release matrices.
