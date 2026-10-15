# GCS-only metadata admission policy

Status: implemented and qualified for the agreed GCS-specific logical memory
scope; [completion checkpoint](gcs-memory-qualification-completion.md). Paused
before unrelated qualification; not a heap/RSS or release acceptance claim. User
approved a small metadata policy, not custom allocator protection. This document
states the concrete current caps and activation boundary; no public wire/schema/
durable version bump, new credential, helper process or allocator replacement.

## Selection and limits

DuckDB internally opts in through SDK `Adapter::bounded_gcs_metadata()` (default
false for other adapters). SDK sees a `bind_connection` with `gs://` root and
selects the policy, not an operator-disableable safety checkbox. All subsequent
packets on that served channel retain it; it cannot revert/evict history.
Local/S3 channels and generic SDK defaults keep existing public limits.

| Structure                                              | GCS selected allowance                                            |
| ------------------------------------------------------ | ----------------------------------------------------------------- |
| Chunked raw metadata document                          | 8MiB each (ordinary generic limit64MiB unchanged)                 |
| JSON logical admission                                 | 8MiB weighted: string/escape byte1, nonstring byte256             |
| JSON depth                                             | 64 opening containers                                             |
| Channel document IDs                                   | 1024 total, across both senders, never reset for reuse            |
| Stream checkpoint IDs                                  | 1024; no replacement/reuse on exhaustion                          |
| Retained SDK hooks / stream evidence / build lifecycle | 8MiB aggregate logical per state family incl. incoming transition |
| Direct runtime connections while GCS retained          | one per Pull Runtime or Build Runtime                             |
| Direct GCS build session identity                      | one per bound handle; same-identity reopen allowed                |
| Native response / decoded native rows                  | existing2MiB /8MiB logical separately                             |

Logical admission is a conservative structural/content reserve for node/container
amplification and owned copies—not actual malloc capacity/RSS or engine budget.
The existing caller-copy stage additionally charges simultaneous typed/file/
contract/discovery/session duplicates before construction. No automatic budget
increase, document truncation, ID reset, metadata repair or session eviction.

## Activation boundary and queue accounting

The bootstrap and GCS binding frame itself are decoded using generic framing
limits before the SDK knows the root. The adapter registration is also generic.
The binding frame is admitted once the root is known, before binding dispatch.
Reader policy activates upon receipt of this frame **before reading subsequent
packets**, independent of main writer/state policy. Main policy activates only
when that binding packet is dispatched; a queued future GCS binding must not
change an earlier operation's output. Reader checks raw frame shape before JSON
Value/typed conversion. State checks held documents on activation and admits
raw document bytes before document-end parse/canonicalization. If a document was
uploaded before binding, its generic decode allocation is not retrospectively
claimed protected; activation checks its current contents before further clones.

SDK input sync_channel4 has at most four packets plus one reader-owned blocked
send plus one dispatch packet; after GCS activation each metadata packet passes
the8MiB logical structural reserve. Binary batch payloads are separate credited
transfer bytes, not metadata; generic max64MiB batch transport remains. Native
worker queues remain synchronous one command/reply; source/result buffers use
separate budgets. Process lifecycle permits only one bound handle and one build
session; direct runtime methods enforce corresponding GCS cardinality before
creating a worker or preparing a second session. Other runtimes/host-owned
arbitrary documents are not a host-process-wide budget.

SDK Outbound admission occurs before frame-to-Value and canonical document
copies, and native metadata/caller admission remains in place. Recursive document
expansion charges the original frame plus **every** referenced document before
any expansion clone, including reference-shaped arbitrary inline JSON. Completed
worker results are admitted before SDK build/pull/bind successful-result caches
are updated; a delivery refusal cannot install unseen successful authorization.
Hook request maps, checkpoint snapshots/pending coverage and SDK build lifecycle
records have aggregate retained-plus-transition admission before additional copies.
Only identified requests use lifecycle admission; non-request document/control
frames retain their own safe refusal path. One outbound
and one worker reply are retained at a time. JCS serialization currently makes
a temporary Value plus canonical bytes/sort scratch; these are separately
bounded by admitted content/structure, not covered by the source scratch32MiB
by assertion. Full aggregate simultaneous copy high-water is qualification work;
no claim that one8MiB cap is the entire channel's memory.

## Refusal and outcomes

Raw framing/document/node/ID allowance refusal closes invalid/unadmitted metadata
handling without executing the affected request. Already executed work can still
have known/unknown effects; SDK stop/join and durable recovery remain mandatory.
Outbound admission refusal is `ADAPTER_FAILURE`, preserving known outcome data
in durable records; it is never translated into no-effect/NotCommitted. If a
known terminal result is refused while cancellation is pending, send the error
and close the channel with joined cleanup/recovery required—no Stopped or
Completed acknowledgement fabricated from the refusal. Direct
runtime connection/session refusal is `INVALID_DECLARATION` before a new worker/
authentication or preparation effects. Reopen of the same session identity stays
available; close a channel/reopen exact durable outcome to recover after ID
exhaustion. No automatic effects retries or channel rotation.

## Evidence and scoped acceptance

Current offline policy tests exercise generic-control versus selected doc limits,
node amplification preparse, escaped/multibyte strings, excessive depth,
retained-ID exhaustion without resetting state, GCS bind reader activation,
aggregate ten-reference expansion refusal before clones, outbound refusal before
document copies and refused bind-result failure/close without retry. Direct
runtime duplicate connection refusal retains first owner, avoids a second DB and
preserves two ordinary local connections. Latest native101/12ignored,
wire5library20protocol, SDK18library3runtime, build21/extraction9, staleCLI1 pass;
strict affected Clippy/fmt/diff pass. Prior offline/live/held stages remain
artifact-specific. New GCS-selected actual SDK/native session-recovery regression
passes exact repeated reopen, channel restart with durable candidate, acceptance
and changed-identity refusal; final build suite22passes. Aggregate inventory is
recorded in [gcs-aggregate-metadata-checkpoint.md](gcs-aggregate-metadata-checkpoint.md),
with scoped review in progress. Final aggregate/state and current-artifact validation now pass in the
[completion checkpoint](gcs-memory-qualification-completion.md); broader gates
remain open. Focused follow-up `bbf792d4-7a24-4ea` verified expansion and ordinary
result guards but found the pending-cancel terminal refusal bug. Fixed by error
then joined channel close, regression confirms no stopped acknowledgement;
initial test incorrectly attempted normal close while cancel remained pending,
then corrected to assert transport closure/recovery. Follow-up did not inspect
the final cancellation fix; subsequent source review `435b1a3a-046f-4cb` inspected
its final ordering and found no concrete defect, without rerunning tests. Source
review `adf5a93f-6948-4f1` supports native
metadata result/chunk closure but could not prove final HTTPFS file-handle release
before staged export. Ownership follow-up `77d0e021-27c0-4dd` traced reader →
CachingFileHandle → unique underlying HTTPFileHandle and a second local Parquet
scan handle. View catalog retains parsed SQL; multi-file statement caching is
unsupported; copied bind data excludes initial/union readers. Independently
retained unexecuted prepared plans remain a possible edge, not evidence GRV
retains them. This was source inspection, not binary/source correspondence or
measured allocation proof.

Parent inspection found the actual export barrier: `BuildStore::start_export`
calls `NativeEngine::close_invocation` **before** `acquire_build_outputs` and any
`fetch_export`. Native code resets/destroys the old Engine then opens a new one;
interrupt state retains no connection/database, workspace ownership stays held.
This is a memory ownership boundary, not immutable rollback/outcome proof.
Engine has no prepared-plan field; GCS metadata SendQuery/result and staging
Prepare handles are function-local. Focused source verification
`435b1a3a-046f-4cb` completed at its turn limit, confirming this barrier for
Engine-owned state and the final cancellation-refusal ordering, with no concrete
defect established. It did not rerun tests, prove arbitrary escaped owners absent,
finish every internal metadata caller/guard or establish signed HTTPFS source/
binary correspondence. No universal HTTPFS lifetime/RSS closure is inferred.

GCS relation-schema decoding now uses the existing8MiB decoded allowance instead
of ordinary unlimited decode; validates native written length before adoption.
Targeted offline regression verifies GCS schema observation and invocation reopen
clears temporary/native GCS state while preserving durable table schema. Final
rerun102native/12ignored, build21/extraction9, wire5library20protocol and
SDK18library3runtime pass; strict affected native Clippy/fmt/diff pass. See
[gcs-source-lifetime-checkpoint.md](gcs-source-lifetime-checkpoint.md) for exact
source ownership evidence, missing correspondence and remaining scope.
No total engine/RSS hard cap or in-flight transport termination promise. User's
pause after genuine memory-scope completion is now in effect. Policy alone did
not close tasks37/43/49: reviewed inventory, recovery/refusal regressions and
final unchanged-artifact live/held evidence complete that agreed scope.
