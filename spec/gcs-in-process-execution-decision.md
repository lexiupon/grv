# Decision: simplify GCS execution to in-process DuckDB

Status: user-approved design decision, recorded 2026-10-12. In-process transition
code is implemented; live/held/installed qualification remains pending. See
[transition checkpoint](gcs-in-process-transition-checkpoint.md). `gcs-view` remains unadvertised.
No publication, commit, IAM change or new credentials are authorized here.

## Decision and rationale

Return GRV-owned GCS native execution to the DuckDB adapter process, matching
local/S3's execution placement. The ordinary installed adapter remains a
separate process from the GRV CLI; remove the additional GCS-only native-owner
process rather than treating DuckDB as part of the CLI.

The user judged the dedicated helper's IPC, descriptor remapping, supervision,
fencing, package and supported-platform complexity disproportionate to the
initial feature. This supersedes the earlier dedicated-owner approval (task39)
as the implementation direction. Historical helper implementation/tests remain
evidence, not requirements to continue expanding that architecture.

Do NOT adopt the research-only helper allocation policy. Keep supported DuckDB
cooperative cancellation and stock transport timeout controls, with the explicit
limitations below. The cloud-view objective, exact reader authority and mandatory
preparation verification remain unchanged.

## Accepted hard-deadline limitation

The execution lease remains an admission/authority budget of at most300seconds,
anchored monotonically before credential acquisition; later configuration can
only shorten it. Require actual token expiry beyond the admitted budget plus
at least120seconds. Acquire a fresh token/connection between executions; no
mid-query renewal or blind retry.

The lease is NOT a hard in-flight native stop/join guarantee. Signed stock
HTTPFS timeout fields are not an absolute whole-request deadline. In the retained
synthetic header-trickle test a250ms lease returned after3792ms. The elapsed
result is refused, but native execution stayed in flight past the lease.
DuckDB interrupt does not reliably interrupt a synchronous stock HTTP request.
DNS/connect/TLS/status/headers/body/trailers or native work may delay completion,
cancellation acknowledgement, rollback and worker join beyond the requested
budget. There is no newly promised finite worst-case overrun.

Keep pre-request/content/handle authority checks and refuse late success where
execution returns control. Do not admit new cloud requests after lease expiry.
An already-dispatched request, native credential copies and its transaction may
outlive the admitted lease or120second margin. Server-side OAuth expiry does not
prove immediate cancellation of an already-authorized response. Do not describe
an in-process owner as physically revoked/stopped merely because its logical
state is expired or an interrupt was requested.

No detach of native work, unsafe asynchronous thread cancellation, concurrent
connection destruction, custom HTTP transport or relaxed TLS/redirect authority
is authorized. If a call remains blocked, retain its owner/workspace ownership
and report incomplete/unresolved work; do not reuse that connection or claim
rollback. A stopped acknowledgement must still follow actual stop/join evidence.

Installed adapter-process termination is an existing containment/recovery path,
not a new proved operation-wide deadline fence. Independently embedded library
consumers cannot assume the host will kill their process. Neither process exit,
Close nor cancellation request alone establishes rollback: retain ownership,
reopen only after appropriate stopped-writer/lock evidence, and resolve immutable
outcomes without repeating effects blindly. Missing proof leaves unresolved
outcomes quarantined. General adapter protocol liveness/recovery duties are not
waived by this GCS limitation.

## Accepted engine-memory limitation

Bound transfer buffers, command/result bytes and decoded metadata under their
own admitted budgets. Preserve durable-read CASE/error admission: oversized
records refuse, never become absent through filtering/truncation/reset/repair.
The former32MiB parent/helper reservation is not a universal RSS bound and must
be reassessed for the simpler path before acceptance.

DuckDB's configured512MiB memory_limit and16GiB staging/spill quota are separate
engine controls, NOT a hard cap over every allocation or total process memory.
Expanded instrumentation shows a small length query on a40million-byte persisted
receipt still allocates storage; the stock engine limit controls that tested
storage path but not the tested expression path. With a16MiB engine setting the
40million-byte expression still requested67,108,864 bytes before output refusal.
Document this partial coverage rather than claiming a comprehensive engine cap.
No automatic resource expansion is authorized. Helper-only quotas remain research
evidence; no parent-process global allocator substitution is approved.

These engine limitations are not intrinsically GCS-specific. Existing S3 uses
in-process DuckDB too; its resource qualification must not assume exemption.
This decision accepts the lack of a universal engine/RSS guarantee, not unbounded
GRV transfer buffering or silent memory-related outcome corruption. Actual
allocation/refusal behavior and safe recovery remain qualification work.

## Safeguards retained

- Explicit keyless account/project, protected configuration, auth override refusal,
  temporary redacted bearer secrets and no tokens in argv/env/views/receipts/logs.
- Signed pinned stock HTTPFS; fixed-host exact read-only source authority, TLS,
  no redirect/proxy/retry fallback and backend separation. No new transport.
- Full-object hash/size/schema/row preparation; query savings do not waive it.
- Best-effort GCS query-time object identity, not per-range generation pinning.
- Transactional refresh, real dependency holds, source-free replay/inspection,
  stale-peer/durable refusal and no automatic migration/repair.
- Late-result refusal, cleanup uncertainty fail-closed, immutable outcome resolution
  and no blind repetition of effects. No hard rollback-at-deadline promise.

## Transition and release status

Task45 has removed active helper execution/IPC/supervision/descriptor remapping,
package/override requirements and discovery handoff. Historical code is archived
outside production build paths. The original discovery connection/snapshot is
preserved through configuration. Uncertain setup status poisons reuse without
exiting the embedding process; explicit synchronous owner-thread teardown/reopen
under retained ownership is required. Offline passing slices do not qualify
blocked HTTP cleanup or full immutable-outcome recovery.

Requalify the actual in-process setup/guard/cleanup/cancellation, source-free
recovery, mixed local/GCS inputs, held execution and supported packages. Prior
helper passes do not qualify the new architecture. Update dependent contracts
without invented wire/schema/durable version changes. All other release gates
remain open; publication requires separate approval.

The user requested a pause when the memory-related scope is completed. Do not
advance unrelated qualification automatically, or mark resource work complete
merely because a former hard guarantee has been explicitly removed.

Related: [RFC](grv-cloud-views-pushdown-v1-rfc.md),
[plan](grv-cloud-views-pushdown-v1-plan.md),
[retained deadline finding](gcs-deadline-qualification-blocker.md),
[memory evidence](gcs-owner-memory-checkpoint.md),
[streaming/allocation evidence](gcs-native-metadata-streaming-checkpoint.md).
