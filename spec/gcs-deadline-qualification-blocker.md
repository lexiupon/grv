# GCS absolute in-flight deadline qualification blocker

Status: **retained failed hard-deadline finding; requirement superseded by an
explicit accepted limitation**, not a passing test. The user approved returning
GCS execution to in-process DuckDB within the adapter; see
[decision and safeguards](gcs-in-process-execution-decision.md). No hard in-flight
stop/join/rollback deadline is promised. Transition and honest cancellation/
cleanup/recovery qualification remain pending; `gcs-view` remains unadvertised.
No new transport or publication is authorized.

The helper history below records the earlier direction and evidence, not the
current architecture requirement.

## Executable finding

Revision 3's engine `HTTPUtil::Request` hooks do not intercept the official
signed HTTPFS extension's private implementations. Revision 4 binds pinned
HTTPFSParams to a virtual SendRequest delegate, fixing that request-entry seam.
The delegate checks exact admitted URL/read method/deadline, resets stock clients,
uses remaining-lease timeout fields and checks GET content delivery.

Signed stock curl has connect/low-speed timeouts but no hard transfer timeout.
Signed stock httplib admits bounded stalled HEAD and trickling GET tests, but
continuously trickling response headers bypass the per-read timeout. With a
250 ms lease, the synthetic header case returned after **3792 ms**. Late results
are rejected, but the request/owner remains in flight. This fails bounded stop/
join acceptance. The revised contract accepts that lack of a hard stop guarantee
explicitly; this remains a failed test of the stronger guarantee, not a harmless
performance claim or proof of timely rollback.

Reproduce offline with signed HTTPFS and the optional native proof target:

```sh
python3 crates/grv-adapter-duckdb/native/test_gcs_stock_request.py \
  --proof /protected/build/grv_gcs_stock_request_proof \
  --httpfs /protected/extensions/httpfs.duckdb_extension
```

The default includes the failing header gate. The explicit
`--skip-known-header-blocker` option runs only supplemental partial proofs and
prints the omitted gate. No cloud credentials are used: loopback synthetic bearer,
direct read, redirects, stalled HEAD, trickling GET, expired/unlisted authority
and header trickle. Zero redirect-destination and inactive-authority requests
are required.

## Independent review and fixes

The review found a potential shared HTTPFSParams timeout/retries race under
parallel reads, plus temporary-secret retention after partial setup failure.
Source fixes construct private per-request params and concrete GET/HEAD request
copies, dispose clients on all exits, wipe policy-owned bearer copies, and attempt
temporary-secret revocation before restoring the failed owner guard. Partial
signed-stock tests pass. Full production rebuild/live qualification of these
changes passed in the rebuilt production library and live pull/self-preparation
rerun at checkpoint `4a17671`; the separately labeled stable-library held test
also passed. These successes do not close the hard in-flight deadline blocker.

The exact pinned upstream params/header and Clone path preserve the policy
binding; supported-target ABI/destruction/transport qualification remains open.
Stock-owned credential/SQL copies cannot be assumed zeroized simply because the
policy-owned buffers are wiped.

## Why existing cancellation is insufficient

DuckDB interrupt does not cancel the synchronous stock HTTP client. Pull/build
workers join their native owner threads; a blocked request prevents that join.
SDK stopped acknowledgement correctly follows successful worker stop/join, so
it cannot claim stopped evidence for this blocked owner.

Adapter-host IPC deadlines are absolute during a read/write, but sending frames
resets them. Cancel timeout alone does not terminate the child. Drop eventually
signals the process group TERM/KILL and waits, without proving stopped descendants
or an immutable operation-wide GCS deadline. ExternalInvocation's retained
workspace descriptor, non-reaping leader observation, checked ownership recovery
and stopped-writer evidence are the stronger precedent.

## Historical dedicated-owner direction (superseded)

The user originally selected option 3: a dedicated GCS native owner process, retaining
signed stock HTTPFS, deadline-triggered escalation, ownership fencing and durable
unresolved-outcome recovery. This authorizes implementation, not capability
activation, IAM changes or publication. The earlier approval dependency is closed;
implementation and adversarial qualification remain separate tasks.

Keep signed stock transport, but isolate each GCS native owner in an exclusively
owned executable process with bounded typed IPC and one monotonic operation
budget. Trigger cooperative cancellation and process-group escalation within
that budget; preserve ownership until exit/native-lock reacquisition proves the
fence. Failure to prove the fence leaves the outcome unresolved and prohibits
workspace mutation; no thread detach, asynchronous thread kill or ownership theft.

The implementation in progress proxies the NativeEngine ABI boundary rather
than moving BuildWorker closures: a dedicated `grv-duckdb-gcs-owner` receives
typed metadata/remote/schema/staging commands, owns the real native connection,
and inherits the workspace lock reference. The parent retains its reference
through supervision and recovery. Local/S3 stay in process. Packaging includes
the sibling helper; development may explicitly select a protected helper path.
Same-process library callers need this helper or must fail closed. This avoids
introducing a separate scan-staging database with unproven snapshot semantics.
Killing the real workspace transaction is not rollback evidence: reopen/recover
and resolve immutable attempt/invocation receipts rather than replaying effects
blindly. Compilation and passing slices are not fence acceptance.

## Current isolated-owner checkpoint (not acceptance)

The helper and ABI proxy compile. Parent/child watchers retain one monotonic
budget (later credential acquisition may only shorten it), shared cancellation
is installed before setup, and child process self-exit covers original deadline
or parent disconnection while synchronous native work is blocked. The parent
retains ownership and quarantines it if the checked process fence is unproved.
Darwin zombie-only groups require positive kernel membership plus non-reaping
exit evidence; permission errors alone never prove absence.

Independent review caught a discovery snapshot defect: build discovery retains
an open transaction. Moving its connection at later reader configuration would
lose that snapshot and allow preparation DDL to autocommit. External GCS discovery
now starts an **unconfigured** child before BEGIN; credentials are acquired later
on that same connection. Source-free injected preparation rollback and live
nonempty preparation/rollback/reopen both pass. Private relation authorization
is also routed to the child; reopened owners reconstruct their own guard.

Passing slices: 84 ordinary native adapter tests; synthetic idle/cancel/partial
IPC/deadline computation; complete native failure followed by rollback, commit
and local reopen; source-free discovery/preparation atomicity; signed-stock
header-trickle process supervision (1200 ms supervisor budget, about1260 ms
including stop/reap, one HEAD then socket close/lock regain). This supplemental
supervised proof does not change the standalone250ms header regression and is
not full network-phase or250ms production-helper qualification. Two authorized
live pull/self-preparation reruns passed and each owned fixture was deleted with
empty all-version inventory and bucket404. The previous full parent-held pass
must be rerun after the owner change.

Verified local descriptor operations now cross a bounded SCM_RIGHTS endpoint,
with readonly regular/seekable checks, CLOEXEC, exact quoted SQL fd remapping and
RAII close on every exit. No source filename is reopened. Lazy VIEW/MACRO/PREPARE
fd definitions are refused before native dispatch. A Darwin adversarial test
exposed installed descriptor leaks when ancillary data was truncated; the receive
buffer now covers the full platform maximum and rejects/closes all extras.
Seventeen owner IPC tests pass. Full native regression is now **95 passed /
13 ignored**; configured synthetic GCS+local self-base import/transaction/reopen/
abort cleanup and unlinked descriptor/widening tests pass. Parent crash self-fence
and uncommitted recovery, lost commit reply resolved without reexecution, and
precommit disappearance tests also pass (native primitives, not all GRV receipt
races). Live pull/self-preparation passed again at `~/grv-shipping-gcs-oh8fzc6h`,
owned generation/bucket removed with empty versions and404. Full actual parent-held post-isolation execution **passed** in1919.00s at
`~/grv-held-gcs-d0xk4f7t`, prefix cleanup plus empty all-version inventory/bucket404.
This supersedes the pending rerun, not abort/prune/release/expiry acceptance.
After it finished, serial regression passed **97 tests /16 ignored**, and five
targeted recovery tests passed including database replacement refusal and
cancellation through an interrupt clone obtained before handoff.32MiB scratch
admission boundary tests pass before helper/auth; pair-wide worstcase allocation
proof remains open. Subsequent private IPC now uses raw2MiB responses and
2MiB+64KiB commandframes plus8MiB GCS metadata owned-size preflight; native109
passed/17ignored and targeted recovery/descriptor/concurrent-owner slices pass.
Live pull/self-preparation `~/grv-shipping-gcs-67d7npml` passed/cleaned404. Full
held lifecycle raw-IPC rerun passed1839.11s at `~/grv-held-gcs-9zqrg2qn`,
emptyversions404; latest borrowed admission/structural preflight guards were not
in that held helper. Post-run artifact hashes verified; no pre-run hashes. See
[gcs-owner-memory-checkpoint.md](gcs-owner-memory-checkpoint.md); reduced
allocation slices are not universal32MiB/RSS acceptance.

Supplemental signed-stock loopback now also exercises incomplete status line,
trickling range body, chunk stream and trailer header under the same original
1200ms process supervisor budget. Each reaches its expected HEAD/GET dispatch,
closes sockets and regains workspace references; tests pass. Stock transport may
reject a phase before the supervisor deadline, so these do not prove every phase
requires escalation. They do not widen production HTTPS authority or qualify
DNS/TCP/TLS or the complete production helper. Standalone250ms header defect
remains retained; no full network-phase acceptance is inferred. Artifact-only
configuration test also proves parent original deadline cannot renew after later
configuration: later expiry leaves original budget unchanged, shorter budget
is retained on subsequent longer offers; fence/reopen succeeds. Setup uses
invalid extension plus synthetic token, not a network or real-OAuth test.

Remaining implementation/acceptance: descendant and full GRV lost-reply/commit
receipt fault matrices; handshake/setup cancellation and inode-replacement
faults; all network phases and supported targets; resource accounting for extra
process/IPC buffers;
helper/package installed identity and relocation. Protected helper override is
operator-selected executable authority, not an executable signature attestation.
No capability advertisement or release readiness is implied.

Ordinary independently configured DuckDB consumers retain their own authority
and expiry limits; GRV cannot promise to stop arbitrary external readers.

This is an architectural/contract decision, not a small timeout adjustment.
The former stronger guarantee called for deadline-triggered escalation and late
result refusal, not universally exact-instant OS reaping. That helper architecture
is now superseded explicitly, not silently qualified. Under the new in-process
direction, test and disclose DNS/TCP/TLS/status/header/body/trailer cancellation
behavior; keep late refusal and fail-closed unresolved outcomes. Do not promise
finite stop/rollback overrun without proof or use prior helper descendant/fence
slices as evidence for the replacement. Requalify commit/recovery behavior.
